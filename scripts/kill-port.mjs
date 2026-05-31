import { execFileSync } from 'node:child_process';

const port = Number.parseInt(process.argv[2] ?? '5173', 10);
if (!Number.isInteger(port) || port <= 0 || port > 65535) {
  throw new Error(`Invalid port: ${process.argv[2]}`);
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function windowsPids() {
  const output = execFileSync('netstat', ['-ano', '-p', 'tcp'], { encoding: 'utf8' });
  return unique(output
    .split(/\r?\n/)
    .map((line) => line.trim().split(/\s+/))
    .filter((parts) => parts.length >= 5 && parts[0] === 'TCP' && parts[3] === 'LISTENING')
    .filter((parts) => parts[1].endsWith(`:${port}`))
    .map((parts) => parts[4]));
}

function unixPids() {
  try {
    const output = execFileSync('lsof', ['-nP', '-ti', `tcp:${port}`, '-sTCP:LISTEN'], { encoding: 'utf8' });
    return unique(output.split(/\s+/));
  } catch (error) {
    if (error.status === 1) {
      return [];
    }
    throw error;
  }
}

const currentPid = String(process.pid);
const pids = (process.platform === 'win32' ? windowsPids() : unixPids()).filter((pid) => pid !== currentPid);

for (const pid of pids) {
  console.log(`Stopping process ${pid} on port ${port}...`);
  if (process.platform === 'win32') {
    execFileSync('taskkill', ['/PID', pid, '/T', '/F'], { stdio: 'ignore' });
  } else {
    try {
      process.kill(Number(pid), 'SIGTERM');
    } catch (error) {
      if (error.code !== 'ESRCH') {
        throw error;
      }
    }
  }
}
