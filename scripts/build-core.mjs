import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const outDir = path.join(root, 'core', 'bin');
const exeName = process.platform === 'win32' ? 'recall-core.exe' : 'recall-core';
const target = path.join(outDir, exeName);

function findGo() {
  const probe = spawnSync('go', ['version'], { stdio: 'ignore' });
  if (probe.status === 0) {
    return 'go';
  }

  throw new Error('Go executable not found. Add Go to PATH and retry.');
}

fs.mkdirSync(outDir, { recursive: true });
fs.rmSync(target, { force: true });

const go = findGo();
const result = spawnSync(
  go,
  ['build', '-trimpath', '-ldflags=-s -w', '-o', target, './core/cmd/recall-core'],
  {
    cwd: root,
    env: {
      ...process.env,
      CGO_ENABLED: '0'
    },
    stdio: 'inherit'
  }
);

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
