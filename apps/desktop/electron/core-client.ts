import { ChildProcessWithoutNullStreams, spawn } from 'node:child_process';
import { app } from 'electron';
import { createInterface } from 'node:readline';
import path from 'node:path';

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
  timer: NodeJS.Timeout;
};

type CoreResponse = {
  type: 'response';
  id: string;
  result?: unknown;
  error?: { code: string; message: string };
};

const SEARCH_TIMEOUT_MS = 120_000;
const DEFAULT_TIMEOUT_MS = 10_000;
const INDEX_TIMEOUT_MS = 30 * 60 * 1000;
const SYNC_TIMEOUT_MS = 2 * 60 * 1000;

let coreProcess: ChildProcessWithoutNullStreams | null = null;
let nextID = 1;
const pending = new Map<string, PendingRequest>();

/** Returns the platform-specific core executable path. */
export function getCorePath(): string {
  if (process.env.PHANTASM_CORE_PATH) {
    return process.env.PHANTASM_CORE_PATH;
  }

  const exeName = process.platform === 'win32' ? 'phantasm-core.exe' : 'phantasm-core';
  if (app.isPackaged) {
    return path.join(process.resourcesPath, 'core', exeName);
  }

  return path.resolve(app.getAppPath(), '..', '..', 'core', 'bin', exeName);
}

/** Starts the Go core process if it is not already running. */
export function ensureCore(): ChildProcessWithoutNullStreams {
  if (coreProcess && !coreProcess.killed) {
    return coreProcess;
  }

  const executable = getCorePath();
  coreProcess = spawn(executable, [], {
    cwd: app.getPath('userData'),
    env: {
      ...process.env,
      PHANTASM_DB_PATH: path.join(app.getPath('userData'), 'recall.db')
    },
    stdio: ['pipe', 'pipe', 'pipe']
  });

  const lines = createInterface({ input: coreProcess.stdout });
  lines.on('line', handleCoreLine);

  coreProcess.stderr.on('data', (chunk) => {
    console.error(`[phantasm-core] ${chunk.toString().trim()}`);
  });

  coreProcess.on('error', (error) => {
    rejectPending(error);
    console.error('[phantasm-core] failed to start:', error);
    coreProcess = null;
  });

  coreProcess.on('exit', (code, signal) => {
    rejectPending(new Error(`Go core exited (${code ?? signal ?? 'unknown'})`));
    coreProcess = null;
  });

  return coreProcess;
}

/** Stops the Go core process and clears pending requests. */
export function stopCore(): void {
  if (!coreProcess) {
    return;
  }

  coreProcess.kill();
  coreProcess = null;
}

/** Sends a JSON-line request to the Go core and resolves with its result. */
export function requestCore<T>(
  method: string,
  params: Record<string, unknown> = {},
  timeoutMs = timeoutFor(method)
): Promise<T> {
  const child = ensureCore();
  const id = String(nextID++);

  const message = JSON.stringify({
    type: 'request',
    id,
    method,
    params
  });

  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`Core request timed out: ${method}`));
    }, timeoutMs);

    pending.set(id, {
      resolve: (value) => resolve(value as T),
      reject,
      timer
    });

    child.stdin.write(`${message}\n`, (error) => {
      if (!error) {
        return;
      }
      clearTimeout(timer);
      pending.delete(id);
      reject(error);
    });
  });
}

function timeoutFor(method: string): number {
  if (method === 'search') {
    return SEARCH_TIMEOUT_MS;
  }
  if (method === 'index_path') {
    return INDEX_TIMEOUT_MS;
  }
  if (method === 'sync_browsers') {
    return SYNC_TIMEOUT_MS;
  }
  return DEFAULT_TIMEOUT_MS;
}

function handleCoreLine(line: string): void {
  let response: CoreResponse;
  try {
    response = JSON.parse(line) as CoreResponse;
  } catch (error) {
    console.error('[phantasm-core] invalid json:', line, error);
    return;
  }

  if (response.type !== 'response') {
    return;
  }

  const request = pending.get(response.id);
  if (!request) {
    return;
  }

  clearTimeout(request.timer);
  pending.delete(response.id);

  if (response.error) {
    request.reject(new Error(response.error.message));
    return;
  }

  request.resolve(response.result);
}

function rejectPending(reason: Error): void {
  for (const request of pending.values()) {
    clearTimeout(request.timer);
    request.reject(reason);
  }
  pending.clear();
}
