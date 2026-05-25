import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

const targetUrl = process.argv[2] ?? 'http://127.0.0.1:5173/';
const chromePath =
  process.env.CHROME_PATH ??
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const debugPort = 9333;
const profileDir = await mkdtemp(path.join(tmpdir(), 'recall-height-test-'));

const chrome = spawn(chromePath, [
  '--headless=new',
  '--disable-gpu',
  '--no-first-run',
  '--no-default-browser-check',
  `--remote-debugging-port=${debugPort}`,
  `--user-data-dir=${profileDir}`,
  'about:blank'
], { stdio: 'ignore' });

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, options = {}, attempts = 50) {
  let lastError;
  for (let i = 0; i < attempts; i += 1) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response.json();
      lastError = new Error(`${response.status} ${response.statusText}`);
    } catch (error) {
      lastError = error;
    }
    await delay(100);
  }
  throw lastError;
}

class CDP {
  constructor(wsUrl) {
    this.nextId = 1;
    this.pending = new Map();
    this.ws = new WebSocket(wsUrl);
    this.ws.addEventListener('message', (event) => {
      const message = JSON.parse(event.data);
      if (message.id && this.pending.has(message.id)) {
        const { resolve, reject } = this.pending.get(message.id);
        this.pending.delete(message.id);
        if (message.error) reject(new Error(message.error.message));
        else resolve(message.result);
      }
    });
  }

  async open() {
    if (this.ws.readyState === WebSocket.OPEN) return;
    await new Promise((resolve, reject) => {
      this.ws.addEventListener('open', resolve, { once: true });
      this.ws.addEventListener('error', reject, { once: true });
    });
  }

  call(method, params = {}) {
    const id = this.nextId;
    this.nextId += 1;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  close() {
    this.ws.close();
  }
}

async function evaluate(cdp, expression) {
  const result = await cdp.call('Runtime.evaluate', {
    expression,
    awaitPromise: true,
    returnByValue: true
  });
  if (result.exceptionDetails) {
    throw new Error(result.exceptionDetails.text);
  }
  return result.result.value;
}

function setInputExpression(value) {
  return `
    (() => {
      const input = document.querySelector('input');
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
      setter.call(input, ${JSON.stringify(value)});
      input.dispatchEvent(new Event('input', { bubbles: true }));
    })()
  `;
}

try {
  const tab = await fetchJson(
    `http://127.0.0.1:${debugPort}/json/new?${encodeURIComponent('about:blank')}`,
    { method: 'PUT' }
  );
  const cdp = new CDP(tab.webSocketDebuggerUrl);
  await cdp.open();
  await cdp.call('Runtime.enable');
  await cdp.call('Page.enable');
  await cdp.call('Page.addScriptToEvaluateOnNewDocument', {
    source: `
      (() => {
        const heightCalls = [];
        const result = {
          rowid: 1,
          item_id: 'sample',
          source: 'file',
          title: '新建 文本文档.txt',
          preview: 'AI只能教人做事，不能教人识人',
          path: 'C:\\\\Users\\\\47527\\\\Desktop\\\\新建 文本文档.txt',
          file_type: 'txt',
          updated_at: 1716427200,
          score: 1,
          metadata: {}
        };
        window.__heightCalls = heightCalls;
        Object.defineProperty(window, 'phantasm', {
          configurable: true,
          value: {
            search: async ({ query }) => {
              await new Promise((resolve) => setTimeout(resolve, 10));
              if (query.includes('无结果')) {
                return { query, elapsed_ms: 1.2, total: 0, results: [] };
              }
              return { query, elapsed_ms: 1.5, total: 1, results: [result] };
            },
            setWindowHeight: async (height) => {
              heightCalls.push(height);
              return true;
            },
            chooseFolder: async () => null,
            indexPath: async () => ({ indexed: 0 }),
            syncBrowsers: async () => true,
            openPath: async () => true,
            openUrl: async () => true,
            hide: async () => true,
            showItemInFolder: async () => true,
            theme: async () => 'light'
          }
        });
      })();
    `
  });
  await cdp.call('Page.navigate', { url: targetUrl });
  await delay(500);

  const initialHeight = await evaluate(cdp, 'window.__heightCalls.at(-1)');

  await evaluate(cdp, setInputExpression('教人'));
  await delay(250);
  const resultHeight = await evaluate(cdp, 'window.__heightCalls.at(-1)');
  const resultCount = await evaluate(cdp, 'document.querySelectorAll(".result").length');
  const resultFits = await evaluate(cdp, `
    (() => {
      const result = document.querySelector('.result');
      const results = document.querySelector('.results');
      if (!result || !results) return false;
      const resultRect = result.getBoundingClientRect();
      const resultsRect = results.getBoundingClientRect();
      return resultRect.bottom <= resultsRect.bottom - 2;
    })()
  `);

  await evaluate(cdp, setInputExpression(''));
  await delay(100);
  const clearedHeight = await evaluate(cdp, 'window.__heightCalls.at(-1)');
  const clearedResultCount = await evaluate(cdp, 'document.querySelectorAll(".result").length');

  await evaluate(cdp, setInputExpression('无结果'));
  await delay(250);
  const noResultHeight = await evaluate(cdp, 'window.__heightCalls.at(-1)');
  const noResultCount = await evaluate(cdp, 'document.querySelectorAll(".result").length');
  const noResultBlank = await evaluate(cdp, 'document.querySelectorAll(".memoryBlank").length');

  const calls = await evaluate(cdp, 'window.__heightCalls');
  const summary = {
    initialHeight,
    resultHeight,
    resultCount,
    resultFits,
    clearedHeight,
    clearedResultCount,
    noResultHeight,
    noResultCount,
    noResultBlank,
    calls
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!(resultHeight > initialHeight)) {
    throw new Error('Expected result height to expand.');
  }
  if (!resultFits) {
    throw new Error('Expected single result to fit without clipping.');
  }
  if (clearedHeight !== initialHeight || clearedResultCount !== 0) {
    throw new Error('Expected clearing input to restore compact height.');
  }
  if (noResultHeight !== initialHeight || noResultCount !== 0 || noResultBlank !== 0) {
    throw new Error('Expected no-result search to restore compact height without empty panel.');
  }

  cdp.close();
} finally {
  chrome.kill();
  await delay(300);
  await rm(profileDir, { recursive: true, force: true }).catch(() => {});
}
