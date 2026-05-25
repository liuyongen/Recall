/// <reference types="vite/client" />

import type { PhantasmAPI } from '../electron/preload';

declare global {
  interface Window {
    phantasm?: PhantasmAPI;
  }
}
