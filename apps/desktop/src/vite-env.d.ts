/// <reference types="vite/client" />

import type { RecallAPI } from '../electron/preload';

declare global {
  interface Window {
    recall?: RecallAPI;
  }
}

