import { contextBridge, ipcRenderer } from 'electron';

const api = {
  health: () => ipcRenderer.invoke('core:health'),
  search: (params: SearchParams) => ipcRenderer.invoke('core:search', params),
  indexPath: (params: IndexPathParams) => ipcRenderer.invoke('core:indexPath', params),
  cancelIndex: () => ipcRenderer.invoke('core:cancelIndex'),
  indexProgress: () => ipcRenderer.invoke('core:indexProgress'),
  syncBrowsers: () => ipcRenderer.invoke('core:syncBrowsers'),
  chooseFolder: () => ipcRenderer.invoke('app:chooseFolder'),
  openPath: (path: string) => ipcRenderer.invoke('app:openPath', path),
  openUrl: (url: string) => ipcRenderer.invoke('app:openUrl', url),
  hide: () => ipcRenderer.invoke('app:hide'),
  setWindowHeight: (height: number) => ipcRenderer.invoke('app:setWindowHeight', height),
  showItemInFolder: (path: string) => ipcRenderer.invoke('app:showItemInFolder', path),
  theme: () => ipcRenderer.invoke('app:theme')
};

contextBridge.exposeInMainWorld('phantasm', api);

export type SearchParams = {
  query: string;
  source?: string;
  file_type?: string;
  since?: number;
  until?: number;
  limit?: number;
  offset?: number;
};

export type IndexPathParams = {
  path: string;
  max_bytes?: number;
};

export type IndexProgress = {
  active: boolean;
  phase: string;
  path?: string;
  current?: string;
  total: number;
  scanned: number;
  indexed: number;
  skipped: number;
  written: number;
  workers: number;
  files_per_sec: number;
  eta_ms: number;
  started_at?: number;
  updated_at?: number;
  elapsed_ms: number;
  last_error?: string;
  last_completed?: number;
};

export type PhantasmAPI = typeof api;
