import {
  app,
  BrowserWindow,
  dialog,
  globalShortcut,
  ipcMain,
  Menu,
  type NativeImage,
  nativeImage,
  nativeTheme,
  screen,
  shell,
  Tray
} from 'electron';
import fs from 'node:fs';
import path from 'node:path';
import { ensureCore, requestCore, stopCore } from './core-client';

let mainWindow: BrowserWindow | null = null;
let tray: Tray | null = null;
let coreStoppedForQuit = false;
type CancelResponse = {
  ok: boolean;
  canceled: boolean;
  timed_out?: boolean;
};

function placeWindowLikeLauncher(win: BrowserWindow): void {
  const cursorPoint = screen.getCursorScreenPoint();
  const display = screen.getDisplayNearestPoint(cursorPoint);
  const { x, y, width, height } = display.workArea;
  const [windowWidth] = win.getSize();
  const targetX = Math.round(x + (width - windowWidth) / 2);
  const topOffsetRatio = 0.22;
  const targetY = y + Math.round(height * topOffsetRatio);
  win.setPosition(targetX, targetY);
}

/** Resolve a non-empty tray icon source for the current platform. */
function resolveTrayIcon(): NativeImage {
  if (process.platform !== 'win32') {
    return nativeImage.createEmpty();
  }

  // Prefer a dedicated transparent tray PNG, fall back to the app ICO.
  // Avoid app.getFileIcon() — it wraps the icon with Windows shell decoration
  // (opaque background) which produces a gray border in the system tray.
  for (const name of ['tray.png', 'icon.ico']) {
    const iconPath = path.resolve(app.getAppPath(), 'build', name);
    if (fs.existsSync(iconPath)) {
      const icon = nativeImage.createFromPath(iconPath);
      if (!icon.isEmpty()) {
        return icon;
      }
    }
  }

  return nativeImage.createEmpty();
}

/** Creates the main search window. */
function createWindow(): void {
  mainWindow = new BrowserWindow({
    width: 760,
    height: 82,
    minWidth: 560,
    minHeight: 82,
    frame: false,
    show: false,
    title: 'Recall',
    backgroundColor: nativeTheme.shouldUseDarkColors ? '#111315' : '#f7f7f4',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true
    }
  });

  const devURL = process.env.VITE_DEV_SERVER_URL;
  if (devURL) {
    void mainWindow.loadURL(devURL);
  } else {
    void mainWindow.loadFile(path.join(__dirname, '..', 'dist', 'index.html'));
  }

  mainWindow.once('ready-to-show', () => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      placeWindowLikeLauncher(mainWindow);
      mainWindow.show();
      mainWindow.focus();
    }
  });

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

function setWindowHeight(height: number): void {
  if (!mainWindow || mainWindow.isDestroyed()) {
    return;
  }

  const [width] = mainWindow.getContentSize();
  const targetHeight = Math.max(82, Math.min(Math.round(height), 520));
  mainWindow.setContentSize(width, targetHeight, true);
}

/** Shows the main window, creating it if necessary. */
function showWindow(): void {
  if (!mainWindow || mainWindow.isDestroyed()) {
    createWindow();
  } else {
    placeWindowLikeLauncher(mainWindow);
    mainWindow.show();
    mainWindow.focus();
  }
}

/** Hides the main window (keeps app alive in tray). */
function hideWindow(): void {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.hide();
  }
}

async function requestCoreCancel(method: 'cancel_search' | 'cancel_index' | 'cancel_sync_browsers'): Promise<CancelResponse> {
  try {
    return await requestCore<CancelResponse>(method);
  } catch (error) {
    if (isCoreTimeoutError(error, method)) {
      console.warn(`[core] ${method} timed out; treating as best-effort cancel`);
      return { ok: false, canceled: false, timed_out: true };
    }
    throw error;
  }
}

function isCoreTimeoutError(error: unknown, method: string): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes(`Core request timed out: ${method}`);
}

/** Registers app-level shortcuts and tray controls. */
async function registerShellControls(): Promise<void> {
  // Ctrl+Space — wake up / show
  globalShortcut.register('Control+Space', showWindow);

  // Ctrl+W — hide to tray
  globalShortcut.register('Control+W', hideWindow);

  try {
    const trayIcon = resolveTrayIcon();
    if (trayIcon.isEmpty()) {
      console.warn('[tray] icon is empty, skip tray setup');
      return;
    }
    tray = new Tray(trayIcon);
    tray.setToolTip('Recall');
    tray.setContextMenu(
      Menu.buildFromTemplate([
        {
          label: '打开',
          click: showWindow
        },
        { label: '退出', click: () => app.quit() }])
    );
  } catch (error) {
    console.error('[tray] setup failed:', error);
  }
}

/** Wires renderer IPC to the Go core child process. */
function registerIpc(): void {
  ipcMain.handle('core:health', () => requestCore('health'));
  ipcMain.handle('core:search', (_event, params) => requestCore('search', params));
  ipcMain.handle('core:cancelSearch', () => requestCoreCancel('cancel_search'));
  ipcMain.handle('core:syncBrowsers', () => requestCore('sync_browsers'));
  ipcMain.handle('core:cancelSyncBrowsers', () => requestCoreCancel('cancel_sync_browsers'));
  ipcMain.handle('core:indexPath', (_event, params) => requestCore('index_path', params));
  ipcMain.handle('core:cancelIndex', () => requestCoreCancel('cancel_index'));
  ipcMain.handle('core:indexProgress', () => requestCore('index_progress'));
  ipcMain.handle('app:openPath', (_event, targetPath: string) => shell.openPath(targetPath));
  ipcMain.handle('app:openUrl', (_event, url: string) => shell.openExternal(url));
  ipcMain.handle('app:hide', () => hideWindow());
  ipcMain.handle('app:setWindowHeight', (_event, height: number) => {
    setWindowHeight(height);
  });
  ipcMain.handle('app:showItemInFolder', (_event, targetPath: string) => {
    shell.showItemInFolder(targetPath);
    return true;
  });
  ipcMain.handle('app:theme', () => (nativeTheme.shouldUseDarkColors ? 'dark' : 'light'));
  ipcMain.handle('app:chooseFolder', async () => {
    const result = await dialog.showOpenDialog({ properties: ['openDirectory'] });
    return result.canceled ? null : result.filePaths[0];
  });
}

// Disable GPU shader disk cache to avoid access-denied errors when
// the cache directory is locked by a previous process.
app.commandLine.appendSwitch('disable-gpu-shader-disk-cache');

app.whenReady().then(async () => {
  registerIpc();
  createWindow();
  await registerShellControls();
  ensureCore();
}).catch((error) => {
  console.error('[app] startup failed:', error);
});

app.on('window-all-closed', () => {
  hideWindow();
});

app.on('before-quit', (event) => {
  if (coreStoppedForQuit) {
    return;
  }
  event.preventDefault();
  globalShortcut.unregisterAll();
  tray = null;
  void stopCore().finally(() => {
    coreStoppedForQuit = true;
    app.quit();
  });
});
