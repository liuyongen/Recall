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

/** 为当前平台解析一个非空的托盘图标来源。 */
function resolveTrayIcon(): NativeImage {
  const iconNames = process.platform === 'darwin'
    ? ['tray.png', 'icon.icns', 'icon.ico']
    : ['tray.ico', 'tray.png', 'icon.ico'];

  // 优先使用专用的多尺寸托盘图标，再回退到 PNG 或应用图标。
  // 避免使用 app.getFileIcon()，它会给图标套上 Windows Shell 装饰
  // （不透明背景），导致系统托盘里出现灰色边框。
  for (const name of iconNames) {
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

/** 创建主搜索窗口。 */
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

/** 显示主窗口；必要时先创建窗口。 */
function showWindow(): void {
  if (!mainWindow || mainWindow.isDestroyed()) {
    createWindow();
  } else {
    placeWindowLikeLauncher(mainWindow);
    mainWindow.show();
    mainWindow.focus();
  }
}

/** 隐藏主窗口，并让应用继续驻留托盘。 */
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

/** 注册应用级快捷键和托盘控制。 */
async function registerShellControls(): Promise<void> {
  // Ctrl+Space - 唤醒 / 显示
  globalShortcut.register('Control+Space', showWindow);

  // Ctrl+W - 隐藏到托盘
  globalShortcut.register('Control+W', hideWindow);

  try {
    const trayIcon = resolveTrayIcon();
    if (trayIcon.isEmpty()) {
      console.warn('[tray] icon is empty, skip tray setup');
      return;
    }
    tray = new Tray(trayIcon);
    tray.setToolTip('Recall');
    tray.on('click', showWindow);
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

/** 将渲染进程 IPC 接到 Go 核心子进程。 */
function registerIpc(): void {
  ipcMain.handle('core:health', () => requestCore('health'));
  ipcMain.handle('core:search', (_event, params) => requestCore('search', params));
  ipcMain.handle('core:cancelSearch', () => requestCoreCancel('cancel_search'));
  ipcMain.handle('core:syncBrowsers', () => requestCore('sync_browsers'));
  ipcMain.handle('core:cancelSyncBrowsers', () => requestCoreCancel('cancel_sync_browsers'));
  ipcMain.handle('core:indexPath', (_event, params) => requestCore('index_path', params));
  ipcMain.handle('core:cancelIndex', () => requestCoreCancel('cancel_index'));
  ipcMain.handle('core:indexProgress', () => requestCore('index_progress'));
  ipcMain.handle('core:pauseContentIndex', () => requestCore('pause_content_index'));
  ipcMain.handle('core:resumeContentIndex', () => requestCore('resume_content_index'));
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

// 禁用 GPU 着色器磁盘缓存，避免缓存目录被上一个进程锁住时出现拒绝访问错误。
app.commandLine.appendSwitch('disable-gpu-shader-disk-cache');

// 无论 npm 包名是什么，都把 userData 覆盖为稳定的 "recall" 文件夹。
// 必须在 app.whenReady() 之前设置，这样之后每次调用 app.getPath('userData')，
// 包括 core-client 中的调用，都会使用正确路径。
app.setPath('userData', path.join(app.getPath('appData'), 'recall'));

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

app.on('activate', () => {
  showWindow();
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
