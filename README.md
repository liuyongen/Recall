# Recall — 本地优先的个人搜索工具

中文 | [English](README.en.md)

**Recall** 是一个本地优先（local-first）的个人记忆搜索桌面应用，适合有隐私需求的个人知识管理（PKM）用户。

它把**文件搜索、浏览器历史、书签、下载记录**整合到一个启动器风格的快速搜索入口，索引与检索全程在本机完成，无需联网、无需云服务。

技术栈：**Electron + React**（桌面壳层）+ **Go**（核心引擎）+ **SQLite FTS5**（全文检索）。

## 演示

<!-- 替换为你的演示视频或截图 -->
![界面预览（待添加）](./assets/preview.gif)

## 为什么是 Recall

- 本地优先：核心搜索不依赖云端
- 检索速度快：基于 SQLite FTS5 与增量索引
- 使用场景清晰：文件、历史记录、书签、下载记录
- 交互简洁：启动器风格的搜索窗口

## 核心特性

- Electron + React 桌面壳层
- Go Core 负责抽取、索引与排序
- 基于 chunk 的增量差分更新
- 长任务可视化索引进度
- 可选本地 Apache Tika（PDF/Office 文档抽取）

## 快速开始

环境要求：
- Node.js 18+
- Go 1.22+

安装依赖（首次或删除 `node_modules` 后）：

```powershell
npm ci
```

如本地没有 lock 对应环境，可使用：

```powershell
npm install
```

开发模式启动：

```powershell
npm run dev
```

构建桌面安装包：

```powershell
npm run dist
```

## 隐私

Recall 以本地数据控制为前提设计：

- 搜索与索引在本机执行
- 数据库存储在本地
- 不要求接入远程索引服务

## 关键词

`本地搜索` `文件搜索` `浏览器历史搜索` `书签管理` `个人知识管理` `PKM` `离线搜索` `隐私优先` `桌面搜索工具` `全文检索` `Electron 桌面应用` `Windows 搜索工具`
