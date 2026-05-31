# Recall — Local-First Personal Search

[中文](README.md) | English

**Recall** is a local-first desktop search app for personal knowledge management (PKM). It lets you search files, browser history, bookmarks, and downloads from a single fast launcher-style window — with all indexing and retrieval running entirely on your machine, no cloud required.

Built with **Electron + React** (UI) and a **Go** core backed by **SQLite FTS5** full-text search.

Supported platforms: Windows and macOS.

## Why Recall

- Local-first by design: no cloud dependency for core search
- Fast retrieval: SQLite FTS5 + incremental indexing
- Practical scope: files, browser history, bookmarks, downloads
- Clean desktop UX: instant launcher-style search panel

## Highlights

- Electron + React desktop shell
- Go core for extraction, indexing, and ranking
- Incremental file indexing with chunk-level diff
- Built-in progress reporting for long indexing tasks
- Optional local Apache Tika integration for PDF/Office extraction

## Quick Start

Prerequisites:
- Node.js 18+
- Go 1.22+

Install dependencies (first run or after removing `node_modules`):

```sh
npm ci
```

If your local lockfile environment differs, use:

```sh
npm install
```

Run in development:

```sh
npm run dev
```

Build desktop package:

```sh
npm run dist
```

Dependency management notes:
- Do not commit `node_modules/` to GitHub (already ignored in `.gitignore`)
- Commit `package.json` and `package-lock.json`

## Documentation

- Product and positioning: [docs/product-overview.md](docs/product-overview.md)
- Development and release: [docs/development-and-release.md](docs/development-and-release.md)
- Architecture and indexing internals: [docs/architecture-and-indexing.md](docs/architecture-and-indexing.md)
- Startup indexing flow notes: [docs/core-startup-indexing-flow.md](docs/core-startup-indexing-flow.md)

## Privacy

Recall is designed for local data ownership.

- Search/indexing runs locally
- Database is stored on your machine
- No required remote indexing service
