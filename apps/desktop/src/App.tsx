import {
  AlertCircle,
  Clock,
  ExternalLink,
  FilePlus2,
  FolderOpen,
  Globe,
  HardDrive,
  Loader2,
  Pause,
  Play,
  Search,
  SlidersHorizontal,
  Square,
  X
} from 'lucide-react';
import { useEffect, useLayoutEffect, useMemo, useRef, useState, type CSSProperties } from 'react';

type SearchResult = {
  rowid: number;
  item_id: string;
  source: string;
  title: string;
  preview: string;
  path?: string;
  file_type?: string;
  updated_at: number;
  score: number;
  metadata?: Record<string, unknown>;
};

function getUrl(result: SearchResult): string | undefined {
  return result.metadata?.url as string | undefined;
}

type SearchResponse = {
  query: string;
  elapsed_ms: number;
  total: number;
  results: SearchResult[];
  has_more?: boolean;
};

type Status = {
  kind: 'idle' | 'working' | 'error';
  text: string;
};

type IndexProgress = {
  active: boolean;
  kind?: 'fast' | 'content';
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
  elapsed_ms: number;
  last_error?: string;
};

const sourceLabels: Record<string, string> = {
  file: '文件',
  chrome: 'Chrome',
  edge: 'Edge',
  firefox: 'Firefox'
};

export function App() {
  const PAGE_SIZE = 50;

  const api = window.recall;
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [selected, setSelected] = useState(0);
  const [source, setSource] = useState('');
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [status, setStatus] = useState<Status>({ kind: 'idle', text: '' });
  const [indexProgress, setIndexProgress] = useState<IndexProgress | null>(null);
  const [indexing, setIndexing] = useState(false);
  const [cancellingIndex, setCancellingIndex] = useState(false);
  const [syncingBrowsers, setSyncingBrowsers] = useState(false);
  const [searching, setSearching] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const searchBandRef = useRef<HTMLElement>(null);
  const resultsRef = useRef<HTMLElement>(null);
  const resultsContentRef = useRef<HTMLDivElement>(null);
  const resultItemRefs = useRef<Array<HTMLElement | null>>([]);
  const statusTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const shouldResetResultsScrollRef = useRef(false);
  const searchSequenceRef = useRef(0);
  const loadMoreResultsRef = useRef<() => Promise<void>>(async () => {});

  function setStatusAuto(s: Status) {
    setStatus(s);
    if (statusTimerRef.current) clearTimeout(statusTimerRef.current);
    if (s.kind === 'idle' && s.text) {
      statusTimerRef.current = setTimeout(() => setStatus({ kind: 'idle', text: '' }), 3000);
    }
  }

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  useEffect(() => {
    if (!api) {
      return;
    }

    let cancelled = false;
    async function pollProgress() {
      try {
        const progress = await api?.indexProgress() as IndexProgress;
        if (!cancelled) {
          setIndexProgress(progress);
        }
      } catch {
        // Progress is best-effort; indexPath still owns completion/errors.
      }
    }

    void pollProgress();
    const timer = window.setInterval(() => void pollProgress(), indexing ? 300 : 1500);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [api, indexing, cancellingIndex]);

  useEffect(() => {
    if (!api) {
      setStatus({ kind: 'error', text: '请在 Electron 客户端中使用搜索' });
      return;
    }

    const trimmed = query.trim();
    if (!trimmed) {
      searchSequenceRef.current += 1;
      void api.cancelSearch().catch(() => undefined);
      setResults([]);
      setHasMore(false);
      setElapsed(null);
      setSearching(false);
      setSelected(0);
      if (resultsRef.current) {
        resultsRef.current.scrollTop = 0;
      }
      return;
    }

    const controller = new AbortController();
    const searchSequence = ++searchSequenceRef.current;
    let started = false;
    const timer = window.setTimeout(async () => {
      started = true;
      setSearching(true);
      try {
        await api.cancelSearch();
      } catch {
        // Best-effort cancel to avoid piling up stale requests.
      }
      if (controller.signal.aborted || searchSequence !== searchSequenceRef.current) {
        return;
      }
      try {
        const response = (await api.search({
          query: trimmed,
          source,
          limit: PAGE_SIZE,
          offset: 0
        })) as SearchResponse;
        if (controller.signal.aborted || searchSequence !== searchSequenceRef.current) {
          return;
        }
        shouldResetResultsScrollRef.current = true;
        setResults(response.results ?? []);
        setHasMore(Boolean(response.has_more));
        setElapsed(response.elapsed_ms);
        setSelected(0);
        if (resultsRef.current) {
          resultsRef.current.scrollTop = 0;
        }
        setStatus({ kind: 'idle', text: '' });
      } catch (error) {
        if (!controller.signal.aborted && searchSequence === searchSequenceRef.current) {
          const message = error instanceof Error ? error.message : String(error);
          if (isSearchTimeoutError(message)) {
            void api.cancelSearch().catch(() => undefined);
          }
          setStatus({ kind: 'error', text: formatSearchError(message) });
        }
      } finally {
        if (!controller.signal.aborted && searchSequence === searchSequenceRef.current) {
          setSearching(false);
        }
      }
    }, 45);

    return () => {
      controller.abort();
      window.clearTimeout(timer);
      if (started) {
        void api.cancelSearch().catch(() => undefined);
      }
    };
  }, [api, query, source]);

  const activeResult = results[selected];

  useEffect(() => {
    resultItemRefs.current.length = results.length;
  }, [results.length]);

  useEffect(() => {
    if (results.length === 0) {
      if (selected !== 0) {
        setSelected(0);
      }
      return;
    }
    if (selected >= results.length) {
      setSelected(results.length - 1);
    }
  }, [selected, results.length]);

  const sourceOptions = useMemo(() => ['', 'file', 'chrome', 'edge', 'firefox'], []);
  const trimmedQuery = query.trim();
  const hasQuery = Boolean(trimmedQuery);
  const showSourcePanel = filtersOpen || Boolean(source);
  const hasResults = hasQuery && results.length > 0;
  const showSearchLoading = searching && hasQuery;
  const showSearchError = status.kind === 'error' && Boolean(status.text);
  const visibleIndexProgress = indexProgress && !cancellingIndex && (indexProgress.active || indexing) && indexProgress.phase !== 'idle'
    ? indexProgress
    : null;
  const showMeta = Boolean(source) || Boolean(status.text) || Boolean(visibleIndexProgress) || (elapsed !== null && hasResults);
  const showResults = hasResults;

  useLayoutEffect(() => {
    if (!resultsRef.current) {
      return;
    }
    if (!shouldResetResultsScrollRef.current) {
      return;
    }
    resultsRef.current.scrollTop = 0;
    shouldResetResultsScrollRef.current = false;
  }, [results]);

  useLayoutEffect(() => {
    if (!showResults || !resultsRef.current) {
      return;
    }
    const container = resultsRef.current;
    if (selected === 0) {
      container.scrollTop = 0;
      return;
    }
    const selectedNode = resultItemRefs.current[selected];
    if (!selectedNode) {
      return;
    }

    ensureElementFullyVisible(container, selectedNode, 12);
  }, [selected, showResults]);

  useLayoutEffect(() => {
    const searchHeight = searchBandRef.current?.scrollHeight ?? 82;
    const resultsHeight = showResults
      ? getResultsHeight(resultsRef.current, resultsContentRef.current)
      : 0;
    const maxResultsHeight = 356;
    const targetHeight = Math.ceil(searchHeight + Math.min(resultsHeight, maxResultsHeight));

    void api?.setWindowHeight(targetHeight);
  }, [api, query, results.length, elapsed, source, filtersOpen, status.text, showResults, indexProgress, indexing]);

  loadMoreResultsRef.current = loadMoreResults;

  useEffect(() => {
    const container = resultsRef.current;
    if (!container || !showResults) return;
    const onScroll = () => {
      if (container.scrollHeight - container.scrollTop - container.clientHeight < 80) {
        void loadMoreResultsRef.current();
      }
    };
    container.addEventListener('scroll', onScroll, { passive: true });
    return () => container.removeEventListener('scroll', onScroll);
  }, [showResults]);

  async function loadMoreResults() {
    if (!api || searching || !hasMore) {
      return;
    }
    const trimmed = query.trim();
    if (!trimmed) {
      return;
    }
    setSearching(true);
    try {
      const response = (await api.search({
        query: trimmed,
        source,
        limit: PAGE_SIZE,
        offset: results.length
      })) as SearchResponse;
      const next = response.results ?? [];
      if (next.length > 0) {
        setResults((previous) => {
          const seen = new Set(previous.map((item) => `${item.source}:${item.item_id}:${item.rowid}`));
          const merged = previous.slice();
          for (const item of next) {
            const key = `${item.source}:${item.item_id}:${item.rowid}`;
            if (!seen.has(key)) {
              seen.add(key);
              merged.push(item);
            }
          }
          return merged;
        });
      }
      setHasMore(Boolean(response.has_more));
      setElapsed(response.elapsed_ms);
      setStatus({ kind: 'idle', text: '' });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (isSearchTimeoutError(message)) {
        void api.cancelSearch().catch(() => undefined);
      }
      setStatus({ kind: 'error', text: formatSearchError(message) });
    } finally {
      setSearching(false);
      inputRef.current?.focus();
    }
  }

  async function indexFolder() {
    if (!api) {
      setStatusAuto({ kind: 'error', text: '请在 Electron 客户端中使用索引' });
      return;
    }
    if (syncingBrowsers) {
      return;
    }
    const folder = await api.chooseFolder();
    if (!folder) {
      return;
    }
    setIndexing(true);
    setIndexProgress({
      active: true,
      phase: 'starting',
      path: folder,
      total: 0,
      scanned: 0,
      indexed: 0,
      skipped: 0,
      written: 0,
      workers: 0,
      files_per_sec: 0,
      eta_ms: 0,
      elapsed_ms: 0
    });
    setStatus({ kind: 'working', text: '正在索引' });
    try {
      const res = await api.indexPath({ path: folder }) as { indexed?: number; canceled?: boolean };
      if (res?.canceled) {
        setIndexProgress(null);
        setStatusAuto({ kind: 'idle', text: '索引已取消' });
        return;
      }
      const count = res?.indexed ?? 0;
      const finalProgress = await api.indexProgress().catch(() => null) as IndexProgress | null;
      if (finalProgress) {
        setIndexProgress(finalProgress);
      }
      setStatusAuto({ kind: 'idle', text: `索引完成，共 ${count} 个文件` });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (isIndexCanceledError(message)) {
        setIndexProgress(null);
        setStatusAuto({ kind: 'idle', text: '索引已取消' });
      } else {
        setStatusAuto({ kind: 'error', text: message });
      }
    } finally {
      setIndexing(false);
      setCancellingIndex(false);
    }
  }

  async function cancelIndexing() {
    if (!api || !indexing || cancellingIndex) {
      return;
    }
    setCancellingIndex(true);
    setIndexProgress(null);
    setStatus({ kind: 'working', text: '正在取消索引...' });
    try {
      await api.cancelIndex();
    } catch (error) {
      setStatusAuto({ kind: 'error', text: error instanceof Error ? error.message : String(error) });
      setCancellingIndex(false);
    }
  }

  async function toggleContentIndex(progress: IndexProgress) {
    if (!api || progress.kind !== 'content') {
      return;
    }
    try {
      if (progress.phase === 'paused') {
        await api.resumeContentIndex();
      } else {
        await api.pauseContentIndex();
      }
      const next = await api.indexProgress().catch(() => null) as IndexProgress | null;
      if (next) {
        setIndexProgress(next);
      }
    } catch (error) {
      setStatusAuto({ kind: 'error', text: error instanceof Error ? error.message : String(error) });
    }
  }

  async function syncBrowsers() {
    if (!api) {
      setStatusAuto({ kind: 'error', text: '请在 Electron 客户端中同步浏览器' });
      return;
    }
    if (indexing || cancellingIndex || syncingBrowsers) {
      return;
    }
    setSyncingBrowsers(true);
    setStatus({ kind: 'working', text: '正在同步浏览器...' });
    try {
      await api.syncBrowsers();
      setStatusAuto({ kind: 'idle', text: '同步完成' });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (isIndexCanceledError(message)) {
        setStatusAuto({ kind: 'idle', text: '浏览器同步已取消' });
      } else {
        setStatusAuto({ kind: 'error', text: message });
      }
    } finally {
      setSyncingBrowsers(false);
    }
  }

  async function cancelSyncBrowsers() {
    if (!api || !syncingBrowsers) {
      return;
    }
    setStatus({ kind: 'working', text: '正在取消浏览器同步...' });
    try {
      await api.cancelSyncBrowsers();
    } catch (error) {
      setStatusAuto({ kind: 'error', text: error instanceof Error ? error.message : String(error) });
    }
  }

  function handleKeyDown(event: React.KeyboardEvent<HTMLInputElement>) {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      if (!results.length) return;
      if (selected === results.length - 1 && hasMore && !searching) {
        void loadMoreResults();
      }
      setSelected((value) => Math.min(value + 1, results.length - 1));
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      if (!results.length) return;
      setSelected((value) => Math.max(value - 1, 0));
    }
    if (event.key === 'Enter' && activeResult) {
      const url = getUrl(activeResult);
      if (activeResult.path) {
        if (event.shiftKey) {
          void api?.showItemInFolder(activeResult.path);
          return;
        }
        void api?.openPath(activeResult.path);
      } else if (url) {
        void api?.openUrl(url);
      }
    }
    if (event.key === 'Escape') {
      setQuery('');
      void api?.hide();
    }
  }

  return (
    <main className={showResults || showSourcePanel || showMeta ? 'shell expanded' : 'shell compact'}>
      <section className="searchBand" ref={searchBandRef}>
        <div className="dragbar" />
        <div className="searchRow">
          <Search className="searchIcon" size={22} aria-hidden="true" />
          <input
            ref={inputRef}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="搜索本地记忆"
            spellCheck={false}
          />
          <div
            className={showSearchError ? 'searchIndicator error' : showSearchLoading ? 'searchIndicator loading' : 'searchIndicator'}
            title={showSearchError ? status.text : showSearchLoading ? '搜索中' : ''}
            aria-live="polite"
          >
            {showSearchLoading ? <Loader2 size={13} /> : null}
            {showSearchError ? <AlertCircle size={13} /> : null}
          </div>
          <button
            className={query ? 'iconButton' : 'iconButton ghost'}
            title="清空"
            tabIndex={query ? 0 : -1}
            aria-hidden={!query}
            onClick={() => setQuery('')}
          >
            <X size={18} />
          </button>
          <button
            className={source || filtersOpen ? 'iconButton active' : 'iconButton'}
            title="筛选来源"
            aria-expanded={showSourcePanel}
            onClick={() => setFiltersOpen((open) => !open)}
          >
            <SlidersHorizontal size={18} />
          </button>
          <button
            className={indexing ? 'iconButton active' : 'iconButton'}
            title={syncingBrowsers ? '浏览器同步进行中，暂不可索引' : indexing ? (cancellingIndex ? '正在取消索引' : '取消索引') : '索引文件夹'}
            disabled={syncingBrowsers}
            onClick={() => {
              if (indexing) {
                void cancelIndexing();
                return;
              }
              void indexFolder();
            }}
          >
            {indexing
              ? (cancellingIndex ? <Loader2 size={18} className="spinIcon" /> : <Square size={16} />)
              : <FolderOpen size={18} />}
          </button>
          <button
            className={syncingBrowsers ? 'iconButton active' : 'iconButton'}
            title={indexing ? '索引进行中，暂不可同步浏览器' : syncingBrowsers ? '取消浏览器同步' : '同步浏览器'}
            disabled={indexing || cancellingIndex}
            onClick={() => {
              if (syncingBrowsers) {
                void cancelSyncBrowsers();
                return;
              }
              void syncBrowsers();
            }}
          >
            {syncingBrowsers ? <Square size={16} /> : <Globe size={18} />}
          </button>
        </div>
        {showMeta ? (
          <div className="recallMeta">
            <div className="sourceSummary">
              {source ? sourceLabels[source] : ''}
            </div>
            <div className="statusCluster">
              {visibleIndexProgress ? (
                <>
                  <IndexProgressView progress={visibleIndexProgress} />
                  {visibleIndexProgress.kind === 'content' ? (
                    <button
                      className="progressIconButton"
                      title={visibleIndexProgress.phase === 'paused' ? '继续全文索引' : '暂停全文索引'}
                      onClick={() => void toggleContentIndex(visibleIndexProgress)}
                    >
                      {visibleIndexProgress.phase === 'paused' ? <Play size={13} /> : <Pause size={13} />}
                    </button>
                  ) : null}
                </>
              ) : status.text ? (
                <span className={status.kind}>{status.text}</span>
              ) : null}
              {elapsed !== null && (hasResults || hasQuery) ? <span>{elapsed.toFixed(1)} ms</span> : null}
            </div>
          </div>
        ) : null}
        {showSourcePanel ? (
          <div className="sourcePanel" role="group" aria-label="来源筛选">
            {sourceOptions.map((option) => (
              <button
                key={option || 'all'}
                className={source === option ? 'filter active' : 'filter'}
                aria-pressed={source === option}
                onClick={() => {
                  setSource(option);
                  setFiltersOpen(false);
                }}
              >
                {option ? sourceLabels[option] : '全部'}
              </button>
            ))}
          </div>
        ) : null}
      </section>

      {showResults ? (
        <section className="results" ref={resultsRef} aria-live="polite">
          <div className="resultStack" ref={resultsContentRef}>
            <div className="resultCount">
              {hasMore ? `已加载 ${results.length} 条` : `共 ${results.length} 条`}
            </div>
            {results.map((result, index) => (
              <article
                key={`${result.source}-${result.item_id}-${result.rowid}`}
                className={index === selected ? 'result selected' : 'result'}
                ref={(node) => {
                  resultItemRefs.current[index] = node;
                }}
                onClick={() => setSelected(index)}
                onDoubleClick={() => openResult(result)}
              >
                <div className="resultTop">
                  <div className="resultTitle">{result.title || result.item_id}</div>
                  <div className="resultActions">
                    {result.path ? (
                      <>
                        <button
                          className="miniIconButton"
                          title="打开文件"
                          onClick={() => openResult(result)}
                        >
                          <ExternalLink size={14} />
                        </button>
                        <button
                          className="miniIconButton"
                          title="在文件夹中显示"
                          onClick={() => showResultInFolder(result)}
                        >
                          <FolderOpen size={14} />
                        </button>
                      </>
                    ) : getUrl(result) ? (
                      <button
                        className="miniIconButton"
                        title="在浏览器中打开"
                        onClick={() => { const u = getUrl(result); if (u) void window.recall?.openUrl(u); }}
                      >
                        <ExternalLink size={14} />
                      </button>
                    ) : null}
                    <div className="sourcePill">{sourceLabels[result.source] ?? result.source}</div>
                  </div>
                </div>
                <p>{result.preview}</p>
                <div className="meta">
                  <span>
                    <Clock size={13} />
                    {formatTime(result.updated_at)}
                  </span>
                  {result.path ? (
                    <button className="path pathButton" title="在文件夹中显示" onClick={() => showResultInFolder(result)}>
                      <HardDrive size={13} />
                      {result.path}
                    </button>
                  ) : getUrl(result) ? (
                    <button className="path pathButton" title="在浏览器中打开" onClick={() => { const u = getUrl(result); if (u) void window.recall?.openUrl(u); }}>
                      <ExternalLink size={13} />
                      {getUrl(result)}
                    </button>
                  ) : null}
                  {result.file_type ? (
                    <span>
                      <FilePlus2 size={13} />
                      {result.file_type}
                    </span>
                  ) : null}
                </div>
              </article>
            ))}
            {hasMore ? (
              <div className="loadMoreWrap">
                <button
                  className="loadMoreButton"
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => void loadMoreResults()}
                >
                  {searching ? '加载中...' : '加载更多'}
                </button>
              </div>
            ) : null}
          </div>
        </section>
      ) : null}
    </main>
  );
}

function openResult(result: SearchResult) {
  if (result.path) {
    void window.recall?.openPath(result.path);
  } else {
    const url = getUrl(result);
    if (url) void window.recall?.openUrl(url);
  }
}

function showResultInFolder(result: SearchResult) {
  if (result.path) {
    void window.recall?.showItemInFolder(result.path);
  }
}

function getResultsHeight(results: HTMLElement | null, content: HTMLElement | null) {
  if (!results || !content) {
    return 0;
  }
  const style = window.getComputedStyle(results);
  const paddingTop = Number.parseFloat(style.paddingTop) || 0;
  const paddingBottom = Number.parseFloat(style.paddingBottom) || 0;
  return content.scrollHeight + paddingTop + paddingBottom;
}

function ensureElementFullyVisible(container: HTMLElement, item: HTMLElement, padding = 0) {
  const itemTop = containerRelativeTop(container, item);
  const itemBottom = itemTop + item.offsetHeight;
  const visibleTop = container.scrollTop + padding;
  const visibleBottom = container.scrollTop + container.clientHeight - padding;
  const maxScrollTop = Math.max(0, container.scrollHeight - container.clientHeight);

  if (itemTop < visibleTop) {
    container.scrollTop = Math.max(0, itemTop - padding);
    return;
  }
  if (itemBottom > visibleBottom) {
    const next = itemBottom - container.clientHeight + padding;
    container.scrollTop = Math.min(maxScrollTop, Math.max(0, next));
  }
}

function containerRelativeTop(container: HTMLElement, item: HTMLElement) {
  const containerRect = container.getBoundingClientRect();
  const itemRect = item.getBoundingClientRect();
  return itemRect.top - containerRect.top + container.scrollTop;
}

function formatTime(unixSeconds: number) {
  if (!unixSeconds) {
    return '';
  }
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  }).format(new Date(unixSeconds * 1000));
}

function formatIndexProgress(progress: IndexProgress) {
  const phaseLabels: Record<string, string> = {
    starting: '准备索引',
    scanning: '扫描文件',
    extracting: '抽取文本',
    writing: '写入索引',
    indexing: '写入索引',
    finalizing: '整理索引',
    paused: '已暂停',
    idle: '索引完成'
  };
  const kind = progress.kind === 'content' ? '全文索引' : '文件索引';
  const total = progress.total > 0 ? `/${progress.total}` : '';
  const speed = progress.files_per_sec > 0 ? ` · ${progress.files_per_sec.toFixed(1)} 文件/秒` : '';
  const eta = progress.eta_ms > 0 ? ` · ETA ${formatDuration(progress.eta_ms)}` : '';
  const workers = progress.workers > 0 ? ` · ${progress.workers} workers` : '';
  const current = progress.current ? ` · ${basename(progress.current)}` : '';
  return `${kind} · ${phaseLabels[progress.phase] ?? '正在索引'} · ${progress.scanned}${total} · 写入 ${progress.written} · 跳过 ${progress.skipped}${speed}${eta}${workers}${current}`;
}

function IndexProgressView({ progress }: { progress: IndexProgress }) {
  const total = progress.total > 0 ? progress.total : 0;
  const scanned = Math.max(0, progress.scanned);
  const ratio = total > 0 ? Math.min(1, scanned / total) : null;
  const percent = ratio === null ? null : Math.round(ratio * 100);
  const summary = progressSummaryLabel(progress, scanned, total, percent);
  const animated = progress.active && progress.phase !== 'idle' && progress.phase !== 'paused';
  const isStarting = progress.phase === 'starting';
  const fillWidth = ratio === null ? '28%' : `${Math.max(2, ratio * 100)}%`;
  const barClass = ratio === null
    ? `indexProgressBar pending${animated ? ' running' : ''}`
    : `indexProgressBar${animated ? ' running' : ''}`;
  const fillClass = animated ? 'indexProgressFill animated' : 'indexProgressFill';
  const barStyle = { '--progress-width': fillWidth } as CSSProperties;
  return (
    <div className="indexProgress" title={formatIndexProgress(progress)}>
      <div className="indexProgressTop">
        <span>{summary}</span>
      </div>
      {!isStarting ? (
        <div className={barClass} style={barStyle} aria-hidden="true">
          <div
            className={fillClass}
            style={{ width: fillWidth }}
          />
        </div>
      ) : (
        <div
          className="indexProgressBar running"
          style={{ ['--progress-width' as any]: '100%' } as CSSProperties}
          aria-hidden="true"
        >
          <div className="indexProgressFill" style={{ width: '0%' }} />
        </div>
      )}
    </div>
  );
}

function progressSummaryLabel(progress: IndexProgress, scanned: number, total: number, percent: number | null) {
  const phaseLabels: Record<string, string> = {
    starting: '准备索引',
    scanning: '扫描文件',
    extracting: '抽取文本',
    writing: '写入索引',
    indexing: '写入索引',
    finalizing: '整理索引',
    paused: '已暂停',
    idle: '索引完成'
  };
  const prefix = progress.kind === 'content' ? '全文索引' : '文件索引';
  const parts = [`${prefix} · ${phaseLabels[progress.phase] ?? '正在索引'}`];
  if (progress.phase !== 'starting') {
    parts.push(total > 0 ? `${scanned}/${total}` : `已扫描 ${scanned}`);
  }
  if (percent !== null) {
    parts.push(`${percent}%`);
  }
  if (progress.files_per_sec > 0) {
    parts.push(`${progress.files_per_sec.toFixed(1)} 文件/秒`);
  }
  if (progress.eta_ms > 0) {
    parts.push(`ETA ${formatDuration(progress.eta_ms)}`);
  }
  return parts.join('  ');
}

function basename(path: string) {
  const normalized = path.replace(/\\/g, '/');
  return normalized.slice(normalized.lastIndexOf('/') + 1);
}

function formatDuration(ms: number) {
  const seconds = Math.max(1, Math.round(ms / 1000));
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return rest ? `${minutes}m${rest}s` : `${minutes}m`;
}

function isIndexCanceledError(message: string) {
  const lower = message.toLowerCase();
  return lower.includes('context canceled') || lower.includes('context cancelled');
}

function isSearchTimeoutError(message: string) {
  return message.toLowerCase().includes('core request timed out: search');
}

function formatSearchError(message: string) {
  if (isSearchTimeoutError(message)) {
    return '搜索超时，已自动取消本次请求，请重试或缩小范围';
  }
  return message;
}

