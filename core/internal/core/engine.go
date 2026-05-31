package core

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	"recall/core/internal/adapters"
	"recall/core/internal/extract"
	"recall/core/internal/indexer"
	"recall/core/internal/model"
	"recall/core/internal/storage"
)

// Config 包含运行时路径和提取设置。
type Config struct {
	DBPath   string
	TikaURL  string
	MaxBytes int64
}

const (
	fingerprintKindPath    = "path:"
	fingerprintKindContent = "content:"
)

// Engine 协调适配器、索引和搜索。
type Engine struct {
	store            *storage.Store
	indexer          *indexer.Indexer
	extractor        extract.Extractor
	browsers         []model.DataAdapter
	startedAt        time.Time
	maxBytes         int64
	indexMu          sync.Mutex
	runMu            sync.Mutex
	indexRunCancel   context.CancelFunc
	syncRunCancel    context.CancelFunc
	searchRunCancel  context.CancelFunc
	searchRunID      uint64
	contentRunMu     sync.Mutex
	contentRunCancel context.CancelFunc
	contentRunID     uint64
	contentPaused    atomic.Bool
	progressMu       sync.RWMutex
	progress         model.IndexProgress
	progressRunSeq   atomic.Uint64
	activeProgressID atomic.Uint64
	// 扫描期间更新的无锁计数器；IndexProgress 会读取它们。
	progressTotal   atomic.Int64
	progressScanned atomic.Int64
	progressIndexed atomic.Int64
	progressSkipped atomic.Int64
	progressWritten atomic.Int64
	// 文件监控
	watcher     *fsnotify.Watcher
	watchMu     sync.RWMutex
	watchedDirs map[string]struct{}
	ctx         context.Context
	cancel      context.CancelFunc
}

// DefaultConfig 从环境变量解析仅本地使用的默认值。
func DefaultConfig() Config {
	return Config{
		DBPath:   defaultDBPath(),
		TikaURL:  os.Getenv("RECALL_TIKA_URL"),
		MaxBytes: extract.DefaultMaxBytes,
	}
}

// New 构造本地优先的搜索引擎。
func New(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = extract.DefaultMaxBytes
	}
	store, err := storage.Open(ctx, storage.Config{Path: cfg.DBPath})
	if err != nil {
		return nil, err
	}

	engineCtx, cancel := context.WithCancel(context.Background())

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("create watcher: %w", err)
	}

	engine := &Engine{
		store:     store,
		indexer:   indexer.New(store),
		extractor: extract.Default(extract.Config{MaxBytes: cfg.MaxBytes, TikaURL: cfg.TikaURL}),
		browsers: []model.DataAdapter{
			adapters.NewBrowserAdapter(adapters.BrowserChrome),
			adapters.NewBrowserAdapter(adapters.BrowserEdge),
			adapters.NewBrowserAdapter(adapters.BrowserFirefox),
		},
		startedAt:   time.Now(),
		maxBytes:    cfg.MaxBytes,
		watcher:     watcher,
		watchedDirs: map[string]struct{}{},
		ctx:         engineCtx,
		cancel:      cancel,
	}

	// 恢复此前监控的路径，并执行增量补齐。
	if watchedPaths, watchErr := store.GetWatchedPaths(ctx); watchErr == nil {
		for _, root := range watchedPaths {
			if !isVolumeRoot(root) {
				engine.addToWatcher(root)
			}
		}
		if len(watchedPaths) > 0 {
			// 延后补齐，让启动保持响应，并让搜索可以立即使用现有索引。
			go engine.scheduleWatchedResync(watchedPaths)
		}
	}
	go engine.watcherLoop()
	go engine.periodicOptimize()
	// 预热 SQLite FTS5 页缓存，让用户第一次搜索命中内存页，
	// 而不是从磁盘读取冷索引段。
	go store.WarmCache(engineCtx)

	return engine, nil
}

// scheduleWatchedResync 延后启动补齐任务，避免与刚启动后的初次搜索争抢资源。
func (e *Engine) scheduleWatchedResync(roots []string) {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	select {
	case <-e.ctx.Done():
		return
	case <-timer.C:
	}

	e.resyncWatched(roots)
}

func (e *Engine) scheduleContentBackfill(root string, maxBytes int64) {
	go func() {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-e.ctx.Done():
			return
		case <-timer.C:
		}
		e.backfillContent(root, maxBytes)
	}()
}

func (e *Engine) backfillContent(root string, maxBytes int64) {
	if e.ctx.Err() != nil {
		return
	}
	runCtx, cancel := context.WithCancel(e.ctx)
	e.contentRunMu.Lock()
	if e.contentRunCancel != nil {
		e.contentRunCancel()
	}
	e.contentRunID++
	runID := e.contentRunID
	e.contentRunCancel = cancel
	e.contentRunMu.Unlock()
	defer func() {
		cancel()
		e.contentRunMu.Lock()
		if e.contentRunID == runID {
			e.contentRunCancel = nil
		}
		e.contentRunMu.Unlock()
	}()

	pathKey := contentPathSyncKey(root)
	lastSync, _ := e.store.GetSyncTime(runCtx, pathKey)
	if lastSync > 0 {
		lastSync--
	}
	adapter := adapters.NewFileAdapter([]string{root}, e.extractor, maxBytes)
	adapter.ContentOnly = true
	if _, err := e.syncFileAdapter(runCtx, adapter, lastSync, pathKey); err != nil {
		log.Printf("content backfill %s: %v", root, err)
	}
}

// Close 释放引擎资源。
func (e *Engine) Close() error {
	e.cancel()
	_ = e.watcher.Close()
	return e.store.Close()
}

// Health 返回给渲染进程使用的运行元数据。
func (e *Engine) Health(ctx context.Context) (map[string]any, error) {
	version, err := e.store.SQLiteVersion(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":             true,
		"sqlite_version": version,
		"uptime_ms":      time.Since(e.startedAt).Milliseconds(),
		"local_only":     true,
	}, nil
}

// Search 执行有界 FTS5 搜索。
func (e *Engine) Search(ctx context.Context, req model.SearchRequest) (model.SearchResponse, error) {
	start := time.Now()
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return model.SearchResponse{Query: req.Query, Results: []model.SearchResult{}}, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	runID := e.startSearchRun(cancel)
	defer e.finishSearchRun(runID, cancel)

	ftsQuery := indexer.BuildFTSQuery(req.Query)
	results, hasMore, err := e.store.Search(runCtx, req, ftsQuery)
	if err != nil {
		return model.SearchResponse{}, err
	}

	for i := range results {
		results[i].Preview = makeSnippet(results[i].Preview, req.Query)
	}
	return model.SearchResponse{
		Query:     req.Query,
		ElapsedMS: float64(time.Since(start).Microseconds()) / 1000,
		Total:     len(results),
		Results:   results,
		HasMore:   hasMore,
	}, nil
}

// CancelSearch 取消正在运行的搜索请求。
func (e *Engine) CancelSearch(ctx context.Context) (map[string]any, error) {
	_ = ctx
	e.runMu.Lock()
	cancel := e.searchRunCancel
	e.runMu.Unlock()
	if cancel == nil {
		return map[string]any{"ok": true, "canceled": false}, nil
	}
	cancel()
	return map[string]any{"ok": true, "canceled": true}, nil
}

func (e *Engine) startSearchRun(cancel context.CancelFunc) uint64 {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if e.searchRunCancel != nil {
		e.searchRunCancel()
	}
	e.searchRunID++
	e.searchRunCancel = cancel
	return e.searchRunID
}

func (e *Engine) finishSearchRun(runID uint64, cancel context.CancelFunc) {
	cancel()
	e.runMu.Lock()
	defer e.runMu.Unlock()
	if e.searchRunID == runID {
		e.searchRunCancel = nil
	}
}

// IndexPath 索引用户选择的文件或目录；重复调用时会增量处理。
func (e *Engine) IndexPath(ctx context.Context, req model.IndexPathRequest) (model.SyncSummary, error) {
	if strings.TrimSpace(req.Path) == "" {
		return model.SyncSummary{}, fmt.Errorf("path is required")
	}
	e.indexMu.Lock()
	defer e.indexMu.Unlock()
	e.cancelContentBackfill()

	maxBytes := e.maxBytes
	if req.MaxBytes > 0 {
		maxBytes = req.MaxBytes
	}

	pathKey := pathSyncKey(req.Path)
	lastSync, _ := e.store.GetSyncTime(ctx, pathKey)
	runCtx, cancel := context.WithCancel(ctx)
	e.setIndexRunCancel(cancel)
	defer e.setIndexRunCancel(nil)
	defer cancel()

	adapter := adapters.NewFileAdapter([]string{req.Path}, e.extractor, maxBytes)
	volumeRoot := isVolumeRoot(req.Path)
	adapter.PathOnly = volumeRoot
	summary, err := e.syncFileAdapter(runCtx, adapter, lastSync, pathKey)
	if err == nil {
		if volumeRoot {
			_ = e.store.AddWatchedPath(ctx, req.Path)
			e.scheduleContentBackfill(req.Path, maxBytes)
		} else {
			e.enableWatch(ctx, req.Path)
		}
	}
	return summary, err
}

// CancelIndexPath 取消正在运行的 index_path 请求。
func (e *Engine) CancelIndexPath(ctx context.Context) (map[string]any, error) {
	_ = ctx
	e.runMu.Lock()
	cancel := e.indexRunCancel
	e.runMu.Unlock()
	if cancel == nil {
		return map[string]any{"ok": true, "canceled": false}, nil
	}
	cancel()
	e.markProgressCanceled()
	return map[string]any{"ok": true, "canceled": true}, nil
}

func (e *Engine) cancelContentBackfill() {
	e.contentRunMu.Lock()
	cancel := e.contentRunCancel
	e.contentRunMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// PauseContentIndex 在下一个协作检查点暂停后台全文补齐。
// 搜索和前台文件索引仍可使用。
func (e *Engine) PauseContentIndex(ctx context.Context) (map[string]any, error) {
	_ = ctx
	e.contentPaused.Store(true)
	e.progressMu.Lock()
	if e.progress.Active && e.progress.Kind == "content" {
		e.progress.Phase = "paused"
		e.progress.UpdatedAt = time.Now().UnixMilli()
	}
	e.progressMu.Unlock()
	return map[string]any{"ok": true, "paused": true}, nil
}

// ResumeContentIndex 恢复已暂停的后台全文补齐。
func (e *Engine) ResumeContentIndex(ctx context.Context) (map[string]any, error) {
	_ = ctx
	e.contentPaused.Store(false)
	e.progressMu.Lock()
	if e.progress.Active && e.progress.Kind == "content" && e.progress.Phase == "paused" {
		e.progress.Phase = "scanning"
		e.progress.UpdatedAt = time.Now().UnixMilli()
	}
	e.progressMu.Unlock()
	return map[string]any{"ok": true, "paused": false}, nil
}

// SyncBrowsers 索引受支持配置中的本地浏览器历史。
func (e *Engine) SyncBrowsers(ctx context.Context) ([]model.SyncSummary, error) {
	e.indexMu.Lock()
	defer e.indexMu.Unlock()
	e.cancelContentBackfill()

	runCtx, cancel := context.WithCancel(ctx)
	e.setSyncRunCancel(cancel)
	defer e.setSyncRunCancel(nil)
	defer cancel()

	summaries := make([]model.SyncSummary, 0, len(e.browsers))
	for _, adapter := range e.browsers {
		if err := runCtx.Err(); err != nil {
			return summaries, err
		}
		if !adapter.IsAvailable() {
			continue
		}
		lastSync, err := e.store.GetSyncTime(runCtx, adapter.ID())
		if err != nil {
			return summaries, err
		}
		summary, err := e.syncAdapter(runCtx, adapter, lastSync)
		if err != nil {
			return summaries, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

// CancelSyncBrowsers 取消正在运行的 sync_browsers 请求。
func (e *Engine) CancelSyncBrowsers(ctx context.Context) (map[string]any, error) {
	_ = ctx
	e.runMu.Lock()
	cancel := e.syncRunCancel
	e.runMu.Unlock()
	if cancel == nil {
		return map[string]any{"ok": true, "canceled": false}, nil
	}
	cancel()
	return map[string]any{"ok": true, "canceled": true}, nil
}

// Optimize 执行 SQLite 维护。
func (e *Engine) Optimize(ctx context.Context) (map[string]any, error) {
	if err := e.store.Optimize(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// IndexProgress 返回最新的实时索引状态快照。
// 计数从无锁原子计数器读取，ETA 会即时计算。
func (e *Engine) IndexProgress(ctx context.Context) (model.IndexProgress, error) {
	e.progressMu.RLock()
	p := e.progress
	e.progressMu.RUnlock()
	if p.Active {
		p.Total = int(e.progressTotal.Load())
		p.Scanned = int(e.progressScanned.Load())
		p.Indexed = int(e.progressIndexed.Load())
		p.Skipped = int(e.progressSkipped.Load())
		p.Written = int(e.progressWritten.Load())
		now := time.Now().UnixMilli()
		if p.StartedAt > 0 {
			elapsed := float64(now - p.StartedAt)
			p.ElapsedMS = elapsed
			if elapsed > 0 {
				p.FilesPerSec = float64(p.Scanned) / (elapsed / 1000)
				remaining := p.Total - p.Scanned
				if remaining > 0 && p.FilesPerSec > 0 {
					p.EtaMS = float64(remaining) / p.FilesPerSec * 1000
				}
			}
		}
	}
	return p, nil
}

// syncAdapter 从一个适配器拉取增量数据并建立索引。
func (e *Engine) syncAdapter(
	ctx context.Context,
	adapter model.DataAdapter,
	lastSync int64,
) (model.SyncSummary, error) {
	start := time.Now()
	items, err := adapter.GetIncrementalData(lastSync)
	if err != nil {
		return model.SyncSummary{}, err
	}

	summary := model.SyncSummary{AdapterID: adapter.ID(), Scanned: len(items)}
	var maxUpdated int64
	const batchSize = 100
	batch := make([]storage.PreparedItem, 0, batchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := e.store.UpsertItems(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		if strings.TrimSpace(item.Content) == "" {
			summary.Skipped++
			continue
		}
		chunks := e.indexer.PrepareItem(&item)
		if chunks == nil {
			summary.Skipped++
			continue
		}
		batch = append(batch, storage.PreparedItem{Item: item, Chunks: chunks})
		summary.Indexed++
		if item.UpdatedAt > maxUpdated {
			maxUpdated = item.UpdatedAt
		}
		if len(batch) >= batchSize {
			if err := flushBatch(); err != nil {
				return summary, err
			}
		}
	}
	if err := flushBatch(); err != nil {
		return summary, err
	}

	if maxUpdated > 0 {
		if err := e.store.SetSyncTime(ctx, adapter.ID(), maxUpdated); err != nil {
			return summary, err
		}
	}
	summary.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
	return summary, nil
}

// syncFileAdapter 使用批量事务将文件条目流式送入索引器，
// 避免每个文件提交一次的开销。
func (e *Engine) syncFileAdapter(
	ctx context.Context,
	adapter *adapters.FileAdapter,
	lastSync int64,
	pathKey string,
) (model.SyncSummary, error) {
	start := time.Now()
	summary := model.SyncSummary{AdapterID: adapter.ID()}
	var maxUpdated int64

	maxWorkers := fileIndexMaxWorkers()
	if adapter.ContentOnly {
		maxWorkers = contentBackfillMaxWorkers()
	}
	progressID := e.startProgress(progressKindForAdapter(adapter), strings.Join(adapter.Roots, ", "), maxWorkers)
	fingerprints, err := e.store.LoadFileFingerprints(ctx, adapter.Roots)
	if err != nil {
		e.finishProgress(progressID, summary, err)
		return summary, err
	}

	// BulkSession 为每个分片运行一个写入协程。分片之间没有每批次屏障，
	// 因而最慢的分片不会拖住更快的分片；文件指纹也随分块写入对应分片，
	// 不再经过单独的主库写入器。这能在索引增长到数万文件后仍保持较高吞吐。
	session := e.store.BeginBulk(ctx, func(written int) {
		e.addProgressWritten(progressID, written)
	})
	sessionClosed := false
	closeSession := func() error {
		if sessionClosed {
			return nil
		}
		sessionClosed = true
		return session.Close()
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type preparedResult struct {
		entry storage.PreparedItem
		item  model.DataItem
		ok    bool
	}

	type workItem struct {
		candidate adapters.FileCandidate
		known     bool
	}

	// 较宽的缓冲区可以平滑大规模扫描时的生产 / 消费突发。
	paths := make(chan workItem, 1024)
	prepared := make(chan preparedResult, 1024)
	errCh := make(chan error, 1)
	var fastSkipped atomic.Int64
	var activeWorkers atomic.Int32

	var workerWG sync.WaitGroup

	// spawnWorker 启动一个提取协程，从共享 paths 通道读取任务。
	spawnWorker := func() {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			send := func(result preparedResult) bool {
				select {
				case <-workCtx.Done():
					return false
				case prepared <- result:
					return true
				}
			}
			for item := range paths {
				if workCtx.Err() != nil {
					return
				}
				if err := e.waitContentBackfillIfPaused(workCtx, adapter, progressID); err != nil {
					return
				}
				entry, dataItem, ok := e.prepareFileCandidate(workCtx, adapter, item.candidate)
				if !ok {
					if !send(preparedResult{}) {
						return
					}
					continue
				}
				if !item.known {
					entry.IsNew = true
				}
				// 对工作线程已物化的分块，在写入线程外预计算 CJK 二元词。
				for i := range entry.Chunks {
					if entry.Chunks[i].CJKGrams == "" {
						entry.Chunks[i].CJKGrams = storage.PrecomputeCJKGrams(entry.Chunks[i])
					}
				}
				if !send(preparedResult{
					entry: entry,
					item:  dataItem,
					ok:    true,
				}) {
					return
				}
			}
		}()
	}

	// 一开始就启用完整工作线程池，让吞吐从启动阶段就最大化。
	// 索引过程受 CPU 限制（分词 + FTS5 写入），而不只是 IO 限制，
	// 所以即使在 HDD 上更多工作线程也有帮助。
	for i := 0; i < maxWorkers; i++ {
		activeWorkers.Add(1)
		spawnWorker()
	}

	// 自适应扩容：预热后测量文件/秒，如果磁盘表现接近 SSD，就增加工作线程。
	// HDD 通常最高约 150 文件/秒；SSD 处理小文本文件通常超过 500 文件/秒。
	// 只统计实际送到工作线程的文件，排除快速跳过的未变化文件，
	// 避免 HDD 上重新索引时误触发 SSD 模式。
	const (
		warmupDuration = 10 * time.Second
		ssdThreshold   = 300.0 // 超过该文件/秒就按 SSD 处理
		ssdMaxWorkers  = 24    // SSD 突发扩容上限
	)
	go func() {
		timer := time.NewTimer(warmupDuration)
		defer timer.Stop()
		select {
		case <-workCtx.Done():
			return
		case <-timer.C:
		}
		// 使用 progressIndexed+progressSkipped（工作线程处理过的文件），
		// 而不是包含快速跳过未变化文件的 progressScanned。
		processed := e.progressIndexed.Load() + e.progressSkipped.Load()
		if processed == 0 {
			return
		}
		fps := float64(processed) / warmupDuration.Seconds()
		if fps < ssdThreshold {
			return // HDD 类速度，当前工作线程数已经合适
		}
		// 检测到 SSD，扩容到默认上限之上
		current := int(activeWorkers.Load())
		toAdd := ssdMaxWorkers - current
		for i := 0; i < toAdd; i++ {
			activeWorkers.Add(1)
			spawnWorker()
		}
		// 只更新工作线程数；不要调用 startProgress，
		// 否则会重置所有计数并把阶段改回 "starting"。
		e.progressMu.Lock()
		if e.activeProgressID.Load() == progressID && e.progress.Active {
			e.progress.Workers = int(activeWorkers.Load())
		}
		e.progressMu.Unlock()
	}()

	go func() {
		e.setProgressPhase(progressID, "scanning")
		err := adapter.WalkIncrementalCandidates(workCtx, lastSync, func(candidate adapters.FileCandidate) (bool, bool) {
			if err := e.waitContentBackfillIfPaused(workCtx, adapter, progressID); err != nil {
				return true, false
			}
			kind := e.fingerprintKindForCandidate(adapter, candidate)
			if kind == "" {
				fastSkipped.Add(1)
				e.addProgressCandidate(progressID, candidate.Path)
				e.addProgressCounts(progressID, 1, 0, 1)
				return true, false
			}
			fp, ok := fingerprints[fingerprintPath(candidate.Path)]
			if !ok {
				return false, false
			}
			if fp.Size == candidate.Size && fp.ModTimeNS == candidate.ModTimeNS && fingerprintSatisfies(fp.ContentHash, kind) {
				fastSkipped.Add(1)
				e.addProgressCandidate(progressID, candidate.Path)
				e.addProgressCounts(progressID, 1, 0, 1)
				return true, true
			}
			return false, true
		}, func(candidate adapters.FileCandidate) error {
			if err := e.waitContentBackfillIfPaused(workCtx, adapter, progressID); err != nil {
				return err
			}
			e.addProgressCandidate(progressID, candidate.Path)
			_, known := fingerprints[fingerprintPath(candidate.Path)]
			select {
			case <-workCtx.Done():
				return workCtx.Err()
			case paths <- workItem{candidate: candidate, known: known}:
				return nil
			}
		})
		close(paths)
		workerWG.Wait()
		close(prepared)
		errCh <- err
	}()

	e.setProgressPhase(progressID, "indexing")
	for result := range prepared {
		summary.Scanned++
		if !result.ok {
			summary.Skipped++
			e.addProgressCounts(progressID, 1, 0, 1)
			continue
		}
		if err := session.Submit(result.entry); err != nil {
			cancel()
			_ = closeSession()
			// 排空剩余已准备结果，解除工作线程阻塞。
			for range prepared {
			}
			<-errCh
			e.finishProgress(progressID, summary, err)
			return summary, err
		}
		summary.Indexed++
		e.addProgressCounts(progressID, 1, 1, 0)
		if result.item.UpdatedAt > maxUpdated {
			maxUpdated = result.item.UpdatedAt
		}
		if adapter.ContentOnly && summary.Indexed%256 == 0 {
			select {
			case <-workCtx.Done():
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	if err := <-errCh; err != nil {
		_ = closeSession()
		e.finishProgress(progressID, summary, err)
		return summary, err
	}
	skippedUnchanged := int(fastSkipped.Load())
	summary.Scanned += skippedUnchanged
	summary.Skipped += skippedUnchanged
	// 批量会话 Close 会等待分片写入器收尾，并恢复批量模式期间调整过的 SQLite 设置。
	// 大索引上这仍可能在最后一个文件写完后花几秒；显式展示该阶段，
	// 避免 UI 看起来卡在 100%。
	e.setProgressPhase(progressID, "finalizing")
	if err := closeSession(); err != nil {
		e.finishProgress(progressID, summary, err)
		return summary, err
	}
	if maxUpdated > 0 {
		if err := e.store.SetSyncTime(ctx, pathKey, maxUpdated); err != nil {
			e.finishProgress(progressID, summary, err)
			return summary, err
		}
	}
	summary.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
	e.finishProgress(progressID, summary, nil)
	return summary, nil
}

func (e *Engine) prepareFileCandidate(
	ctx context.Context,
	adapter *adapters.FileAdapter,
	candidate adapters.FileCandidate,
) (storage.PreparedItem, model.DataItem, bool) {
	if candidate.IsDir {
		if adapter.ContentOnly {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		item := pathOnlyItem(candidate, true)
		chunks := pathOnlyChunks(item)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        item,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindPath),
		}, item, true
	}

	if !extract.SupportsIndexedPath(candidate.Path) {
		return storage.PreparedItem{}, model.DataItem{}, false
	}

	if adapter.ContentOnly && !e.shouldIndexFileContent(adapter, candidate) {
		return storage.PreparedItem{}, model.DataItem{}, false
	}

	if adapter.PathOnly {
		item := pathOnlyItem(candidate, false)
		chunks := pathOnlyChunks(item)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        item,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindPath),
		}, item, true
	}

	if extract.SupportsPlainText(candidate.Path) {
		item := streamingFileItem(candidate, adapter.MaxBytes)
		// 对小 / 中型纯文本文件，在工作线程中完成重活
		// （读文件、UTF-8 规范化、分块、哈希），让写入器临界区只执行 SQL。
		// 这会把索引流水线从近似单线程（所有工作都卡在写入互斥锁之后）
		// 变成 N 个工作线程真正并行工作。大文件则走流式路径，
		// 避免每个工作线程缓冲数百 MB。
		if candidate.Size <= 0 || candidate.Size > e.contentSizeLimit(adapter) {
			if adapter.ContentOnly {
				return storage.PreparedItem{}, model.DataItem{}, false
			}
			chunks := pathOnlyChunks(item)
			if len(chunks) == 0 {
				return storage.PreparedItem{}, model.DataItem{}, false
			}
			return storage.PreparedItem{
				Item:        item,
				Chunks:      chunks,
				Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindPath),
			}, item, true
		}
		if candidate.Size > 0 {
			if chunks, ok := e.eagerChunkPlainText(ctx, item); ok {
				if len(chunks) == 0 {
					chunks = pathOnlyChunks(item)
				}
				if len(chunks) == 0 {
					return storage.PreparedItem{}, model.DataItem{}, false
				}
				return storage.PreparedItem{
					Item:        item,
					Chunks:      chunks,
					Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindContent),
				}, item, true
			}
		}
		if adapter.ContentOnly {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		chunks := pathOnlyChunks(item)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        item,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindPath),
		}, item, true
	}

	if !adapter.Extractor.Supports(candidate.Path) {
		if adapter.ContentOnly {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		item := pathOnlyItem(candidate, false)
		chunks := pathOnlyChunks(item)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        item,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindPath),
		}, item, true
	}

	if candidate.Size <= 0 || candidate.Size > e.contentSizeLimit(adapter) {
		if adapter.ContentOnly {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		fallback := pathOnlyItem(candidate, false)
		chunks := pathOnlyChunks(fallback)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        fallback,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindPath),
		}, fallback, true
	}

	item, ok := adapter.ExtractFile(ctx, candidate.Path)
	if !ok {
		if adapter.ContentOnly {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		fallback := pathOnlyItem(candidate, false)
		chunks := pathOnlyChunks(fallback)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        fallback,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindPath),
		}, fallback, true
	}
	item.Metadata = ensureFileMetadata(item.Metadata, candidate, false)
	chunks := e.indexer.PrepareItem(&item)
	if chunks == nil {
		if adapter.ContentOnly {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		fallback := pathOnlyItem(candidate, false)
		pathChunks := pathOnlyChunks(fallback)
		if len(pathChunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        fallback,
			Chunks:      pathChunks,
			Fingerprint: fileFingerprint(candidate, pathChunks, fingerprintKindPath),
		}, fallback, true
	}
	return storage.PreparedItem{
		Item:        item,
		Chunks:      chunks,
		Fingerprint: fileFingerprint(candidate, chunks, fingerprintKindContent),
	}, item, true
}

// plainTextContentThreshold 限制前台完整内容索引大小。
// 更大的文本文件会先按文件名 / 路径索引，让全盘索引保持稳定文件/秒，
// 不被大型文本块的分词成本主导。
const plainTextContentThreshold int64 = 64 * 1024

// contentBackfillThreshold 更大，因为它在快速路径之后于后台运行。
// 超过该大小的文件仍保留路径索引，避免巨型日志或生成包垄断 SQLite FTS 写入。
const contentBackfillThreshold int64 = 1024 * 1024

func (e *Engine) contentSizeLimit(adapter *adapters.FileAdapter) int64 {
	if adapter.ContentOnly {
		return min(contentBackfillThreshold, adapter.MaxBytes)
	}
	return min(plainTextContentThreshold, adapter.MaxBytes)
}

func (e *Engine) shouldIndexFileContent(adapter *adapters.FileAdapter, candidate adapters.FileCandidate) bool {
	if candidate.IsDir || adapter.PathOnly {
		return false
	}
	if !extract.SupportsIndexedPath(candidate.Path) {
		return false
	}
	if candidate.Size <= 0 || candidate.Size > e.contentSizeLimit(adapter) {
		return false
	}
	if extract.SupportsPlainText(candidate.Path) {
		return true
	}
	return adapter.Extractor.Supports(candidate.Path)
}

func (e *Engine) fingerprintKindForCandidate(adapter *adapters.FileAdapter, candidate adapters.FileCandidate) string {
	if candidate.IsDir || adapter.PathOnly {
		if adapter.ContentOnly {
			return ""
		}
		return fingerprintKindPath
	}
	if e.shouldIndexFileContent(adapter, candidate) {
		return fingerprintKindContent
	}
	if adapter.ContentOnly {
		return ""
	}
	return fingerprintKindPath
}

// eagerChunkPlainText 在调用方工作线程中完整读取纯文本文件，并返回准备好的分块。
// 如果文件无法读取，则返回 (nil, false)。
func (e *Engine) eagerChunkPlainText(ctx context.Context, item model.DataItem) ([]model.Chunk, bool) {
	if item.ContentSource == nil {
		return nil, false
	}
	reader, err := item.ContentSource(ctx)
	if err != nil || reader == nil {
		return nil, false
	}
	defer reader.Close()
	chunks := make([]model.Chunk, 0, 4)
	err = e.indexer.StreamItemChunks(ctx, item, reader, func(c model.Chunk) error {
		// 在写入线程外预计算 CJK 二元词，让写入器临界区只执行 SQL。
		c.CJKGrams = storage.PrecomputeCJKGrams(c)
		chunks = append(chunks, c)
		return nil
	})
	if err != nil {
		return nil, false
	}
	return chunks, true
}

func (e *Engine) fileChunkSource(item model.DataItem) storage.ChunkSource {
	return func(ctx context.Context, yield func(model.Chunk) error) error {
		if item.ContentSource == nil {
			return storage.ErrSkipItem
		}
		reader, err := item.ContentSource(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return storage.ErrSkipItem
		}
		defer reader.Close()
		emitted := false
		wrappedYield := func(chunk model.Chunk) error {
			emitted = true
			return yield(chunk)
		}
		if err := e.indexer.StreamItemChunks(ctx, item, reader, wrappedYield); err != nil {
			return err
		}
		if emitted {
			return nil
		}
		for _, chunk := range pathOnlyChunks(item) {
			if err := yield(chunk); err != nil {
				return err
			}
		}
		return nil
	}
}

func streamingFileItem(candidate adapters.FileCandidate, maxBytes int64) model.DataItem {
	path := candidate.Path
	modified := time.Unix(0, candidate.ModTimeNS).Unix()
	metadata := ensureFileMetadata(map[string]any{}, candidate, false)
	return model.DataItem{
		ID:     adapters.StableFileID(path),
		Source: "file",
		Title:  filepath.Base(path),
		ContentSource: func(ctx context.Context) (io.ReadCloser, error) {
			reader, _, _, err := extract.OpenPlainText(ctx, path, maxBytes)
			return reader, err
		},
		Preview:   "",
		Metadata:  metadata,
		CreatedAt: modified,
		UpdatedAt: modified,
	}
}

func pathOnlyItem(candidate adapters.FileCandidate, isDir bool) model.DataItem {
	path := candidate.Path
	modified := time.Unix(0, candidate.ModTimeNS).Unix()
	entryType := "file"
	if isDir {
		entryType = "folder"
	}
	title := filepath.Base(path)
	if title == "." || title == string(filepath.Separator) {
		title = path
	}
	return model.DataItem{
		ID:        adapters.StableFileID(path),
		Source:    "file",
		Title:     title,
		Preview:   entryType + ": " + path,
		Metadata:  ensureFileMetadata(map[string]any{"entry_type": entryType}, candidate, isDir),
		CreatedAt: modified,
		UpdatedAt: modified,
	}
}

func pathOnlyChunks(item model.DataItem) []model.Chunk {
	pathValue, _ := item.Metadata["path"].(string)
	fileType, _ := item.Metadata["file_type"].(string)
	entryType, _ := item.Metadata["entry_type"].(string)
	if entryType == "" {
		entryType = "file"
	}
	searchText := strings.TrimSpace(strings.Join([]string{
		item.Title,
		pathValue,
		strings.ReplaceAll(pathValue, "\\", " "),
		strings.ReplaceAll(pathValue, "/", " "),
	}, " "))
	if searchText == "" {
		return nil
	}
	hash := hashChunkText(searchText)
	preview := item.Preview
	if strings.TrimSpace(preview) == "" {
		preview = entryType + ": " + pathValue
	}
	return []model.Chunk{{
		ChunkID:   fmt.Sprintf("%s:%s:%04d:%s", item.Source, item.ID, 0, hash[:16]),
		ItemID:    item.ID,
		Source:    item.Source,
		Title:     item.Title,
		Content:   searchText,
		Preview:   preview,
		Ordinal:   0,
		Hash:      hash,
		Path:      pathValue,
		FileType:  fileType,
		Metadata:  item.Metadata,
		CreatedAt: item.CreatedAt,
		UpdatedAt: item.UpdatedAt,
	}}
}

func ensureFileMetadata(metadata map[string]any, candidate adapters.FileCandidate, isDir bool) map[string]any {
	if metadata == nil {
		metadata = map[string]any{}
	}
	modified := time.Unix(0, candidate.ModTimeNS).Unix()
	metadata["path"] = candidate.Path
	metadata["size"] = candidate.Size
	metadata["modified"] = modified
	if isDir {
		metadata["file_type"] = "folder"
		metadata["entry_type"] = "folder"
		return metadata
	}
	fileType, _ := metadata["file_type"].(string)
	if strings.TrimSpace(fileType) == "" {
		fileType = strings.TrimPrefix(strings.ToLower(filepath.Ext(candidate.Path)), ".")
		if fileType == "" {
			fileType = "file"
		}
		metadata["file_type"] = fileType
	}
	metadata["entry_type"] = "file"
	return metadata
}

func hashChunkText(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func fileIndexMaxWorkers() int {
	n := runtime.NumCPU()
	if n <= 1 {
		return 1
	}
	workers := n
	if workers > 24 {
		workers = 24
	}
	return workers
}

func contentBackfillMaxWorkers() int {
	n := runtime.NumCPU() / 2
	if n < 2 {
		return 2
	}
	if n > 6 {
		return 6
	}
	return n
}

func (e *Engine) waitContentBackfillIfPaused(ctx context.Context, adapter *adapters.FileAdapter, progressID uint64) error {
	if !adapter.ContentOnly {
		return ctx.Err()
	}
	wasPaused := false
	for e.contentPaused.Load() {
		if !wasPaused {
			e.setProgressPhase(progressID, "paused")
			wasPaused = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	if wasPaused {
		e.setProgressPhase(progressID, "scanning")
	}
	return ctx.Err()
}

func fileFingerprint(candidate adapters.FileCandidate, chunks []model.Chunk, kind string) *storage.FileFingerprint {
	h := sha1.New()
	for _, chunk := range chunks {
		_, _ = h.Write([]byte(chunk.Hash))
	}
	return &storage.FileFingerprint{
		Path:        candidate.Path,
		Size:        candidate.Size,
		ModTimeNS:   candidate.ModTimeNS,
		ContentHash: kind + hex.EncodeToString(h.Sum(nil)),
	}
}

func fingerprintSatisfies(stored string, desired string) bool {
	switch desired {
	case fingerprintKindPath:
		return strings.HasPrefix(stored, fingerprintKindPath) || strings.HasPrefix(stored, fingerprintKindContent)
	case fingerprintKindContent:
		return strings.HasPrefix(stored, fingerprintKindContent)
	default:
		return false
	}
}

func fingerprintPath(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	return strings.ToLower(filepath.ToSlash(filepath.Clean(absolute)))
}

func progressKindForAdapter(adapter *adapters.FileAdapter) string {
	if adapter.ContentOnly {
		return "content"
	}
	return "fast"
}

func (e *Engine) startProgress(kind string, path string, workers int) uint64 {
	now := time.Now()
	progressID := e.progressRunSeq.Add(1)
	e.activeProgressID.Store(progressID)
	e.progressTotal.Store(0)
	e.progressScanned.Store(0)
	e.progressIndexed.Store(0)
	e.progressSkipped.Store(0)
	e.progressWritten.Store(0)
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	e.progress = model.IndexProgress{
		Active:    true,
		Kind:      kind,
		Phase:     "starting",
		Path:      path,
		Workers:   workers,
		StartedAt: now.UnixMilli(),
		UpdatedAt: now.UnixMilli(),
	}
	return progressID
}

func (e *Engine) setProgressPhase(progressID uint64, phase string) {
	if e.activeProgressID.Load() != progressID {
		return
	}
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if e.activeProgressID.Load() != progressID || !e.progress.Active {
		return
	}
	e.progress.Phase = phase
	e.progress.UpdatedAt = time.Now().UnixMilli()
}

func (e *Engine) addProgressCandidate(progressID uint64, path string) {
	if e.activeProgressID.Load() != progressID {
		return
	}
	e.progressTotal.Add(1)
	e.progressMu.Lock()
	if e.activeProgressID.Load() == progressID && e.progress.Active {
		e.progress.Current = path
	}
	e.progressMu.Unlock()
}

func (e *Engine) addProgressCounts(progressID uint64, scanned, indexed, skipped int) {
	if e.activeProgressID.Load() != progressID {
		return
	}
	e.progressScanned.Add(int64(scanned))
	e.progressIndexed.Add(int64(indexed))
	e.progressSkipped.Add(int64(skipped))
}

func (e *Engine) addProgressWritten(progressID uint64, written int) {
	if e.activeProgressID.Load() != progressID {
		return
	}
	e.progressWritten.Add(int64(written))
}

func (e *Engine) finishProgress(progressID uint64, summary model.SyncSummary, err error) {
	if e.activeProgressID.Load() != progressID {
		return
	}
	now := time.Now()
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if e.activeProgressID.Load() != progressID {
		return
	}
	e.activeProgressID.Store(0)
	e.progress.Active = false
	e.progress.Phase = "idle"
	e.progress.Scanned = summary.Scanned
	e.progress.Indexed = summary.Indexed
	e.progress.Skipped = summary.Skipped
	e.progress.ElapsedMS = summary.ElapsedMS
	e.progress.UpdatedAt = now.UnixMilli()
	e.progress.LastCompleted = now.UnixMilli()
	if err != nil {
		e.progress.LastError = err.Error()
	} else {
		e.progress.LastError = ""
	}
}

func (e *Engine) markProgressCanceled() {
	now := time.Now()
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if !e.progress.Active {
		return
	}
	e.progress.Active = false
	e.progress.Phase = "idle"
	e.progress.Current = ""
	e.progress.EtaMS = 0
	e.progress.UpdatedAt = now.UnixMilli()
	if e.progress.StartedAt > 0 {
		e.progress.ElapsedMS = float64(now.UnixMilli() - e.progress.StartedAt)
	}
	e.progress.LastError = context.Canceled.Error()
}

// periodicOptimize 每 30 分钟合并 FTS5 段并更新查询规划器统计信息，
// 防止搜索随着索引增长而变慢。
func (e *Engine) periodicOptimize() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			_ = e.store.Optimize(e.ctx)
		}
	}
}

func (e *Engine) setIndexRunCancel(cancel context.CancelFunc) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	e.indexRunCancel = cancel
}

func (e *Engine) setSyncRunCancel(cancel context.CancelFunc) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	e.syncRunCancel = cancel
}

// enableWatch 持久化路径，并注册到 fsnotify watcher。
func (e *Engine) enableWatch(ctx context.Context, root string) {
	_ = e.store.AddWatchedPath(ctx, root)
	e.addToWatcher(root)
}

// addToWatcher 递归地将文件或目录树添加到 fsnotify watcher。
func (e *Engine) addToWatcher(root string) {
	info, err := os.Stat(root)
	if err != nil {
		return
	}
	if !info.IsDir() {
		e.watchDir(filepath.Dir(root))
		return
	}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, walkerr error) error {
		if walkerr != nil || !d.IsDir() {
			return nil
		}
		if adapters.ShouldSkipDir(path, d) {
			return filepath.SkipDir
		}
		e.watchDir(path)
		return nil
	})
}

func (e *Engine) watchDir(dir string) {
	key := watchDirKey(dir)
	e.watchMu.Lock()
	defer e.watchMu.Unlock()
	if _, ok := e.watchedDirs[key]; ok {
		return
	}
	if err := e.watcher.Add(key); err == nil {
		e.watchedDirs[key] = struct{}{}
	}
}

func (e *Engine) unwatchSubtree(root string) {
	key := watchDirKey(root)
	prefix := key + string(filepath.Separator)
	var dirs []string

	e.watchMu.Lock()
	for dir := range e.watchedDirs {
		if dir == key || strings.HasPrefix(dir, prefix) {
			delete(e.watchedDirs, dir)
			dirs = append(dirs, dir)
		}
	}
	e.watchMu.Unlock()

	for _, dir := range dirs {
		_ = e.watcher.Remove(dir)
	}
}

func watchDirKey(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	return filepath.Clean(absolute)
}

// watcherLoop 处理 fsnotify 事件，并保持文件索引最新。
func (e *Engine) watcherLoop() {
	pending := make(map[string]fsnotify.Op)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case event, ok := <-e.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) ||
				event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				pending[event.Name] |= event.Op
			}
		case _, ok := <-e.watcher.Errors:
			if !ok {
				return
			}
		case <-ticker.C:
			if len(pending) == 0 {
				continue
			}
			toProcess := pending
			pending = make(map[string]fsnotify.Op)
			for path, op := range toProcess {
				e.applyWatchEvent(path, op)
			}
		}
	}
}

func (e *Engine) applyWatchEvent(path string, op fsnotify.Op) {
	if op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		e.removeIndexedPath(path)
		return
	}
	if op&(fsnotify.Create|fsnotify.Write) != 0 {
		e.reindexChangedPath(path)
	}
}

func (e *Engine) removeIndexedPath(path string) {
	e.indexMu.Lock()
	defer e.indexMu.Unlock()

	if err := e.store.DeleteFilePath(e.ctx, path); err != nil {
		log.Printf("remove indexed path %s: %v", path, err)
	}
	e.unwatchSubtree(path)
}

// reindexChangedPath 立即重新索引变化文件或新目录。
func (e *Engine) reindexChangedPath(path string) {
	e.indexMu.Lock()
	defer e.indexMu.Unlock()

	info, err := os.Stat(path)
	if err != nil {
		if err := e.store.DeleteFilePath(e.ctx, path); err != nil {
			log.Printf("remove missing path %s: %v", path, err)
		}
		return
	}
	if !info.IsDir() && info.Size() > e.maxBytes {
		return
	}
	if info.IsDir() {
		e.addToWatcher(path)
		adapter := adapters.NewFileAdapter([]string{path}, e.extractor, e.maxBytes)
		_, err := e.syncFileAdapter(e.ctx, adapter, 0, pathSyncKey(path))
		if err != nil {
			log.Printf("reindex directory %s: %v", path, err)
		}
		return
	}

	adapter := adapters.NewFileAdapter([]string{path}, e.extractor, e.maxBytes)
	candidate := adapters.FileCandidate{
		Path:      path,
		Size:      info.Size(),
		ModTimeNS: info.ModTime().UnixNano(),
		IsDir:     info.IsDir(),
	}
	entry, _, ok := e.prepareFileCandidate(e.ctx, adapter, candidate)
	if !ok {
		return
	}
	_ = e.store.UpsertItems(e.ctx, []storage.PreparedItem{entry})
}

// resyncWatched 在引擎重启后对所有监控根路径执行增量补齐。
func (e *Engine) resyncWatched(roots []string) {
	e.indexMu.Lock()
	defer e.indexMu.Unlock()

	for _, root := range roots {
		if e.ctx.Err() != nil {
			return
		}
		pathKey := pathSyncKey(root)
		lastSync, _ := e.store.GetSyncTime(e.ctx, pathKey)
		if lastSync > 0 {
			lastSync--
		}
		volumeRoot := isVolumeRoot(root)
		adapter := adapters.NewFileAdapter([]string{root}, e.extractor, e.maxBytes)
		adapter.PathOnly = volumeRoot
		_, err := e.syncFileAdapter(e.ctx, adapter, lastSync, pathKey)
		if err != nil {
			log.Printf("resync %s: %v", root, err)
		} else if volumeRoot {
			e.scheduleContentBackfill(root, e.maxBytes)
		}
		// 错开根路径处理，让后台补齐更温和。
		select {
		case <-e.ctx.Done():
			return
		case <-time.After(1500 * time.Millisecond):
		}
	}
}

// pathSyncKey 为 sync_state 派生稳定的逐路径适配器 key。
func pathSyncKey(path string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(filepath.Clean(path))))
	return "file:" + hex.EncodeToString(sum[:8])
}

func contentPathSyncKey(path string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(filepath.Clean(path))))
	return "file-content:" + hex.EncodeToString(sum[:8])
}

func isVolumeRoot(path string) bool {
	cleaned := filepath.Clean(path)
	volume := filepath.VolumeName(cleaned)
	if volume == "" {
		return false
	}
	rest := strings.TrimPrefix(cleaned, volume)
	return rest == "" || rest == string(filepath.Separator)
}

func defaultDBPath() string {
	if path := os.Getenv("RECALL_DB_PATH"); path != "" {
		return path
	}
	if local := os.Getenv("LOCALAPPDATA"); local != "" {
		return filepath.Join(local, "Recall", "recall.db")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "recall.db"
	}
	return filepath.Join(home, ".recall", "recall.db")
}

// makeSnippet 将结果预览居中到第一个匹配词附近。
func makeSnippet(preview string, query string) string {
	preview = strings.TrimSpace(preview)
	if preview == "" {
		return ""
	}
	lowerPreview := strings.ToLower(preview)
	for _, token := range strings.Fields(strings.ToLower(query)) {
		token = strings.Trim(token, `"*()`)
		if token == "" {
			continue
		}
		if idx := strings.Index(lowerPreview, token); idx >= 0 {
			return windowAround(preview, idx, 220)
		}
	}
	return windowAround(preview, 0, 220)
}

// windowAround 返回某个字节索引附近的有界 rune 窗口。
func windowAround(text string, byteIndex int, limit int) string {
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	runeIndex := len([]rune(text[:min(byteIndex, len(text))]))
	start := max(0, runeIndex-limit/4)
	end := min(len(runes), start+limit)
	prefix := ""
	suffix := ""
	if start > 0 {
		prefix = "..."
	}
	if end < len(runes) {
		suffix = "..."
	}
	return prefix + strings.TrimSpace(string(runes[start:end])) + suffix
}
