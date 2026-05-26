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

	"phantasm/core/internal/adapters"
	"phantasm/core/internal/extract"
	"phantasm/core/internal/indexer"
	"phantasm/core/internal/model"
	"phantasm/core/internal/storage"
)

// Config contains runtime paths and extraction settings.
type Config struct {
	DBPath   string
	TikaURL  string
	MaxBytes int64
}

// Engine coordinates adapters, indexing, and search.
type Engine struct {
	store          *storage.Store
	indexer        *indexer.Indexer
	extractor      extract.Extractor
	browsers       []model.DataAdapter
	startedAt      time.Time
	maxBytes       int64
	indexMu        sync.Mutex
	runMu          sync.Mutex
	indexRunCancel context.CancelFunc
	progressMu     sync.RWMutex
	progress       model.IndexProgress
	// file watching
	watcher *fsnotify.Watcher
	watchMu sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
}

// DefaultConfig resolves local-only defaults from environment variables.
func DefaultConfig() Config {
	return Config{
		DBPath:   defaultDBPath(),
		TikaURL:  os.Getenv("PHANTASM_TIKA_URL"),
		MaxBytes: extract.DefaultMaxBytes,
	}
}

// New constructs a local-first search engine.
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
		startedAt: time.Now(),
		maxBytes:  cfg.MaxBytes,
		watcher:   watcher,
		ctx:       engineCtx,
		cancel:    cancel,
	}

	// Restore previously watched paths and perform incremental catch-up.
	if watchedPaths, watchErr := store.GetWatchedPaths(ctx); watchErr == nil {
		for _, root := range watchedPaths {
			engine.addToWatcher(root)
		}
		if len(watchedPaths) > 0 {
			// Delay catch-up so startup stays responsive and search can use existing index immediately.
			go engine.scheduleWatchedResync(watchedPaths)
		}
	}
	go engine.watcherLoop()

	return engine, nil
}

// scheduleWatchedResync delays startup catch-up work to avoid competing with
// initial searches right after launch.
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

// Close releases engine resources.
func (e *Engine) Close() error {
	e.cancel()
	_ = e.watcher.Close()
	return e.store.Close()
}

// Health returns operational metadata for the renderer.
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

// Search executes a bounded FTS5 search.
func (e *Engine) Search(ctx context.Context, req model.SearchRequest) (model.SearchResponse, error) {
	start := time.Now()
	req.Query = strings.TrimSpace(req.Query)
	if req.Query == "" {
		return model.SearchResponse{Query: req.Query, Results: []model.SearchResult{}}, nil
	}

	ftsQuery := indexer.BuildFTSQuery(req.Query)
	results, hasMore, err := e.store.Search(ctx, req, ftsQuery)
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

// IndexPath indexes a user-selected file or directory (incremental on repeat calls).
func (e *Engine) IndexPath(ctx context.Context, req model.IndexPathRequest) (model.SyncSummary, error) {
	if strings.TrimSpace(req.Path) == "" {
		return model.SyncSummary{}, fmt.Errorf("path is required")
	}
	e.indexMu.Lock()
	defer e.indexMu.Unlock()

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
	summary, err := e.syncFileAdapter(runCtx, adapter, lastSync, pathKey)
	if err == nil {
		e.enableWatch(ctx, req.Path)
	}
	return summary, err
}

// CancelIndexPath cancels the active index_path request if one is running.
func (e *Engine) CancelIndexPath(ctx context.Context) (map[string]any, error) {
	_ = ctx
	e.runMu.Lock()
	cancel := e.indexRunCancel
	e.runMu.Unlock()
	if cancel == nil {
		return map[string]any{"ok": true, "canceled": false}, nil
	}
	cancel()
	return map[string]any{"ok": true, "canceled": true}, nil
}

// SyncBrowsers indexes local browser history from supported profiles.
func (e *Engine) SyncBrowsers(ctx context.Context) ([]model.SyncSummary, error) {
	e.indexMu.Lock()
	defer e.indexMu.Unlock()

	summaries := make([]model.SyncSummary, 0, len(e.browsers))
	for _, adapter := range e.browsers {
		if !adapter.IsAvailable() {
			continue
		}
		lastSync, err := e.store.GetSyncTime(ctx, adapter.ID())
		if err != nil {
			return summaries, err
		}
		summary, err := e.syncAdapter(ctx, adapter, lastSync)
		if err != nil {
			return summaries, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

// Optimize runs SQLite maintenance.
func (e *Engine) Optimize(ctx context.Context) (map[string]any, error) {
	if err := e.store.Optimize(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// IndexProgress returns the latest live indexing status snapshot.
func (e *Engine) IndexProgress(ctx context.Context) (model.IndexProgress, error) {
	e.progressMu.RLock()
	defer e.progressMu.RUnlock()
	return e.progress, nil
}

// syncAdapter pulls incremental data from one adapter and indexes it.
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

// syncFileAdapter streams file items directly into the indexer using batched
// transactions to avoid the overhead of one commit per file.
func (e *Engine) syncFileAdapter(
	ctx context.Context,
	adapter *adapters.FileAdapter,
	lastSync int64,
	pathKey string,
) (model.SyncSummary, error) {
	start := time.Now()
	summary := model.SyncSummary{AdapterID: adapter.ID()}
	var maxUpdated int64

	workers := fileIndexWorkers()
	e.startProgress(strings.Join(adapter.Roots, ", "), workers)
	fingerprints, err := e.store.LoadFileFingerprints(ctx, adapter.Roots)
	if err != nil {
		e.finishProgress(summary, err)
		return summary, err
	}

	// Larger batches reduce commit overhead for very large scans, but we keep
	// this below 500 to avoid long write transactions and UI jitter.
	const batchSize = 400
	batch := make([]storage.PreparedItem, 0, batchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := e.store.UpsertItems(ctx, batch); err != nil {
			return err
		}
		e.addProgressWritten(len(batch))
		batch = batch[:0]
		return nil
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type preparedResult struct {
		entry storage.PreparedItem
		item  model.DataItem
		ok    bool
	}

	// Wider buffers smooth producer/consumer bursts when scanning huge volumes.
	paths := make(chan adapters.FileCandidate, 1024)
	prepared := make(chan preparedResult, 1024)
	errCh := make(chan error, 1)
	var fastSkipped atomic.Int64

	var workerWG sync.WaitGroup
	for i := 0; i < workers; i++ {
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
			for candidate := range paths {
				if workCtx.Err() != nil {
					return
				}
				entry, item, ok := e.prepareFileCandidate(workCtx, adapter, candidate)
				if !ok {
					if !send(preparedResult{}) {
						return
					}
					continue
				}
				if !send(preparedResult{
					entry: entry,
					item:  item,
					ok:    true,
				}) {
					return
				}
			}
		}()
	}

	go func() {
		e.setProgressPhase("scanning")
		err := adapter.WalkIncrementalCandidates(workCtx, lastSync, func(candidate adapters.FileCandidate) (bool, bool) {
			fp, ok := fingerprints[fingerprintPath(candidate.Path)]
			if !ok {
				return false, false
			}
			if fp.Size == candidate.Size && fp.ModTimeNS == candidate.ModTimeNS {
				fastSkipped.Add(1)
				e.addProgressCandidate(candidate.Path)
				e.addProgressCounts(1, 0, 1)
				return true, true
			}
			return false, true
		}, func(candidate adapters.FileCandidate) error {
			e.addProgressCandidate(candidate.Path)
			select {
			case <-workCtx.Done():
				return workCtx.Err()
			case paths <- candidate:
				return nil
			}
		})
		close(paths)
		workerWG.Wait()
		close(prepared)
		errCh <- err
	}()

	e.setProgressPhase("extracting")
	for result := range prepared {
		summary.Scanned++
		if !result.ok {
			summary.Skipped++
			e.addProgressCounts(1, 0, 1)
			continue
		}
		batch = append(batch, result.entry)
		summary.Indexed++
		e.addProgressCounts(1, 1, 0)
		if result.item.UpdatedAt > maxUpdated {
			maxUpdated = result.item.UpdatedAt
		}
		if len(batch) >= batchSize {
			e.setProgressPhase("writing")
			if err := flushBatch(); err != nil {
				cancel()
				e.finishProgress(summary, err)
				return summary, err
			}
			e.setProgressPhase("extracting")
		}
	}
	if err := <-errCh; err != nil {
		e.finishProgress(summary, err)
		return summary, err
	}
	skippedUnchanged := int(fastSkipped.Load())
	summary.Scanned += skippedUnchanged
	summary.Skipped += skippedUnchanged
	e.setProgressPhase("writing")
	if err := flushBatch(); err != nil {
		e.finishProgress(summary, err)
		return summary, err
	}
	if maxUpdated > 0 {
		if err := e.store.SetSyncTime(ctx, pathKey, maxUpdated); err != nil {
			e.finishProgress(summary, err)
			return summary, err
		}
	}
	summary.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
	e.finishProgress(summary, nil)
	return summary, nil
}

func (e *Engine) prepareFileCandidate(
	ctx context.Context,
	adapter *adapters.FileAdapter,
	candidate adapters.FileCandidate,
) (storage.PreparedItem, model.DataItem, bool) {
	if candidate.IsDir {
		item := pathOnlyItem(candidate, true)
		chunks := pathOnlyChunks(item)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        item,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks),
		}, item, true
	}

	if !extract.SupportsIndexedPath(candidate.Path) {
		return storage.PreparedItem{}, model.DataItem{}, false
	}

	if extract.SupportsPlainText(candidate.Path) {
		item := streamingFileItem(candidate, adapter.MaxBytes)
		entry := storage.PreparedItem{
			Item:        item,
			ChunkSource: e.fileChunkSource(item),
			Fingerprint: fileFingerprintBase(candidate),
		}
		return entry, item, true
	}

	if !adapter.Extractor.Supports(candidate.Path) {
		item := pathOnlyItem(candidate, false)
		chunks := pathOnlyChunks(item)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        item,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks),
		}, item, true
	}

	item, ok := adapter.ExtractFile(ctx, candidate.Path)
	if !ok {
		fallback := pathOnlyItem(candidate, false)
		chunks := pathOnlyChunks(fallback)
		if len(chunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        fallback,
			Chunks:      chunks,
			Fingerprint: fileFingerprint(candidate, chunks),
		}, fallback, true
	}
	item.Metadata = ensureFileMetadata(item.Metadata, candidate, false)
	chunks := e.indexer.PrepareItem(&item)
	if chunks == nil {
		fallback := pathOnlyItem(candidate, false)
		pathChunks := pathOnlyChunks(fallback)
		if len(pathChunks) == 0 {
			return storage.PreparedItem{}, model.DataItem{}, false
		}
		return storage.PreparedItem{
			Item:        fallback,
			Chunks:      pathChunks,
			Fingerprint: fileFingerprint(candidate, pathChunks),
		}, fallback, true
	}
	return storage.PreparedItem{
		Item:        item,
		Chunks:      chunks,
		Fingerprint: fileFingerprint(candidate, chunks),
	}, item, true
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

func fileIndexWorkers() int {
	n := runtime.NumCPU()
	if n <= 2 {
		return 1
	}
	// Keep one core free for UI/OS and cap to avoid over-scheduling on
	// high-core machines. This tends to be a good throughput/stability balance.
	workers := n - 1
	if workers > 12 {
		workers = 12
	}
	return workers
}

func fileFingerprint(candidate adapters.FileCandidate, chunks []model.Chunk) *storage.FileFingerprint {
	h := sha1.New()
	for _, chunk := range chunks {
		_, _ = h.Write([]byte(chunk.Hash))
	}
	return &storage.FileFingerprint{
		Path:        candidate.Path,
		Size:        candidate.Size,
		ModTimeNS:   candidate.ModTimeNS,
		ContentHash: hex.EncodeToString(h.Sum(nil)),
	}
}

func fileFingerprintBase(candidate adapters.FileCandidate) *storage.FileFingerprint {
	return &storage.FileFingerprint{
		Path:      candidate.Path,
		Size:      candidate.Size,
		ModTimeNS: candidate.ModTimeNS,
	}
}

func fingerprintPath(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	return strings.ToLower(filepath.ToSlash(filepath.Clean(absolute)))
}

func (e *Engine) startProgress(path string, workers int) {
	now := time.Now()
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	e.progress = model.IndexProgress{
		Active:    true,
		Phase:     "starting",
		Path:      path,
		Workers:   workers,
		StartedAt: now.UnixMilli(),
		UpdatedAt: now.UnixMilli(),
	}
}

func (e *Engine) setProgressPhase(phase string) {
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if !e.progress.Active {
		return
	}
	e.progress.Phase = phase
	e.refreshProgressLocked(time.Now())
}

func (e *Engine) addProgressCandidate(path string) {
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if !e.progress.Active {
		return
	}
	e.progress.Current = path
	e.progress.Total++
	e.refreshProgressLocked(time.Now())
}

func (e *Engine) addProgressCounts(scanned, indexed, skipped int) {
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if !e.progress.Active {
		return
	}
	e.progress.Scanned += scanned
	e.progress.Indexed += indexed
	e.progress.Skipped += skipped
	e.refreshProgressLocked(time.Now())
}

func (e *Engine) addProgressWritten(written int) {
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if !e.progress.Active {
		return
	}
	e.progress.Written += written
	e.refreshProgressLocked(time.Now())
}

func (e *Engine) finishProgress(summary model.SyncSummary, err error) {
	now := time.Now()
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
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

func (e *Engine) refreshProgressLocked(now time.Time) {
	e.progress.UpdatedAt = now.UnixMilli()
	if e.progress.StartedAt > 0 {
		elapsed := float64(now.UnixMilli() - e.progress.StartedAt)
		e.progress.ElapsedMS = elapsed
		if elapsed > 0 {
			e.progress.FilesPerSec = float64(e.progress.Scanned) / (elapsed / 1000)
			remaining := e.progress.Total - e.progress.Scanned
			if remaining > 0 && e.progress.FilesPerSec > 0 {
				e.progress.EtaMS = float64(remaining) / e.progress.FilesPerSec * 1000
			} else {
				e.progress.EtaMS = 0
			}
		}
	}
}

func (e *Engine) setIndexRunCancel(cancel context.CancelFunc) {
	e.runMu.Lock()
	defer e.runMu.Unlock()
	e.indexRunCancel = cancel
}

// enableWatch persists the path and registers it with the fsnotify watcher.
func (e *Engine) enableWatch(ctx context.Context, root string) {
	_ = e.store.AddWatchedPath(ctx, root)
	e.addToWatcher(root)
}

// addToWatcher recursively adds a file or directory tree to the fsnotify watcher.
func (e *Engine) addToWatcher(root string) {
	info, err := os.Stat(root)
	if err != nil {
		return
	}
	if !info.IsDir() {
		_ = e.watcher.Add(filepath.Dir(root))
		return
	}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, walkerr error) error {
		if walkerr != nil || !d.IsDir() {
			return nil
		}
		if adapters.ShouldSkipDir(path, d) {
			return filepath.SkipDir
		}
		_ = e.watcher.Add(path)
		return nil
	})
}

// watcherLoop processes fsnotify events and re-indexes changed files.
func (e *Engine) watcherLoop() {
	pending := make(map[string]struct{})
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
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) {
				pending[event.Name] = struct{}{}
				// Watch newly created subdirectories.
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					_ = e.watcher.Add(event.Name)
				}
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
			pending = make(map[string]struct{})
			for path := range toProcess {
				e.reindexFile(path)
			}
		}
	}
}

// reindexFile immediately re-indexes a single changed file.
func (e *Engine) reindexFile(path string) {
	e.indexMu.Lock()
	defer e.indexMu.Unlock()

	info, err := os.Stat(path)
	if err != nil {
		return
	}
	if !info.IsDir() && info.Size() > e.maxBytes {
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
	if info.IsDir() {
		e.addToWatcher(path)
	}
}

// resyncWatched performs incremental catch-up for all watched roots after engine restart.
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
		adapter := adapters.NewFileAdapter([]string{root}, e.extractor, e.maxBytes)
		_, err := e.syncFileAdapter(e.ctx, adapter, lastSync, pathKey)
		if err != nil {
			log.Printf("resync %s: %v", root, err)
		}
		// Stagger roots so catch-up stays gentle in the background.
		select {
		case <-e.ctx.Done():
			return
		case <-time.After(1500 * time.Millisecond):
		}
	}
}

// pathSyncKey derives a stable per-path adapter key for sync_state.
func pathSyncKey(path string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(filepath.Clean(path))))
	return "file:" + hex.EncodeToString(sum[:8])
}
func defaultDBPath() string {
	if path := os.Getenv("PHANTASM_DB_PATH"); path != "" {
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

// makeSnippet centers a result preview around the first matched token.
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

// windowAround returns a bounded rune window around a byte index.
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
