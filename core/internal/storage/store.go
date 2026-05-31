package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	_ "modernc.org/sqlite"

	"recall/core/internal/model"
)

const cjkLayeredFTSTable = "chunk_cjk_layered_fts"
const noCJKGrams = "\x00"

// Config 保存 SQLite 存储配置。
type Config struct {
	Path string
}

// Store 封装 SQLite 元数据、分块和 FTS5 访问。
// db 是主库只读连接池，writeDB 是主库单写连接。
// 分块和 FTS 数据拆到多个相邻的 SQLite 分片文件中（见 shard.go），
// 让多个工作线程可以真正并行写入：单个 SQLite 文件一次只能有一个写入器，
// 但 N 个分片就能有 N 个并发写入器。
type Store struct {
	db      *sql.DB
	writeDB *sql.DB
	writeMu sync.Mutex
	shards  []*chunkShard
}

// ChunkSource 将准备好的分块流式送入存储层。它必须可重复执行，
// 这样 SQLite busy 导致事务重试时可以从头重放。
type ChunkSource func(context.Context, func(model.Chunk) error) error

// ErrSkipItem 告诉批量写入忽略易变文件，而不让整个索引流程失败。
var ErrSkipItem = errors.New("skip item")

// PreparedItem 是已经预处理、可批量持久化的条目。
type PreparedItem struct {
	Item        model.DataItem
	Chunks      []model.Chunk
	ChunkSource ChunkSource
	Fingerprint *FileFingerprint
	// IsNew 表示条目此前从未索引过。为 true 时，
	// 存储层会跳过既有分块查询，直接插入。
	IsNew bool
}

// FileFingerprint 保存廉价的文件变化签名，用于快速跳过未变化文件。
type FileFingerprint struct {
	Path        string
	Size        int64
	ModTimeNS   int64
	ContentHash string
}

// Open 创建 SQLite 数据库并应用生产环境 pragma。
// 主库使用独立读写连接池：读池服务并发查询，单写连接只维护主库元数据。
// 分块索引写入由各分片自己的写连接承担。
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, err
	}

	// 读池：连接数按 CPU 核心数配置，让搜索可以并行运行。
	readDB, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, err
	}
	readers := runtime.NumCPU()
	if readers < 2 {
		readers = 2
	}
	readDB.SetMaxOpenConns(readers)
	readDB.SetMaxIdleConns(readers)
	readDB.SetConnMaxLifetime(0)

	// 写池：单连接。SQLite 同一数据库一次只允许一个写入器，
	// 且 writeMu 已经串行化主库写入，所以单连接最合适。
	writeDB, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		_ = readDB.Close()
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0)

	store := &Store{db: readDB, writeDB: writeDB}
	if err := store.configure(ctx, readDB); err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, err
	}
	if err := store.configure(ctx, writeDB); err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, err
	}
	if err := store.migrate(ctx); err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, err
	}
	shards, err := openShards(ctx, cfg.Path)
	if err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, err
	}
	store.shards = shards
	return store, nil
}

// Close 释放主库连接池和所有分片连接池。
func (s *Store) Close() error {
	var firstErr error
	for _, sh := range s.shards {
		if err := sh.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := s.writeDB.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := s.db.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// SQLiteVersion 返回当前链接的 SQLite 引擎版本。
func (s *Store) SQLiteVersion(ctx context.Context) (string, error) {
	var version string
	err := s.db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version)
	return version, err
}

// Optimize 请求 SQLite 更新主库和所有分片的查询规划器与 FTS 统计信息。
// 分片会并行执行。
func (s *Store) Optimize(ctx context.Context) error {
	if err := runShardsParallel(s.shards, func(sh *chunkShard) error {
		return sh.optimize(ctx)
	}); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.writeDB.ExecContext(ctx, "PRAGMA optimize")
	return err
}

// UpsertItem 为一个数据条目增量应用变化分块。
func (s *Store) UpsertItem(ctx context.Context, item model.DataItem, chunks []model.Chunk) error {
	return s.UpsertItems(ctx, []PreparedItem{{Item: item, Chunks: chunks}})
}

// UpsertItems 通过扇出方式应用一批条目：每个分片在一个事务中写入
// 分配给自己的分块和指纹，并且分片之间完全并行。
// 这里没有主库写入步骤：items 表已移除，指纹也已经移到分片里，
// 并按与文件分块相同的 key 分片。
func (s *Store) UpsertItems(ctx context.Context, entries []PreparedItem) error {
	if len(entries) == 0 {
		return nil
	}

	groups := make([][]PreparedItem, len(s.shards))
	for _, entry := range entries {
		idx := pickShard(entry.Item.Source, entry.Item.ID)
		groups[idx] = append(groups[idx], entry)
	}

	return runShardsParallel(s.shards, func(sh *chunkShard) error {
		group := groups[sh.idx]
		if len(group) == 0 {
			return nil
		}
		return sh.writeEntries(ctx, group)
	})
}

// DeleteFilePath 删除某个文件或目录子树对应的索引分块和指纹。
// 文件系统 watcher 在受监控路径被删除或改名移走时会使用它。
func (s *Store) DeleteFilePath(ctx context.Context, path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	normalized := normalizeFingerprintPath(path)
	return runShardsParallel(s.shards, func(sh *chunkShard) error {
		return sh.deleteFilePath(ctx, normalized)
	})
}

// Search 执行 FTS5 查询并返回排序后的分块结果。
// 每个分片会并行查询，过采样候选集会先合并，再做全局重排。
func (s *Store) Search(ctx context.Context, req model.SearchRequest, ftsQuery string) ([]model.SearchResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	limit := normalizeSearchLimit(req.Limit)
	offset := normalizeSearchOffset(req.Offset)
	window := offset + limit
	if cjkQuery := buildCJKQuery(req.Query); cjkQuery != "" {
		results, err := s.searchAllShards(ctx, req, cjkQuery, cjkLayeredFTSTable, "bm25("+cjkLayeredFTSTable+")", window)
		if err == nil && len(results) > 0 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			ranked := rerankSearchResults(results, req.Query, 0)
			page, hasMore := paginateResults(ranked, offset, limit)
			return page, hasMore, nil
		}
	}

	results, err := s.searchAllShards(ctx, req, ftsQuery, "chunk_fts", "bm25(chunk_fts, 8.0, 1.0)", window)
	if err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	ranked := rerankSearchResults(results, req.Query, 0)
	page, hasMore := paginateResults(ranked, offset, limit)
	return page, hasMore, nil
}

// searchAllShards 将 FTS 查询扇出到每个分片，并合并过采样候选集。
func (s *Store) searchAllShards(
	ctx context.Context,
	req model.SearchRequest,
	matchQuery string,
	table string,
	scoreExpr string,
	window int,
) ([]model.SearchResult, error) {
	perShardLimit := searchCandidateLimit(window)
	resultsCh := make(chan []model.SearchResult, len(s.shards))
	errCh := make(chan error, len(s.shards))
	var wg sync.WaitGroup
	for _, sh := range s.shards {
		wg.Add(1)
		go func(sh *chunkShard) {
			defer wg.Done()
			res, err := sh.searchShard(ctx, req, matchQuery, table, scoreExpr, perShardLimit)
			if err != nil {
				errCh <- err
				return
			}
			resultsCh <- res
		}(sh)
	}
	wg.Wait()
	close(resultsCh)
	close(errCh)
	if err := <-errCh; err != nil {
		return nil, err
	}
	// 合并候选集。
	merged := make([]model.SearchResult, 0, perShardLimit*len(s.shards))
	for r := range resultsCh {
		merged = append(merged, r...)
	}
	return merged, nil
}

// GetWatchedPaths 返回所有应主动监控的路径。
func (s *Store) GetWatchedPaths(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT path FROM watched_paths ORDER BY added_at")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// AddWatchedPath 记录一个需要持久监控的路径。
func (s *Store) AddWatchedPath(ctx context.Context, path string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return retryBusy(ctx, func() error {
		_, err := s.writeDB.ExecContext(ctx,
			`INSERT INTO watched_paths(path, added_at) VALUES(?, ?) ON CONFLICT(path) DO NOTHING`,
			path, time.Now().Unix(),
		)
		return err
	})
}

// RemoveWatchedPath 从持久监控列表中移除路径。
func (s *Store) RemoveWatchedPath(ctx context.Context, path string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return retryBusy(ctx, func() error {
		_, err := s.writeDB.ExecContext(ctx, "DELETE FROM watched_paths WHERE path = ?", path)
		return err
	})
}

// GetSyncTime 返回适配器上次成功同步时间。
func (s *Store) GetSyncTime(ctx context.Context, adapterID string) (int64, error) {
	var lastSync sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		"SELECT last_sync_time FROM sync_state WHERE adapter_id = ?",
		adapterID,
	).Scan(&lastSync)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil || !lastSync.Valid {
		return 0, err
	}
	return lastSync.Int64, nil
}

// SetSyncTime 记录适配器上次成功同步时间。
func (s *Store) SetSyncTime(ctx context.Context, adapterID string, lastSync int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return retryBusy(ctx, func() error {
		_, err := s.writeDB.ExecContext(ctx, `
INSERT INTO sync_state(adapter_id, last_sync_time, updated_at)
VALUES(?, ?, ?)
ON CONFLICT(adapter_id) DO UPDATE SET
  last_sync_time = excluded.last_sync_time,
  updated_at = excluded.updated_at`,
			adapterID,
			lastSync,
			time.Now().Unix(),
		)
		return err
	})
}

// LoadFileFingerprints 返回指定根路径下已知的文件签名。
// 指纹现在存放在各分片中（与文件分块位于同一分片），
// 因此这里会把查询并行扇出到每个分片。
func (s *Store) LoadFileFingerprints(ctx context.Context, roots []string) (map[string]FileFingerprint, error) {
	if len(roots) == 0 {
		return map[string]FileFingerprint{}, nil
	}

	clauses := make([]string, 0, len(roots)*2)
	args := make([]any, 0, len(roots)*2)
	for _, root := range roots {
		normalizedRoot := normalizeFingerprintPath(root)
		likeRoot := strings.TrimRight(filepath.ToSlash(normalizedRoot), "/") + "/%"
		clauses = append(clauses, "path = ?", "path LIKE ?")
		args = append(args, normalizedRoot, likeRoot)
	}
	query := "SELECT path, size, mod_time_ns, content_hash FROM file_fingerprints WHERE " +
		strings.Join(clauses, " OR ")

	type shardResult struct {
		rows []FileFingerprint
		err  error
	}
	results := make([]shardResult, len(s.shards))
	var wg sync.WaitGroup
	for i, sh := range s.shards {
		wg.Add(1)
		go func(i int, sh *chunkShard) {
			defer wg.Done()
			rows, err := sh.db.QueryContext(ctx, query, args...)
			if err != nil {
				results[i].err = err
				return
			}
			defer rows.Close()
			for rows.Next() {
				if err := ctx.Err(); err != nil {
					results[i].err = err
					return
				}
				var fp FileFingerprint
				if err := rows.Scan(&fp.Path, &fp.Size, &fp.ModTimeNS, &fp.ContentHash); err != nil {
					results[i].err = err
					return
				}
				results[i].rows = append(results[i].rows, fp)
			}
			results[i].err = rows.Err()
		}(i, sh)
	}
	wg.Wait()

	fingerprints := make(map[string]FileFingerprint)
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for _, fp := range r.rows {
			fingerprints[fp.Path] = fp
		}
	}
	return fingerprints, nil
}

// configure 将 SQLite pragma 应用到给定连接池。
func (s *Store) configure(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA cache_size=-50000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=30000000000",
		"PRAGMA foreign_keys=ON",
		// 这里刻意不执行 PRAGMA optimize：大数据库上它可能阻塞数百毫秒，
		// 延迟引擎响应第一次搜索请求。periodicOptimize 会每 30 分钟在后台处理。
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("%s: %w", pragma, err)
		}
	}
	return nil
}

// WarmCache 执行轻量读取查询，把 FTS5 索引根节点拉进 SQLite 页缓存。
// Open 后在 goroutine 中调用一次即可，让第一次真实搜索命中热页而不是冷磁盘。
func (s *Store) WarmCache(ctx context.Context) {
	for _, sh := range s.shards {
		if ctx.Err() != nil {
			return
		}
		sh.warmCache(ctx)
	}
}

// migrate 创建主库仍负责的表：sync_state 和 watched_paths。
// 分块、FTS 索引和文件指纹位于分片数据库文件中（见 shard.go）。
// 旧版本遗留在主库中的 chunks/FTS/items/指纹表会被删除，
// 之后通过重新索引干净地填充分片。
func (s *Store) migrate(ctx context.Context) error {
	// 删除主库中的旧 chunks/FTS 表。分片前它们曾经放在这里；
	// 现在主库只保存 sync_state 和 watched_paths。
	// 重新索引会干净地填充分片。
	legacyDrops := []string{
		"DROP TABLE IF EXISTS chunk_fts",
		"DROP TABLE IF EXISTS " + cjkLayeredFTSTable,
		"DROP TABLE IF EXISTS chunks",
		// items 从未被任何查询读取，只会带来写放大。
		// file_fingerprints 已移入分片库，让每个文件的分块和指纹
		// 在同一个事务里提交，移除跨分片屏障和单主库写入器瓶颈。
		"DROP TABLE IF EXISTS items",
		"DROP TABLE IF EXISTS file_fingerprints",
	}
	for _, stmt := range legacyDrops {
		if _, err := s.writeDB.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	statements := []string{
		`CREATE TABLE IF NOT EXISTS sync_state (
			adapter_id TEXT PRIMARY KEY,
			last_sync_time INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS watched_paths (
			path TEXT PRIMARY KEY,
			added_at INTEGER NOT NULL
		)`,
	}

	for _, statement := range statements {
		if _, err := s.writeDB.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// applyChunkDiff 原子地执行删除、替换和插入操作。
func applyChunkDiff(
	ctx context.Context,
	tx *sql.Tx,
	item model.DataItem,
	oldChunks map[int]model.Chunk,
	newChunks []model.Chunk,
) error {
	seen := make(map[int]struct{}, len(newChunks))
	for _, chunk := range newChunks {
		seen[chunk.Ordinal] = struct{}{}
		if old, ok := oldChunks[chunk.Ordinal]; ok {
			if old.Hash == chunk.Hash && old.Title == chunk.Title {
				continue
			}
			if err := replaceChunk(ctx, tx, old, chunk); err != nil {
				return err
			}
			continue
		}
		if err := insertChunk(ctx, tx, item, chunk); err != nil {
			return err
		}
	}

	for ordinal, old := range oldChunks {
		if _, ok := seen[ordinal]; !ok {
			if err := deleteChunk(ctx, tx, old); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadChunks 加载既有分块，并以 ordinal 建立索引。
func loadChunks(ctx context.Context, tx *sql.Tx, source string, itemID string) (map[int]model.Chunk, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT rowid, chunk_id, item_id, source, title, content, preview, ordinal, hash,
       path, file_type, metadata_json, created_at, updated_at
FROM chunks WHERE source = ? AND item_id = ?`, source, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunks := make(map[int]model.Chunk)
	for rows.Next() {
		chunk, err := scanChunk(rows)
		if err != nil {
			return nil, err
		}
		chunks[chunk.Ordinal] = chunk
	}
	return chunks, rows.Err()
}

// insertChunk 存储新分块并写入 FTS5 索引。
func insertChunk(ctx context.Context, tx *sql.Tx, item model.DataItem, chunk model.Chunk) error {
	rowID, err := insertChunkRow(ctx, tx, item, chunk)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx,
		"INSERT INTO chunk_fts(rowid, title, content) VALUES(?, ?, ?)",
		rowID, chunk.Title, chunk.Content,
	); err != nil {
		return err
	}
	return insertCJK(ctx, tx, rowID, chunk)
}

// replaceChunk 在保留 rowid 的同时替换变化的分块。
func replaceChunk(ctx context.Context, tx *sql.Tx, old model.Chunk, next model.Chunk) error {
	if err := deleteFTS(ctx, tx, old); err != nil {
		return err
	}
	if err := updateChunkRow(ctx, tx, old.RowID, next); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx,
		"INSERT INTO chunk_fts(rowid, title, content) VALUES(?, ?, ?)",
		old.RowID, next.Title, next.Content,
	)
	if err != nil {
		return err
	}
	return insertCJK(ctx, tx, old.RowID, next)
}

// deleteChunk 从元数据和 FTS 索引中删除分块。
func deleteChunk(ctx context.Context, tx *sql.Tx, old model.Chunk) error {
	if err := deleteFTS(ctx, tx, old); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "DELETE FROM chunks WHERE rowid = ?", old.RowID)
	return err
}

// deleteFTS 发出 contentless FTS5 删除命令。
func deleteFTS(ctx context.Context, tx *sql.Tx, old model.Chunk) error {
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO chunk_fts(chunk_fts, rowid, title, content) VALUES('delete', ?, ?, ?)",
		old.RowID, old.Title, old.Content,
	); err != nil {
		return err
	}
	return deleteCJK(ctx, tx, old)
}

// insertCJK 存储生成的 CJK 二元词，用于子串搜索。
func insertCJK(ctx context.Context, tx *sql.Tx, rowID int64, chunk model.Chunk) error {
	grams := chunk.CJKGrams
	if grams == noCJKGrams {
		return nil
	}
	if grams == "" {
		grams = cjkGrams(cjkIndexText(chunk))
	}
	if grams == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		"INSERT INTO "+cjkLayeredFTSTable+"(rowid, grams) VALUES(?, ?)",
		rowID, grams,
	)
	return err
}

// deleteCJK 删除某个分块生成的 CJK 二元词。
func deleteCJK(ctx context.Context, tx *sql.Tx, chunk model.Chunk) error {
	grams := cjkGrams(cjkIndexText(chunk))
	if grams == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		"INSERT INTO "+cjkLayeredFTSTable+"("+cjkLayeredFTSTable+", rowid, grams) VALUES('delete', ?, ?)",
		chunk.RowID, grams,
	)
	return err
}

func cjkIndexText(chunk model.Chunk) string {
	const maxFullCJKRunes = 4096
	if utf8.RuneCountInString(chunk.Content) <= maxFullCJKRunes {
		return chunk.Title + " " + chunk.Content
	}
	return chunk.Title + " " + chunk.Preview
}

// PrecomputeCJKGrams 返回某个分块将要持久化的 CJK 二元词串。
// 调用方（例如在写入线程外准备条目的工作线程）可以把结果存进 Chunk.CJKGrams，
// 从而跳过写事务里的 CPU 计算。
func PrecomputeCJKGrams(chunk model.Chunk) string {
	if isASCII(chunk.Title) && isASCII(chunk.Content) && isASCII(chunk.Preview) {
		return noCJKGrams
	}
	return cjkGrams(cjkIndexText(chunk))
}

// insertChunkRow 插入分块元数据并返回 SQLite rowid。
func insertChunkRow(ctx context.Context, tx *sql.Tx, item model.DataItem, chunk model.Chunk) (int64, error) {
	meta, err := encodeMetadata(chunk.Metadata)
	if err != nil {
		return 0, err
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO chunks(chunk_id, item_id, source, title, content, preview, ordinal, hash,
                   path, file_type, metadata_json, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		chunk.ChunkID, item.ID, item.Source, chunk.Title, chunk.Content,
		chunk.Preview, chunk.Ordinal, chunk.Hash, chunk.Path, chunk.FileType,
		meta, chunk.CreatedAt, chunk.UpdatedAt,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// updateChunkRow 更新既有 rowid 对应的分块元数据。
func updateChunkRow(ctx context.Context, tx *sql.Tx, rowID int64, chunk model.Chunk) error {
	meta, err := encodeMetadata(chunk.Metadata)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
UPDATE chunks SET chunk_id = ?, title = ?, content = ?, preview = ?, hash = ?,
  path = ?, file_type = ?, metadata_json = ?, created_at = ?, updated_at = ?
WHERE rowid = ?`,
		chunk.ChunkID, chunk.Title, chunk.Content, chunk.Preview, chunk.Hash,
		chunk.Path, chunk.FileType, meta, chunk.CreatedAt, chunk.UpdatedAt, rowID,
	)
	return err
}

func normalizeFingerprintPath(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	return strings.ToLower(filepath.ToSlash(filepath.Clean(absolute)))
}

// buildSearchWhere 构造 SQL 谓词和绑定参数。
func buildSearchWhere(req model.SearchRequest, ftsQuery string, table string) ([]string, []any) {
	where := []string{table + " MATCH ?"}
	args := []any{ftsQuery}
	if req.Source != "" {
		where = append(where, "c.source = ?")
		args = append(args, req.Source)
	}
	if req.FileType != "" {
		where = append(where, "c.file_type = ?")
		args = append(args, req.FileType)
	}
	if req.Since > 0 {
		where = append(where, "c.updated_at >= ?")
		args = append(args, req.Since)
	}
	if req.Until > 0 {
		where = append(where, "c.updated_at <= ?")
		args = append(args, req.Until)
	}
	return where, args
}

// normalizeSearchLimit 用合理默认值保持分页高效。
func normalizeSearchLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func normalizeSearchOffset(offset int) int {
	if offset < 0 {
		return 0
	}
	return offset
}

// searchCandidateLimit 在面向用户的重排前对 FTS 命中进行过采样。
func searchCandidateLimit(window int) int {
	if window <= 0 {
		window = 50
	}
	candidateLimit := max(window*4, 80)
	return min(candidateLimit, 5000)
}

func paginateResults(results []model.SearchResult, offset int, limit int) ([]model.SearchResult, bool) {
	if offset >= len(results) {
		return []model.SearchResult{}, false
	}
	end := min(len(results), offset+limit)
	hasMore := end < len(results)
	return results[offset:end], hasMore
}

// rerankSearchResults 根据标题、路径、来源和新鲜度调整 FTS 顺序。
func rerankSearchResults(results []model.SearchResult, query string, limit int) []model.SearchResult {
	if len(results) == 0 {
		return results
	}

	profile := newSearchRankProfile(query)
	now := time.Now().Unix()
	for i := range results {
		results[i].Score += rankAdjustment(results[i], profile, now)
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			return results[i].UpdatedAt > results[j].UpdatedAt
		}
		return results[i].Score < results[j].Score
	})
	results = dedupeSearchResults(results)
	results = filterNoisySearchResults(results, profile)
	if limit > 0 && len(results) > limit {
		return results[:limit]
	}
	return results
}

type searchRankProfile struct {
	query        string
	compactQuery string
	tokens       []string
	fileLike     bool
	folderLike   bool
	// filenameLike 表示查询本身像文件名（例如 "rpc.php"）。
	// 该模式会强力提升精确文件名匹配，使其压过内容命中。
	filenameLike bool
	// urlLike 表示查询像 URL；该模式会优先浏览器结果。
	urlLike bool
	// bareNameQuery 表示查询是单个干净词，不含扩展名或路径分隔符
	// （例如 "texas"、"myProject"）。该模式会强烈优先精确文件夹 / 文件名匹配，
	// 使其排在纯内容命中之前。
	bareNameQuery bool
}

// newSearchRankProfile 为候选重排预先规范化一次查询文本。
func newSearchRankProfile(query string) searchRankProfile {
	normalized := normalizeRankText(query)
	compact := strings.ReplaceAll(normalized, " ", "")
	tokens := rankTokens(query)
	return searchRankProfile{
		query:         normalized,
		compactQuery:  compact,
		tokens:        tokens,
		fileLike:      looksFileLikeQuery(normalized, tokens),
		folderLike:    looksFolderLikeQuery(normalized, tokens),
		filenameLike:  looksFilenameLikeQuery(normalized),
		urlLike:       looksURLLikeQuery(query),
		bareNameQuery: looksBareNameQuery(normalized),
	}
}

// rankAdjustment 返回单条结果的分数调整值，数值越低越好。
func rankAdjustment(result model.SearchResult, profile searchRankProfile, now int64) float64 {
	var adjustment float64
	title := normalizeRankText(result.Title)
	path := normalizePathText(result.Path)
	name := normalizeRankText(filepath.Base(result.Path))
	if name == "." || name == string(filepath.Separator) {
		name = ""
	}
	if name == "" {
		name = title
	}
	stem := strings.TrimSuffix(name, normalizeRankText(filepath.Ext(name)))
	preview := normalizeRankText(result.Preview)
	// 这些值预先计算一次，避免在各个子函数里重复计算。
	nameWords := normalizeNameWords(name)
	pathBase := normalizeNameWords(normalizeRankText(filepath.Base(path)))

	adjustment += exactQueryAdjustment(profile, title, name, nameWords, stem, pathBase, path, preview)
	adjustment += tokenAdjustment(profile.tokens, title, name, nameWords, stem, pathBase, path, preview)
	adjustment += sourceAdjustment(result, profile, path)
	adjustment += folderBoostAdjustment(result, profile, name, nameWords, stem)
	adjustment += noiseAdjustment(result, path, name)
	adjustment += freshnessAdjustment(result.UpdatedAt, now)
	return adjustment
}

// dedupeSearchResults 为每个来源条目保留最佳分块。
func dedupeSearchResults(results []model.SearchResult) []model.SearchResult {
	seen := make(map[string]struct{}, len(results))
	deduped := results[:0]
	for _, result := range results {
		key := resultDedupeKey(result)
		if key != "" {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
		}
		deduped = append(deduped, result)
	}
	return deduped
}

// filterNoisySearchResults 隐藏生成型应用 / 缓存文件，除非查询明确需要。
func filterNoisySearchResults(results []model.SearchResult, profile searchRankProfile) []model.SearchResult {
	if allowsNoisySearchResults(profile) {
		return results
	}
	filtered := results[:0]
	for _, result := range results {
		if isNoisySearchPath(normalizePathText(result.Path)) {
			continue
		}
		filtered = append(filtered, result)
	}
	return filtered
}

// allowsNoisySearchResults 为明确的应用查询保留应用包命中。
func allowsNoisySearchResults(profile searchRankProfile) bool {
	for _, term := range noisyQueryTerms {
		if strings.Contains(profile.query, term) {
			return true
		}
	}
	for _, token := range profile.tokens {
		if _, ok := noisyQueryTermSet[token]; ok {
			return true
		}
	}
	return false
}

// resultDedupeKey 返回用于搜索结果分组的稳定身份。
func resultDedupeKey(result model.SearchResult) string {
	if result.Path != "" {
		return "path:" + normalizePathText(result.Path)
	}
	if isBrowserSource(result.Source) && result.Title != "" {
		return result.Source + ":title:" + normalizeRankText(result.Title)
	}
	if result.ItemID != "" {
		return result.Source + ":" + result.ItemID
	}
	if result.Title != "" {
		return result.Source + ":title:" + normalizeRankText(result.Title)
	}
	return ""
}

// isBrowserSource 判断来源是否来自浏览器历史数据。
func isBrowserSource(source string) bool {
	switch source {
	case "chrome", "edge", "firefox":
		return true
	default:
		return false
	}
}

// exactQueryAdjustment 奖励标题、文件名、路径和预览中的直接命中。
func exactQueryAdjustment(profile searchRankProfile, title, name, nameWords, stem, pathBase, path, preview string) float64 {
	if profile.query == "" {
		return 0
	}

	var adjustment float64
	var matchedOutsidePreview bool

	switch {
	case title == profile.query || name == profile.query || stem == profile.query || nameWords == profile.query || pathBase == profile.query:
		adjustment -= 36
		matchedOutsidePreview = true
	case strings.Contains(title, profile.query) || strings.Contains(name, profile.query) || strings.Contains(nameWords, profile.query):
		adjustment -= 20
		matchedOutsidePreview = true
	}

	if strings.HasPrefix(name, profile.query) || strings.HasPrefix(nameWords, profile.query) || strings.HasPrefix(pathBase, profile.query) {
		adjustment -= 10
		matchedOutsidePreview = true
	}
	if strings.HasPrefix(stem, profile.query) {
		adjustment -= 8
		matchedOutsidePreview = true
	}
	if strings.Contains(path, profile.query) {
		adjustment -= 8
		matchedOutsidePreview = true
	}
	if strings.Contains(preview, profile.query) {
		adjustment -= 2
	}

	if profile.compactQuery != "" && profile.compactQuery != profile.query {
		compactTitle := strings.ReplaceAll(title, " ", "")
		compactName := strings.ReplaceAll(nameWords, " ", "")
		if strings.Contains(compactTitle, profile.compactQuery) || strings.Contains(compactName, profile.compactQuery) {
			adjustment -= 14
			matchedOutsidePreview = true
		}
	}

	// 对文件名式查询（例如 "rpc.php"），精确文件名匹配必须压过内容再密集的文档。
	// BM25 会给术语密集文档很大的负分，所以这里额外强力加权，保证正确文件浮到前面。
	// 反过来，只在预览内容中命中的结果会被降权。
	if profile.filenameLike {
		switch {
		case name == profile.query || stem == profile.query:
			adjustment -= 80 // 明确文件名匹配，压过 BM25 优势
		case strings.HasPrefix(name, profile.query) || strings.HasPrefix(stem, profile.query):
			adjustment -= 30
		case !matchedOutsidePreview:
			adjustment += 18 // 文件名查询只命中预览内容，降权
		}
	}

	return adjustment
}

// tokenAdjustment 奖励高信号字段中的部分查询词命中。
func tokenAdjustment(tokens []string, title, name, nameWords, stem, pathBase, path, preview string) float64 {
	var adjustment float64
	for _, token := range tokens {
		if token == "" {
			continue
		}
		switch {
		case title == token || name == token || stem == token:
			adjustment -= 9
		case nameWords == token || pathBase == token:
			adjustment -= 8
		case strings.HasPrefix(name, token) || strings.HasPrefix(nameWords, token) || strings.HasPrefix(pathBase, token):
			adjustment -= 5.5
		case strings.Contains(title, token) || strings.Contains(name, token):
			adjustment -= 4
		case strings.Contains(nameWords, token) || strings.Contains(pathBase, token):
			adjustment -= 3.6
		}
		if strings.Contains(path, token) {
			adjustment -= 1.2
		}
		if strings.Contains(preview, token) {
			adjustment -= 0.3
		}
	}
	return adjustment
}

// folderBoostAdjustment 在裸名称查询精确命中文件夹名时强力提升文件夹结果。
// 对裸名称查询中的精确文件名匹配也会加权，使其压过深层内容命中。
func folderBoostAdjustment(result model.SearchResult, profile searchRankProfile, name, nameWords, stem string) float64 {
	if !profile.bareNameQuery || profile.filenameLike {
		return 0
	}
	isFolder := strings.TrimPrefix(strings.ToLower(result.FileType), ".") == "folder"
	q := profile.query
	switch {
	case isFolder && (name == q || nameWords == q):
		// 文件夹名精确匹配，始终优先浮到前面。
		return -60
	case isFolder && (strings.HasPrefix(name, q) || strings.HasPrefix(nameWords, q)):
		return -25
	case !isFolder && (name == q || stem == q || nameWords == q):
		// 文件名精确匹配，也应明显优先于内容命中。
		return -40
	case !isFolder && (strings.HasPrefix(name, q) || strings.HasPrefix(stem, q)):
		return -15
	}
	return 0
}

// sourceAdjustment 对文件式搜索略微提升本地文件，对 URL 式搜索提升浏览器结果。
func sourceAdjustment(result model.SearchResult, profile searchRankProfile, path string) float64 {
	// URL 式查询（例如 "https://example.com"）应优先浮出浏览器历史。
	if profile.urlLike && isBrowserSource(result.Source) {
		return -12.0
	}

	if result.Source != "file" {
		return 0
	}

	adjustment := -2.0
	if profile.fileLike || path != "" {
		adjustment -= 3
	}
	// 文件名式查询（例如 "rpc.php"）会额外提升文件来源，
	// 让文件结果能在同等字段上更好地与浏览器条目竞争。
	if profile.filenameLike {
		adjustment -= 4
	}
	fileType := strings.TrimPrefix(strings.ToLower(result.FileType), ".")
	if fileType == "folder" {
		if profile.folderLike {
			adjustment -= 6
		} else if profile.bareNameQuery {
			// bareNameQuery 的文件夹处理在 folderBoostAdjustment 中完成；
			// 这里不要再套用通用的非 folderLike 惩罚。
		} else {
			adjustment += 1.2
		}
	} else if profile.folderLike {
		adjustment += 2.2
	}
	if isPersonalFilePath(path) {
		adjustment -= 1.5
	}
	return adjustment
}

// noiseAdjustment 降低生成型应用包和浏览器缓存产物的排名。
func noiseAdjustment(result model.SearchResult, path string, name string) float64 {
	var adjustment float64
	if isNoisySearchPath(path) {
		adjustment += 42
	}
	if isGeneratedAsset(result.FileType, name) {
		adjustment += 8
		if isNoisySearchPath(path) {
			adjustment += 18
		}
	}
	return adjustment
}

// freshnessAdjustment 轻微偏向近期个人数据，但不隐藏旧命中。
func freshnessAdjustment(updatedAt int64, now int64) float64 {
	if updatedAt <= 0 || now <= 0 {
		return 0
	}
	age := now - updatedAt
	if age < 0 {
		return -0.5
	}
	switch {
	case age <= int64(7*24*time.Hour/time.Second):
		return -1.2
	case age <= int64(30*24*time.Hour/time.Second):
		return -0.8
	case age <= int64(180*24*time.Hour/time.Second):
		return -0.3
	case age >= int64(3*365*24*time.Hour/time.Second):
		return 0.4
	default:
		return 0
	}
}

// rankTokens 提取仅用于重排的可搜索查询词。
func rankTokens(input string) []string {
	seen := make(map[string]struct{})
	parts := strings.FieldsFunc(strings.ToLower(input), func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' || isCJK(r))
	})
	tokens := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		tokens = append(tokens, part)
	}
	return tokens
}

// looksBareNameQuery 判断查询是否是单个干净词，且不含文件扩展名或路径分隔符，
// 例如 "texas"、"myProject"、"recall"。这类查询很可能是在按名称找特定文件或文件夹。
func looksBareNameQuery(query string) bool {
	if query == "" {
		return false
	}
	// 必须是单个词元（无空白）。
	if strings.ContainsAny(query, " \t") {
		return false
	}
	// 不能像路径或 URL。
	if strings.ContainsAny(query, `/\.`) {
		return false
	}
	// 至少 2 个字符，避免单字母误判。
	return len([]rune(query)) >= 2
}

// looksFilenameLikeQuery 判断查询本身是否像文件名：
// 单个词元、无空格、以可识别扩展名结尾，例如 "rpc.php"。
func looksFilenameLikeQuery(query string) bool {
	if strings.ContainsAny(query, " \t") {
		return false
	}
	dotIdx := strings.LastIndex(query, ".")
	if dotIdx <= 0 || dotIdx == len(query)-1 {
		return false
	}
	ext := query[dotIdx+1:]
	if len(ext) < 2 || len(ext) > 6 {
		return false
	}
	for _, r := range ext {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			return false
		}
	}
	return true
}

// looksURLLikeQuery 判断查询是否像 URL。
func looksURLLikeQuery(query string) bool {
	lower := strings.ToLower(strings.TrimSpace(query))
	return strings.HasPrefix(lower, "http://") ||
		strings.HasPrefix(lower, "https://") ||
		strings.Contains(lower, "://")
}

// looksFileLikeQuery 判断查询是否像是在查找本地文件。
func looksFileLikeQuery(query string, tokens []string) bool {
	if strings.ContainsAny(query, `./\`) {
		return true
	}
	for _, token := range tokens {
		if strings.Contains(token, ".") {
			return true
		}
		if _, ok := fileTypeTerms[token]; ok {
			return true
		}
	}
	return false
}

func looksFolderLikeQuery(query string, tokens []string) bool {
	for _, token := range tokens {
		if _, ok := folderTerms[token]; ok {
			return true
		}
	}
	for _, term := range folderPhrases {
		if strings.Contains(query, term) {
			return true
		}
	}
	return false
}

func normalizeNameWords(name string) string {
	if name == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		".", " ",
		"-", " ",
		"_", " ",
	)
	return normalizeRankText(replacer.Replace(name))
}

// normalizeRankText 转为小写并压缩空白，用于排名比较。
func normalizeRankText(input string) string {
	return strings.Join(strings.Fields(strings.ToLower(input)), " ")
}

// normalizePathText 将路径转换为小写、斜杠分隔的形式。
func normalizePathText(path string) string {
	if path == "" {
		return ""
	}
	return strings.ToLower(filepath.ToSlash(filepath.Clean(path)))
}

// isPersonalFilePath 判断路径是否位于常见用户文档区域。
func isPersonalFilePath(path string) bool {
	return strings.Contains(path, "/desktop/") ||
		strings.Contains(path, "/documents/") ||
		strings.Contains(path, "/downloads/")
}

// isNoisySearchPath 检测生成型应用包和缓存密集位置。
func isNoisySearchPath(path string) bool {
	if path == "" {
		return false
	}
	for _, fragment := range noisySearchPathFragments {
		if strings.Contains(path, fragment) {
			return true
		}
	}
	return false
}

// isGeneratedAsset 判断结果是否像生成出来的 Web 资源。
func isGeneratedAsset(fileType string, name string) bool {
	switch strings.TrimPrefix(strings.ToLower(fileType), ".") {
	case "map", "min", "bundle":
		return true
	case "js", "mjs", "cjs", "css":
		return strings.Contains(name, ".min.") ||
			strings.Contains(name, ".bundle.") ||
			strings.Contains(name, ".chunk.") ||
			strings.Count(name, ".") >= 2
	default:
		return false
	}
}

var fileTypeTerms = map[string]struct{}{
	"csv":  {},
	"doc":  {},
	"docx": {},
	"json": {},
	"md":   {},
	"pdf":  {},
	"ppt":  {},
	"pptx": {},
	"txt":  {},
	"xls":  {},
	"xlsx": {},
}

var folderTerms = map[string]struct{}{
	"dir":       {},
	"directory": {},
	"folder":    {},
	"folders":   {},
}

var folderPhrases = []string{
	"open folder",
	"in folder",
	"directory",
	"folder",
}

var noisySearchPathFragments = []string{
	"/$recycle.bin/",
	"/appdata/local/google/chrome/user data/",
	"/appdata/local/microsoft/edge/user data/",
	"/cache/",
	"/code cache/",
	"/dist/",
	"/dist-electron/",
	"/gpucache/",
	"/jssdk/",
	"/node_modules/",
	"/software/dingding/",
	"/software/dingtalk/",
	"/software/feishu/",
	"/software/lark/",
	"/web_content/",
	"/webcontent/",
}

var noisyQueryTerms = []string{
	"dingding",
	"dingtalk",
	"feishu",
	"jssdk",
	"lark",
	"web_content",
	"webcontent",
}

var noisyQueryTermSet = map[string]struct{}{
	"dingding":    {},
	"dingtalk":    {},
	"feishu":      {},
	"jssdk":       {},
	"lark":        {},
	"web_content": {},
	"webcontent":  {},
}

// scanChunk 将一行 SQL 结果转换为 Chunk。
func scanChunk(rows interface {
	Scan(dest ...any) error
}) (model.Chunk, error) {
	var chunk model.Chunk
	var metadataJSON string
	err := rows.Scan(
		&chunk.RowID, &chunk.ChunkID, &chunk.ItemID, &chunk.Source, &chunk.Title,
		&chunk.Content, &chunk.Preview, &chunk.Ordinal, &chunk.Hash, &chunk.Path,
		&chunk.FileType, &metadataJSON, &chunk.CreatedAt, &chunk.UpdatedAt,
	)
	if err != nil {
		return chunk, err
	}
	chunk.Metadata = decodeMetadata(metadataJSON)
	return chunk, nil
}

// scanSearchCandidate 将一行搜索结果转换为 SearchResult。
// metadata_json 会就地解码，避免第二次查询。
func scanSearchCandidate(rows *sql.Rows) (model.SearchResult, error) {
	var result model.SearchResult
	var metadataJSON string
	err := rows.Scan(
		&result.RowID, &result.ItemID, &result.Source, &result.Title,
		&result.Preview, &result.Path, &result.FileType, &result.UpdatedAt,
		&metadataJSON, &result.Score,
	)
	if err != nil {
		return result, err
	}
	result.Metadata = decodeMetadata(metadataJSON)
	return result, nil
}

// encodeMetadata 将元数据 map 序列化为 SQLite 存储字符串。
func encodeMetadata(metadata map[string]any) (string, error) {
	if metadata == nil {
		return "{}", nil
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// decodeMetadata 反序列化元数据，失败时回退为空 map。
func decodeMetadata(value string) map[string]any {
	metadata := make(map[string]any)
	if value == "" {
		return metadata
	}
	if err := json.Unmarshal([]byte(value), &metadata); err != nil {
		return map[string]any{}
	}
	return metadata
}

// rollbackUnlessDone 回滚失败事务。
func rollbackUnlessDone(tx *sql.Tx, err *error) {
	if *err != nil {
		_ = tx.Rollback()
	}
}

func retryBusy(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		if err = fn(); err == nil || !isBusyError(err) {
			return err
		}
		delay := time.Duration(25*(attempt+1)*(attempt+1)) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}

func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sqlite_busy") ||
		strings.Contains(text, "database is locked") ||
		strings.Contains(text, "database table is locked") ||
		strings.Contains(text, "busy")
}

// buildCJKQuery 根据生成的 CJK 二元词构造 FTS 查询。
func buildCJKQuery(input string) string {
	terms := cjkGramList(input)
	if len(terms) == 0 {
		return ""
	}
	quoted := make([]string, 0, len(terms))
	for _, term := range terms {
		quoted = append(quoted, quoteMatch(term))
	}
	return strings.Join(quoted, " AND ")
}

// cjkGrams 返回以空格分隔的 CJK 二元词，用于 FTS 索引。
func cjkGrams(input string) string {
	return strings.Join(cjkGramList(input), " ")
}

// cjkGramList 从连续 CJK 片段生成去重的一元词和二元词。
func cjkGramList(input string) []string {
	if isASCII(input) {
		return nil
	}
	seen := make(map[string]struct{})
	terms := make([]string, 0, utf8.RuneCountInString(input)/2)
	emit := func(term string) {
		if term == "" {
			return
		}
		if _, ok := seen[term]; ok {
			return
		}
		seen[term] = struct{}{}
		terms = append(terms, term)
	}

	var run []rune
	flush := func() {
		if len(run) == 1 {
			emit(string(run))
		}
		for i := 0; i+1 < len(run); i++ {
			emit(string(run[i : i+2]))
		}
		run = run[:0]
	}

	for _, r := range input {
		if isCJK(r) {
			run = append(run, r)
			continue
		}
		flush()
	}
	flush()
	return terms
}

// isCJK 判断 rune 是否处于常见 CJK 范围。
func isCJK(r rune) bool {
	return (r >= 0x3400 && r <= 0x4DBF) ||
		(r >= 0x4E00 && r <= 0x9FFF) ||
		(r >= 0xF900 && r <= 0xFAFF)
}

func isASCII(input string) bool {
	for i := 0; i < len(input); i++ {
		if input[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// quoteMatch 为 FTS MATCH 语法转义生成词元。
func quoteMatch(token string) string {
	return `"` + strings.ReplaceAll(token, `"`, `""`) + `"`
}
