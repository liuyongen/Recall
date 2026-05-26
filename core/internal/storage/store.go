package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	_ "modernc.org/sqlite"

	"phantasm/core/internal/model"
)

const cjkLayeredFTSTable = "chunk_cjk_layered_fts"

// Config holds SQLite store settings.
type Config struct {
	Path string
}

// Store wraps SQLite access for metadata, chunks, and FTS5.
type Store struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// ChunkSource streams prepared chunks to the store. It must be repeatable so a
// busy SQLite transaction can be retried from the beginning.
type ChunkSource func(context.Context, func(model.Chunk) error) error

// ErrSkipItem tells a batch write to ignore a volatile file without failing the
// whole indexing run.
var ErrSkipItem = errors.New("skip item")

// PreparedItem is a preprocessed item ready to be persisted in a batch.
type PreparedItem struct {
	Item        model.DataItem
	Chunks      []model.Chunk
	ChunkSource ChunkSource
	Fingerprint *FileFingerprint
}

// FileFingerprint stores a cheap file-change signature for fast skip checks.
type FileFingerprint struct {
	Path        string
	Size        int64
	ModTimeNS   int64
	ContentHash string
}

// Open creates the SQLite database and applies production pragmas.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, err
	}

	// WAL mode allows concurrent reads with one writer.
	// Use a small pool for read concurrency while writes are serialized by writeMu.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)

	store := &Store{db: db}
	if err := store.configure(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// Close releases the SQLite connection pool.
func (s *Store) Close() error {
	return s.db.Close()
}

// SQLiteVersion returns the linked SQLite engine version.
func (s *Store) SQLiteVersion(ctx context.Context) (string, error) {
	var version string
	err := s.db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version)
	return version, err
}

// Optimize asks SQLite to update query planner and FTS statistics.
func (s *Store) Optimize(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "PRAGMA optimize")
	return err
}

// UpsertItem incrementally applies changed chunks for a data item.
func (s *Store) UpsertItem(ctx context.Context, item model.DataItem, chunks []model.Chunk) error {
	return s.UpsertItems(ctx, []PreparedItem{{Item: item, Chunks: chunks}})
}

// UpsertItems incrementally applies a batch of changed items in one write transaction.
func (s *Store) UpsertItems(ctx context.Context, entries []PreparedItem) error {
	if len(entries) == 0 {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return retryBusy(ctx, func() (err error) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer rollbackUnlessDone(tx, &err)

		for _, entry := range entries {
			if entry.ChunkSource != nil {
				if err = upsertItemFromChunkSource(ctx, tx, entry); err != nil {
					return err
				}
				continue
			}
			if err = upsertPreparedItem(ctx, tx, entry); err != nil {
				return err
			}
		}

		err = tx.Commit()
		return err
	})
}

func upsertPreparedItem(ctx context.Context, tx *sql.Tx, entry PreparedItem) error {
	if err := upsertItem(ctx, tx, entry.Item); err != nil {
		return err
	}
	oldChunks, err := loadChunks(ctx, tx, entry.Item.Source, entry.Item.ID)
	if err != nil {
		return err
	}
	if err = applyChunkDiff(ctx, tx, entry.Item, oldChunks, entry.Chunks); err != nil {
		return err
	}
	if entry.Fingerprint != nil {
		if err = upsertFileFingerprint(ctx, tx, *entry.Fingerprint); err != nil {
			return err
		}
	}
	return nil
}

// Search executes an FTS5 query and returns ranked chunk results.
func (s *Store) Search(ctx context.Context, req model.SearchRequest, ftsQuery string) ([]model.SearchResult, bool, error) {
	limit := normalizeSearchLimit(req.Limit)
	offset := normalizeSearchOffset(req.Offset)
	window := offset + limit
	if cjkQuery := buildCJKQuery(req.Query); cjkQuery != "" {
		results, err := s.searchTable(ctx, req, cjkQuery, cjkLayeredFTSTable, "bm25("+cjkLayeredFTSTable+")", window)
		if err == nil && len(results) > 0 {
			ranked := rerankSearchResults(results, req.Query, 0)
			page, hasMore := paginateResults(ranked, offset, limit)
			return page, hasMore, nil
		}
	}

	results, err := s.searchTable(ctx, req, ftsQuery, "chunk_fts", "bm25(chunk_fts, 8.0, 1.0)", window)
	if err != nil {
		return nil, false, err
	}
	ranked := rerankSearchResults(results, req.Query, 0)
	page, hasMore := paginateResults(ranked, offset, limit)
	return page, hasMore, nil
}

// searchTable fetches an over-sampled candidate set from one FTS5 table.
func (s *Store) searchTable(
	ctx context.Context,
	req model.SearchRequest,
	matchQuery string,
	table string,
	scoreExpr string,
	window int,
) ([]model.SearchResult, error) {
	where, args := buildSearchWhere(req, matchQuery, table)
	sqlText := fmt.Sprintf(`
SELECT c.rowid, c.item_id, c.source, c.title, c.content, c.preview, c.path,
       c.file_type, c.updated_at, %s AS score, c.metadata_json
FROM %s
JOIN chunks c ON c.rowid = %s.rowid
WHERE %s
ORDER BY score ASC, c.updated_at DESC`, scoreExpr, table, table, strings.Join(where, " AND "))
	candidateCap := searchCandidateLimit(window)
	sqlText += "\nLIMIT ?"
	args = append(args, candidateCap)

	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]model.SearchResult, 0, candidateCap)
	for rows.Next() {
		result, err := scanSearchResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

// GetWatchedPaths returns all paths that should be actively watched.
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

// AddWatchedPath records a path for persistent watching.
func (s *Store) AddWatchedPath(ctx context.Context, path string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return retryBusy(ctx, func() error {
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO watched_paths(path, added_at) VALUES(?, ?) ON CONFLICT(path) DO NOTHING`,
			path, time.Now().Unix(),
		)
		return err
	})
}

// RemoveWatchedPath removes a path from persistent watching.
func (s *Store) RemoveWatchedPath(ctx context.Context, path string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return retryBusy(ctx, func() error {
		_, err := s.db.ExecContext(ctx, "DELETE FROM watched_paths WHERE path = ?", path)
		return err
	})
}

// GetSyncTime returns the last successful sync time for an adapter.
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

// SetSyncTime records the last successful sync time for an adapter.
func (s *Store) SetSyncTime(ctx context.Context, adapterID string, lastSync int64) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return retryBusy(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
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

// LoadFileFingerprints returns known file signatures under the provided roots.
func (s *Store) LoadFileFingerprints(ctx context.Context, roots []string) (map[string]FileFingerprint, error) {
	fingerprints := make(map[string]FileFingerprint)
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		normalizedRoot := normalizeFingerprintPath(root)
		likeRoot := strings.TrimRight(filepath.ToSlash(normalizedRoot), "/") + "/%"
		rows, err := s.db.QueryContext(ctx, `
SELECT path, size, mod_time_ns, content_hash
FROM file_fingerprints
WHERE path = ? OR path LIKE ?`, normalizedRoot, likeRoot)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var fp FileFingerprint
			if err := rows.Scan(&fp.Path, &fp.Size, &fp.ModTimeNS, &fp.ContentHash); err != nil {
				rows.Close()
				return nil, err
			}
			fingerprints[fp.Path] = fp
			if err := ctx.Err(); err != nil {
				rows.Close()
				return nil, err
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return fingerprints, nil
}

// configure applies SQLite pragmas required by the search engine.
func (s *Store) configure(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA cache_size=-50000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=30000000000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA optimize",
	}
	for _, pragma := range pragmas {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("%s: %w", pragma, err)
		}
	}
	return nil
}

// migrate creates metadata, chunk, FTS, and sync-state tables.
func (s *Store) migrate(ctx context.Context) error {
	hadCJKTable, err := s.hasTable(ctx, cjkLayeredFTSTable)
	if err != nil {
		return err
	}

	statements := []string{
		`CREATE TABLE IF NOT EXISTS items (
			source TEXT NOT NULL,
			id TEXT NOT NULL,
			title TEXT NOT NULL,
			preview TEXT NOT NULL,
			path TEXT NOT NULL DEFAULT '',
			file_type TEXT NOT NULL DEFAULT '',
			metadata_json TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY(source, id)
		)`,
		`CREATE TABLE IF NOT EXISTS chunks (
			rowid INTEGER PRIMARY KEY AUTOINCREMENT,
			chunk_id TEXT NOT NULL UNIQUE,
			item_id TEXT NOT NULL,
			source TEXT NOT NULL,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			preview TEXT NOT NULL,
			ordinal INTEGER NOT NULL,
			hash TEXT NOT NULL,
			path TEXT NOT NULL DEFAULT '',
			file_type TEXT NOT NULL DEFAULT '',
			metadata_json TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			UNIQUE(source, item_id, ordinal)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_chunks_source_time ON chunks(source, updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chunks_file_type ON chunks(file_type)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
			title,
			content,
			content='',
			tokenize='unicode61 remove_diacritics 2'
		)`,
		`CREATE TABLE IF NOT EXISTS sync_state (
			adapter_id TEXT PRIMARY KEY,
			last_sync_time INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS watched_paths (
			path TEXT PRIMARY KEY,
			added_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS file_fingerprints (
			path TEXT PRIMARY KEY,
			size INTEGER NOT NULL,
			mod_time_ns INTEGER NOT NULL,
			content_hash TEXT NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS ` + cjkLayeredFTSTable + ` USING fts5(
			grams,
			content='',
			tokenize='unicode61 remove_diacritics 2'
		)`,
	}

	for _, statement := range statements {
		if _, err = s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}

	if !hadCJKTable {
		return s.rebuildLayeredCJKIndex(ctx)
	}
	return nil
}

func (s *Store) hasTable(ctx context.Context, table string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `
SELECT 1
FROM sqlite_master
WHERE type IN ('table','view') AND name = ?
LIMIT 1`, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return exists == 1, nil
}

// rebuildLayeredCJKIndex rebuilds the only supported CJK index shape.
func (s *Store) rebuildLayeredCJKIndex(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
SELECT rowid, chunk_id, item_id, source, title, content, preview, ordinal, hash,
       path, file_type, metadata_json, created_at, updated_at
FROM chunks`)
	if err != nil {
		return err
	}

	var pending []model.Chunk
	for rows.Next() {
		chunk, scanErr := scanChunk(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		pending = append(pending, chunk)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	if _, err := s.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+cjkLayeredFTSTable); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `CREATE VIRTUAL TABLE `+cjkLayeredFTSTable+` USING fts5(
		grams,
		content='',
		tokenize='unicode61 remove_diacritics 2'
	)`); err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollbackUnlessDone(tx, &err)

	for _, chunk := range pending {
		if err = insertCJK(ctx, tx, chunk.RowID, chunk); err != nil {
			return err
		}
	}

	err = tx.Commit()
	return err
}

// upsertItem writes item-level metadata without duplicating full content.
func upsertItem(ctx context.Context, tx *sql.Tx, item model.DataItem) error {
	meta, err := encodeMetadata(item.Metadata)
	if err != nil {
		return err
	}

	pathValue, _ := item.Metadata["path"].(string)
	fileType, _ := item.Metadata["file_type"].(string)
	_, err = tx.ExecContext(ctx, `
INSERT INTO items(source, id, title, preview, path, file_type, metadata_json, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source, id) DO UPDATE SET
  title = excluded.title,
  preview = excluded.preview,
  path = excluded.path,
  file_type = excluded.file_type,
  metadata_json = excluded.metadata_json,
  updated_at = excluded.updated_at`,
		item.Source, item.ID, item.Title, item.Preview, pathValue, fileType,
		meta, item.CreatedAt, item.UpdatedAt,
	)
	return err
}

// applyChunkDiff performs delete, replace, and insert operations atomically.
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

func upsertItemFromChunkSource(ctx context.Context, tx *sql.Tx, entry PreparedItem) error {
	oldChunks, err := loadChunks(ctx, tx, entry.Item.Source, entry.Item.ID)
	if err != nil {
		return err
	}

	item := entry.Item
	seen := make(map[int]struct{}, len(oldChunks))
	hasher := sha1.New()
	wroteItem := false
	chunkCount := 0

	err = entry.ChunkSource(ctx, func(chunk model.Chunk) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(chunk.Content) == "" {
			return nil
		}
		if !wroteItem {
			if strings.TrimSpace(item.Preview) == "" {
				item.Preview = chunk.Preview
			}
			if err := upsertItem(ctx, tx, item); err != nil {
				return err
			}
			wroteItem = true
		}
		seen[chunk.Ordinal] = struct{}{}
		_, _ = hasher.Write([]byte(chunk.Hash))
		if old, ok := oldChunks[chunk.Ordinal]; ok {
			if old.Hash == chunk.Hash && old.Title == chunk.Title {
				chunkCount++
				return nil
			}
			if err := replaceChunk(ctx, tx, old, chunk); err != nil {
				return err
			}
			chunkCount++
			return nil
		}
		if err := insertChunk(ctx, tx, item, chunk); err != nil {
			return err
		}
		chunkCount++
		return nil
	})
	if errors.Is(err, ErrSkipItem) {
		if chunkCount > 0 || wroteItem {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}

	if !wroteItem {
		item.Preview = strings.TrimSpace(item.Preview)
		if err := upsertItem(ctx, tx, item); err != nil {
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
	if entry.Fingerprint != nil {
		fp := *entry.Fingerprint
		fp.ContentHash = hex.EncodeToString(hasher.Sum(nil))
		if err := upsertFileFingerprint(ctx, tx, fp); err != nil {
			return err
		}
	}
	return nil
}

// loadChunks loads existing chunks keyed by ordinal.
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

// insertChunk stores a new chunk and indexes it in FTS5.
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

// replaceChunk swaps a changed chunk while preserving its rowid.
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

// deleteChunk removes a chunk from metadata and the FTS index.
func deleteChunk(ctx context.Context, tx *sql.Tx, old model.Chunk) error {
	if err := deleteFTS(ctx, tx, old); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "DELETE FROM chunks WHERE rowid = ?", old.RowID)
	return err
}

// deleteFTS emits the contentless FTS5 delete command.
func deleteFTS(ctx context.Context, tx *sql.Tx, old model.Chunk) error {
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO chunk_fts(chunk_fts, rowid, title, content) VALUES('delete', ?, ?, ?)",
		old.RowID, old.Title, old.Content,
	); err != nil {
		return err
	}
	return deleteCJK(ctx, tx, old)
}

// insertCJK stores generated CJK bigrams for substring search.
func insertCJK(ctx context.Context, tx *sql.Tx, rowID int64, chunk model.Chunk) error {
	grams := cjkGrams(cjkIndexText(chunk))
	if grams == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		"INSERT INTO "+cjkLayeredFTSTable+"(rowid, grams) VALUES(?, ?)",
		rowID, grams,
	)
	return err
}

// deleteCJK removes generated CJK bigrams for a chunk.
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

// insertChunkRow inserts chunk metadata and returns the SQLite rowid.
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

// updateChunkRow updates chunk metadata for an existing rowid.
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

func upsertFileFingerprint(ctx context.Context, tx *sql.Tx, fp FileFingerprint) error {
	if fp.Path == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO file_fingerprints(path, size, mod_time_ns, content_hash, updated_at)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(path) DO UPDATE SET
  size = excluded.size,
  mod_time_ns = excluded.mod_time_ns,
  content_hash = excluded.content_hash,
  updated_at = excluded.updated_at`,
		normalizeFingerprintPath(fp.Path), fp.Size, fp.ModTimeNS, fp.ContentHash, time.Now().Unix(),
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

// buildSearchWhere builds SQL predicates and bound arguments.
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

// normalizeSearchLimit keeps paging efficient with a sane default.
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

// searchCandidateLimit over-samples FTS hits before human-friendly reranking.
func searchCandidateLimit(window int) int {
	if window <= 0 {
		window = 50
	}
	candidateLimit := max(window*8, 160)
	return min(candidateLimit, 20000)
}

func paginateResults(results []model.SearchResult, offset int, limit int) ([]model.SearchResult, bool) {
	if offset >= len(results) {
		return []model.SearchResult{}, false
	}
	end := min(len(results), offset+limit)
	hasMore := end < len(results)
	return results[offset:end], hasMore
}

// rerankSearchResults adjusts FTS order using title, path, source, and freshness.
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
}

// newSearchRankProfile normalizes query text once for reranking candidates.
func newSearchRankProfile(query string) searchRankProfile {
	normalized := normalizeRankText(query)
	compact := strings.ReplaceAll(normalized, " ", "")
	tokens := rankTokens(query)
	return searchRankProfile{
		query:        normalized,
		compactQuery: compact,
		tokens:       tokens,
		fileLike:     looksFileLikeQuery(normalized, tokens),
		folderLike:   looksFolderLikeQuery(normalized, tokens),
	}
}

// rankAdjustment returns a lower-is-better score adjustment for one result.
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

	adjustment += exactQueryAdjustment(profile, title, name, stem, path, preview)
	adjustment += tokenAdjustment(profile.tokens, title, name, stem, path, preview)
	adjustment += sourceAdjustment(result, profile, path)
	adjustment += noiseAdjustment(result, path, name)
	adjustment += freshnessAdjustment(result.UpdatedAt, now)
	return adjustment
}

// dedupeSearchResults keeps the best chunk for each source item.
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

// filterNoisySearchResults hides generated app/cache files unless requested.
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

// allowsNoisySearchResults keeps app bundle hits for explicit app queries.
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

// resultDedupeKey returns a stable identity for grouped search results.
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

// isBrowserSource reports whether a source comes from browser history data.
func isBrowserSource(source string) bool {
	switch source {
	case "chrome", "edge", "firefox":
		return true
	default:
		return false
	}
}

// exactQueryAdjustment rewards direct title, filename, path, and preview hits.
func exactQueryAdjustment(profile searchRankProfile, title string, name string, stem string, path string, preview string) float64 {
	if profile.query == "" {
		return 0
	}

	var adjustment float64
	nameWords := normalizeNameWords(name)
	pathBase := normalizeNameWords(normalizeRankText(filepath.Base(path)))
	switch {
	case title == profile.query || name == profile.query || stem == profile.query || nameWords == profile.query || pathBase == profile.query:
		adjustment -= 36
	case strings.Contains(title, profile.query) || strings.Contains(name, profile.query) || strings.Contains(nameWords, profile.query):
		adjustment -= 20
	}

	if strings.HasPrefix(name, profile.query) || strings.HasPrefix(nameWords, profile.query) || strings.HasPrefix(pathBase, profile.query) {
		adjustment -= 10
	}
	if strings.HasPrefix(stem, profile.query) {
		adjustment -= 8
	}
	if strings.Contains(path, profile.query) {
		adjustment -= 8
	}
	if strings.Contains(preview, profile.query) {
		adjustment -= 2
	}

	if profile.compactQuery != "" && profile.compactQuery != profile.query {
		compactTitle := strings.ReplaceAll(title, " ", "")
		compactName := strings.ReplaceAll(nameWords, " ", "")
		if strings.Contains(compactTitle, profile.compactQuery) || strings.Contains(compactName, profile.compactQuery) {
			adjustment -= 14
		}
	}
	return adjustment
}

// tokenAdjustment rewards partial query terms in high-signal fields.
func tokenAdjustment(tokens []string, title string, name string, stem string, path string, preview string) float64 {
	var adjustment float64
	nameWords := normalizeNameWords(name)
	pathBase := normalizeNameWords(normalizeRankText(filepath.Base(path)))
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

// sourceAdjustment gives local files a small lift for file-like searches.
func sourceAdjustment(result model.SearchResult, profile searchRankProfile, path string) float64 {
	if result.Source != "file" {
		return 0
	}

	adjustment := -2.0
	if profile.fileLike || path != "" {
		adjustment -= 3
	}
	fileType := strings.TrimPrefix(strings.ToLower(result.FileType), ".")
	if fileType == "folder" {
		if profile.folderLike {
			adjustment -= 6
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

// noiseAdjustment demotes generated app bundles and browser cache artifacts.
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

// freshnessAdjustment lightly favors recent personal data without hiding old hits.
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

// rankTokens extracts searchable query terms for reranking only.
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

// looksFileLikeQuery reports whether the query appears to target local files.
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

// normalizeRankText lowercases and compacts whitespace for ranking compares.
func normalizeRankText(input string) string {
	return strings.Join(strings.Fields(strings.ToLower(input)), " ")
}

// normalizePathText converts paths to a lowercase slash-separated form.
func normalizePathText(path string) string {
	if path == "" {
		return ""
	}
	return strings.ToLower(filepath.ToSlash(filepath.Clean(path)))
}

// isPersonalFilePath reports whether a path is in a common user document area.
func isPersonalFilePath(path string) bool {
	return strings.Contains(path, "/desktop/") ||
		strings.Contains(path, "/documents/") ||
		strings.Contains(path, "/downloads/")
}

// isNoisySearchPath detects generated app bundles and cache-heavy locations.
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

// isGeneratedAsset reports whether a result looks like a generated web asset.
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

// scanChunk converts one SQL row into a Chunk.
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

// scanSearchResult converts one SQL row into a SearchResult.
func scanSearchResult(rows *sql.Rows) (model.SearchResult, error) {
	var result model.SearchResult
	var content string
	var metadataJSON string
	err := rows.Scan(
		&result.RowID, &result.ItemID, &result.Source, &result.Title, &content,
		&result.Preview, &result.Path, &result.FileType, &result.UpdatedAt,
		&result.Score, &metadataJSON,
	)
	if err != nil {
		return result, err
	}
	if result.Preview == "" {
		result.Preview = content
	}
	result.Metadata = decodeMetadata(metadataJSON)
	return result, nil
}

// encodeMetadata marshals metadata maps for SQLite storage.
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

// decodeMetadata unmarshals metadata and falls back to an empty map.
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

// rollbackUnlessDone rolls back failed transactions.
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

// buildCJKQuery builds an FTS query from generated CJK bigrams.
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

// cjkGrams returns space-separated CJK bigrams for FTS indexing.
func cjkGrams(input string) string {
	return strings.Join(cjkGramList(input), " ")
}

// cjkGramList generates unique CJK unigrams and bigrams from contiguous runs.
func cjkGramList(input string) []string {
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

// isCJK reports whether a rune is in common CJK ranges.
func isCJK(r rune) bool {
	return (r >= 0x3400 && r <= 0x4DBF) ||
		(r >= 0x4E00 && r <= 0x9FFF) ||
		(r >= 0xF900 && r <= 0xFAFF)
}

// quoteMatch escapes a generated token for FTS MATCH syntax.
func quoteMatch(token string) string {
	return `"` + strings.ReplaceAll(token, `"`, `""`) + `"`
}
