package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"

	"recall/core/internal/model"
)

// shardCount is the number of independent SQLite files used to store chunks
// and FTS indexes. Each shard has its own writer connection and mutex so up
// to N inserts run truly in parallel (SQLite only allows one writer per DB
// file, but here we have N DB files).
//
// 4 was chosen as a balance between parallelism and disk overhead — each FTS5
// shadow table carries fixed metadata, so doubling shards doubles that cost.
// On a 12-core machine, 4 shards comfortably saturate disk I/O while keeping
// search fan-out cheap.
const shardCount = 4

// rowIDShardShift packs the shard index into the high byte of a 64-bit rowid
// so callers (e.g. the React UI) can use a single integer as a stable key.
// 56 bits is more than enough for billions of chunks per shard.
const rowIDShardShift = 56
const rowIDLocalMask = (int64(1) << rowIDShardShift) - 1

// encodeGlobalRowID packs shard index and local rowid into one int64.
func encodeGlobalRowID(shardIdx int, localRowID int64) int64 {
	return (int64(shardIdx) << rowIDShardShift) | (localRowID & rowIDLocalMask)
}

// decodeGlobalRowID extracts the shard index and local rowid. Reserved for
// future use (e.g. open-chunk-by-id features).
func decodeGlobalRowID(global int64) (int, int64) {
	return int(global>>rowIDShardShift) & 0xFF, global & rowIDLocalMask
}

var _ = decodeGlobalRowID

// pickShard returns the deterministic shard index for an item.
func pickShard(source, itemID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(source))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(itemID))
	return int(h.Sum32() % shardCount)
}

// chunkShard owns one SQLite file containing chunks + FTS indexes.
type chunkShard struct {
	idx     int
	path    string
	db      *sql.DB
	writeDB *sql.DB
	writeMu sync.Mutex
}

// shardPath returns the on-disk path for shard N adjacent to the main DB.
func shardPath(mainPath string, idx int) string {
	dir := filepath.Dir(mainPath)
	base := strings.TrimSuffix(filepath.Base(mainPath), filepath.Ext(mainPath))
	return filepath.Join(dir, fmt.Sprintf("%s.shard-%d.db", base, idx))
}

// openShards opens (and migrates) all chunk shards alongside the main DB.
func openShards(ctx context.Context, mainPath string) ([]*chunkShard, error) {
	shards := make([]*chunkShard, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		s, err := openShard(ctx, mainPath, i)
		if err != nil {
			for _, opened := range shards {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open shard %d: %w", i, err)
		}
		shards = append(shards, s)
	}
	return shards, nil
}

func openShard(ctx context.Context, mainPath string, idx int) (*chunkShard, error) {
	path := shardPath(mainPath, idx)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	readDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	readDB.SetMaxOpenConns(4)
	readDB.SetMaxIdleConns(4)
	readDB.SetConnMaxLifetime(0)

	writeDB, err := sql.Open("sqlite", path)
	if err != nil {
		_ = readDB.Close()
		return nil, err
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0)

	s := &chunkShard{idx: idx, path: path, db: readDB, writeDB: writeDB}
	if err := applyPragmas(ctx, readDB); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := applyPragmas(ctx, writeDB); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *chunkShard) Close() error {
	var firstErr error
	if s.writeDB != nil {
		if err := s.writeDB.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *chunkShard) migrate(ctx context.Context) error {
	statements := []string{
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
		`CREATE INDEX IF NOT EXISTS idx_chunks_source_item ON chunks(source, item_id)`,
		`CREATE INDEX IF NOT EXISTS idx_chunks_source_time ON chunks(source, updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chunks_file_type ON chunks(file_type)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(
			title,
			content,
			content='',
			tokenize='unicode61 remove_diacritics 2'
		)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS ` + cjkLayeredFTSTable + ` USING fts5(
			grams,
			content='',
			tokenize='unicode61 remove_diacritics 2'
		)`,
	}
	for _, stmt := range statements {
		if _, err := s.writeDB.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// itemHashUpdate carries the content-hash a streaming entry produced so that
// the caller can attach it to the file fingerprint written to the main DB.
type itemHashUpdate struct {
	Source string
	ItemID string
	Hash   string
}

// writeEntries persists every chunk for the given entries in one transaction.
// It returns hash updates collected from streaming entries so the caller can
// stamp those onto file fingerprints in the main DB.
func (s *chunkShard) writeEntries(ctx context.Context, entries []PreparedItem) ([]itemHashUpdate, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	var updates []itemHashUpdate
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	err := retryBusy(ctx, func() (err error) {
		updates = updates[:0]
		tx, err := s.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer rollbackUnlessDone(tx, &err)

		for _, entry := range entries {
			if entry.ChunkSource != nil {
				upd, err := writeStreamingChunks(ctx, tx, entry)
				if err != nil {
					if errors.Is(err, ErrSkipItem) {
						// Volatile file changed mid-read; ignore so the rest
						// of the batch still commits.
						continue
					}
					return err
				}
				updates = append(updates, upd)
				continue
			}
			if entry.IsNew {
				// Defensive cleanup in case a previous run crashed after
				// shard write but before main-DB item/fingerprint write.
				if err := deleteAllChunksForItem(ctx, tx, entry.Item.Source, entry.Item.ID); err != nil {
					return err
				}
				for _, chunk := range entry.Chunks {
					if err := insertChunk(ctx, tx, entry.Item, chunk); err != nil {
						return err
					}
				}
				continue
			}
			oldChunks, err := loadChunks(ctx, tx, entry.Item.Source, entry.Item.ID)
			if err != nil {
				return err
			}
			if err := applyChunkDiff(ctx, tx, entry.Item, oldChunks, entry.Chunks); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return updates, err
}

// writeStreamingChunks consumes a ChunkSource and applies a diff against
// existing chunks for the same item. The accumulated SHA-1 of chunk hashes
// is returned so the main-DB fingerprint write can stamp the final value.
func writeStreamingChunks(ctx context.Context, tx *sql.Tx, entry PreparedItem) (itemHashUpdate, error) {
	oldChunks, err := loadChunks(ctx, tx, entry.Item.Source, entry.Item.ID)
	if err != nil {
		return itemHashUpdate{}, err
	}
	seen := make(map[int]struct{}, len(oldChunks))
	hasher := sha1.New()

	err = entry.ChunkSource(ctx, func(chunk model.Chunk) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(chunk.Content) == "" {
			return nil
		}
		seen[chunk.Ordinal] = struct{}{}
		_, _ = hasher.Write([]byte(chunk.Hash))
		if old, ok := oldChunks[chunk.Ordinal]; ok {
			if old.Hash == chunk.Hash && old.Title == chunk.Title {
				return nil
			}
			return replaceChunk(ctx, tx, old, chunk)
		}
		return insertChunk(ctx, tx, entry.Item, chunk)
	})
	if err != nil {
		return itemHashUpdate{}, err
	}
	for ordinal, old := range oldChunks {
		if _, ok := seen[ordinal]; !ok {
			if err := deleteChunk(ctx, tx, old); err != nil {
				return itemHashUpdate{}, err
			}
		}
	}
	return itemHashUpdate{
		Source: entry.Item.Source,
		ItemID: entry.Item.ID,
		Hash:   hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func deleteAllChunksForItem(ctx context.Context, tx *sql.Tx, source, itemID string) error {
	// Look up existing chunk rowids so we can clean up their FTS entries.
	rows, err := tx.QueryContext(ctx, `
SELECT rowid, chunk_id, item_id, source, title, content, preview, ordinal, hash,
       path, file_type, metadata_json, created_at, updated_at
FROM chunks WHERE source = ? AND item_id = ?`, source, itemID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var existing []model.Chunk
	for rows.Next() {
		c, err := scanChunk(rows)
		if err != nil {
			return err
		}
		existing = append(existing, c)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, c := range existing {
		if err := deleteChunk(ctx, tx, c); err != nil {
			return err
		}
	}
	return nil
}

// searchShard runs an FTS query against this shard and returns candidates
// with rowids already encoded with the shard index.
func (s *chunkShard) searchShard(
	ctx context.Context,
	req model.SearchRequest,
	matchQuery string,
	table string,
	scoreExpr string,
	limit int,
) ([]model.SearchResult, error) {
	where, args := buildSearchWhere(req, matchQuery, table)
	sqlText := fmt.Sprintf(`
SELECT c.rowid, c.item_id, c.source, c.title, c.preview, c.path,
       c.file_type, c.updated_at, c.metadata_json, %s AS score
FROM %s
JOIN chunks c ON c.rowid = %s.rowid
WHERE %s
ORDER BY score ASC, c.updated_at DESC
LIMIT ?`, scoreExpr, table, table, strings.Join(where, " AND "))
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]model.SearchResult, 0, limit)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := scanSearchCandidate(rows)
		if err != nil {
			return nil, err
		}
		result.RowID = encodeGlobalRowID(s.idx, result.RowID)
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *chunkShard) optimize(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.writeDB.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		return err
	}
	if _, err := s.writeDB.ExecContext(ctx, "INSERT INTO chunk_fts(chunk_fts) VALUES('optimize')"); err != nil {
		return err
	}
	if _, err := s.writeDB.ExecContext(ctx,
		"INSERT INTO "+cjkLayeredFTSTable+"("+cjkLayeredFTSTable+") VALUES('optimize')"); err != nil {
		return err
	}
	return nil
}

func (s *chunkShard) warmCache(ctx context.Context) {
	for _, table := range []string{"chunk_fts", cjkLayeredFTSTable} {
		if ctx.Err() != nil {
			return
		}
		_, _ = s.db.ExecContext(ctx, "SELECT COUNT(*) FROM "+table)
	}
}

// runShardsParallel invokes fn on each shard concurrently and returns the
// first non-nil error encountered. Shards continue running even if one fails
// so partial work is not silently lost in unrelated shards.
func runShardsParallel(shards []*chunkShard, fn func(s *chunkShard) error) error {
	if len(shards) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	errs := make([]error, len(shards))
	for i, sh := range shards {
		wg.Add(1)
		go func(idx int, shard *chunkShard) {
			defer wg.Done()
			errs[idx] = fn(shard)
		}(i, sh)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// applyPragmas applies the production SQLite pragmas to a connection pool.
func applyPragmas(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA cache_size=-50000",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA mmap_size=30000000000",
		"PRAGMA foreign_keys=ON",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("%s: %w", pragma, err)
		}
	}
	return nil
}
