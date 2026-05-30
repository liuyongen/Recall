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
// 8 shards give the bulk indexer enough independent SQLite writers to keep an
// SSD busy. Search still fans out cheaply at this size, and this project is
// new enough that we do not need to preserve older 4-shard layouts.
const shardCount = 8

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
	// commitsSinceMaint counts how many write transactions have committed
	// since the last incremental FTS merge / WAL checkpoint. Used to keep
	// throughput steady on long indexing runs.
	commitsSinceMaint int
	// bulkMode disables all per-commit maintenance (FTS5 merge checks,
	// WAL checkpoints). FTS5 segments accumulate freely, which keeps
	// every commit O(1) regardless of index size. endBulkMode consolidates
	// the segments and trims WAL once, eliminating the gradual slowdown
	// caused by LSM-style automatic merges.
	bulkMode bool
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
	// chunks deliberately has NO UNIQUE constraints. Earlier versions had
	// `chunk_id UNIQUE` and `UNIQUE(source, item_id, ordinal)` but neither
	// was ever queried — uniqueness was enforced by the application layer
	// (loadChunks + diff). Each UNIQUE created a hidden b-tree that had to
	// be probed and updated on every insert; once that b-tree no longer fit
	// in SQLite's page cache, throughput collapsed as the index grew. Same
	// reasoning applies to idx_chunks_source_time / idx_chunks_file_type:
	// the query planner never picked them (FTS5 MATCH dominates), so they
	// were pure write amplification.
	statements := []string{
		`CREATE TABLE IF NOT EXISTS chunks (
			rowid INTEGER PRIMARY KEY,
			chunk_id TEXT NOT NULL,
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
			updated_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_chunks_source_item ON chunks(source, item_id)`,
		// file_fingerprints lives in the shard so each file's chunks +
		// fingerprint commit together in a single transaction. The path is
		// hashed into the same shard as the file's chunks via pickShard().
		`CREATE TABLE IF NOT EXISTS file_fingerprints (
			path TEXT PRIMARY KEY,
			size INTEGER NOT NULL,
			mod_time_ns INTEGER NOT NULL,
			content_hash TEXT NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
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
		// Set normal automerge for the steady-state (post-bulk) workload.
		// During bulk indexing the BulkSession switches this to 0 so commits
		// stay O(1) regardless of how big the index has grown; endBulkMode
		// then forces a one-shot merge before restoring automerge here.
		`INSERT INTO chunk_fts(chunk_fts, rank) VALUES('automerge', 16)`,
		`INSERT INTO ` + cjkLayeredFTSTable + `(` + cjkLayeredFTSTable + `, rank) VALUES('automerge', 16)`,
	}
	for _, stmt := range statements {
		if _, err := s.writeDB.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// writeEntries persists every chunk for the given entries in one transaction.
// In bulkMode the fast path uses multi-row INSERT statements which are much
// faster than per-row inserts with modernc.org/sqlite. Changed existing items
// are replaced wholesale instead of chunk-diffed; unchanged files are filtered
// before this path by fingerprints, so the extra diff precision is not worth
// the write amplification during high-rate indexing.
func (s *chunkShard) writeEntries(ctx context.Context, entries []PreparedItem) error {
	if len(entries) == 0 {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	err := retryBusy(ctx, func() (err error) {
		tx, err := s.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer rollbackUnlessDone(tx, &err)

		if s.bulkMode {
			if err := s.writeBulkModeEntries(ctx, tx, entries); err != nil {
				return err
			}
			return tx.Commit()
		}

		for _, entry := range entries {
			if entry.ChunkSource != nil {
				hash, err := writeStreamingChunks(ctx, tx, entry)
				if err != nil {
					if errors.Is(err, ErrSkipItem) {
						// Volatile file changed mid-read; ignore so the rest
						// of the batch still commits.
						continue
					}
					return err
				}
				if entry.Fingerprint != nil {
					fp := *entry.Fingerprint
					fp.ContentHash = hash
					if err := upsertFingerprintTx(ctx, tx, fp); err != nil {
						return err
					}
				}
				continue
			}
			if entry.IsNew {
				// Defensive cleanup in case a previous run crashed mid-write.
				if err := deleteAllChunksForItem(ctx, tx, entry.Item.Source, entry.Item.ID); err != nil {
					return err
				}
				for _, chunk := range entry.Chunks {
					if err := insertChunk(ctx, tx, entry.Item, chunk); err != nil {
						return err
					}
				}
			} else {
				oldChunks, err := loadChunks(ctx, tx, entry.Item.Source, entry.Item.ID)
				if err != nil {
					return err
				}
				if err := applyChunkDiff(ctx, tx, entry.Item, oldChunks, entry.Chunks); err != nil {
					return err
				}
			}
			if entry.Fingerprint != nil {
				if err := upsertFingerprintTx(ctx, tx, *entry.Fingerprint); err != nil {
					return err
				}
			}
		}
		return tx.Commit()
	})
	if err == nil {
		s.runWriteMaintenance(ctx)
	}
	return err
}

func (s *chunkShard) writeBulkModeEntries(ctx context.Context, tx *sql.Tx, entries []PreparedItem) error {
	materialized := make([]PreparedItem, 0, len(entries))
	for _, entry := range entries {
		if entry.ChunkSource != nil {
			continue
		}
		if !entry.IsNew {
			if err := deleteAllChunksForItem(ctx, tx, entry.Item.Source, entry.Item.ID); err != nil {
				return err
			}
		}
		materialized = append(materialized, entry)
	}
	if err := bulkInsertEntries(ctx, tx, materialized); err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.ChunkSource == nil {
			continue
		}
		hash, err := writeStreamingChunks(ctx, tx, entry)
		if err != nil {
			if errors.Is(err, ErrSkipItem) {
				continue
			}
			return err
		}
		if entry.Fingerprint != nil {
			fp := *entry.Fingerprint
			fp.ContentHash = hash
			if err := upsertFingerprintTx(ctx, tx, fp); err != nil {
				return err
			}
		}
	}
	return nil
}

// runWriteMaintenance keeps long indexing runs from slowing down by
// occasionally trimming the WAL and feeding the FTS5 incremental merger
// a small slice of work. Both operations are non-blocking for readers.
// Must be called while holding writeMu. Skipped entirely while bulkMode
// is active so high-rate ingestion stays at O(1) cost per commit.
func (s *chunkShard) runWriteMaintenance(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if s.bulkMode {
		return
	}
	s.commitsSinceMaint++
	// Every few commits, ask FTS5 to merge a bounded number of pages from
	// its largest segment level. With automerge=16 segments accumulate
	// freely between merges, so we drip-feed work here instead of letting
	// a "crisis merge" stall a future commit.
	if s.commitsSinceMaint%4 == 0 {
		// Negative argument = incremental, page-bounded merge (FTS5 docs).
		_, _ = s.writeDB.ExecContext(ctx,
			"INSERT INTO chunk_fts(chunk_fts, rank) VALUES('merge', -64)")
		_, _ = s.writeDB.ExecContext(ctx,
			"INSERT INTO "+cjkLayeredFTSTable+"("+cjkLayeredFTSTable+", rank) VALUES('merge', -64)")
	}
	// Trim WAL roughly every 16 commits. PASSIVE never blocks readers or
	// other writers and silently no-ops if a reader still pins the WAL.
	if s.commitsSinceMaint >= 16 {
		s.commitsSinceMaint = 0
		_, _ = s.writeDB.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	}
}

// beginBulkMode disables FTS5 automatic segment merges, per-commit
// maintenance, and fsync (synchronous=OFF) so high-rate ingestion stays
// at constant cost. Must be paired with endBulkMode which restores normal
// durability and consolidates the FTS5 segments accumulated during bulk.
//
// Safety: a crash during bulk indexing can corrupt a shard. That's
// acceptable here because bulk runs are recoverable by re-indexing.
// endBulkMode runs a TRUNCATE checkpoint that fsyncs everything before
// returning, so once it completes the shard is durable.
func (s *chunkShard) beginBulkMode(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.bulkMode {
		return nil
	}
	if _, err := s.writeDB.ExecContext(ctx,
		"INSERT INTO chunk_fts(chunk_fts, rank) VALUES('automerge', 0)"); err != nil {
		return err
	}
	if _, err := s.writeDB.ExecContext(ctx,
		"INSERT INTO "+cjkLayeredFTSTable+"("+cjkLayeredFTSTable+", rank) VALUES('automerge', 0)"); err != nil {
		return err
	}
	// synchronous=OFF removes the fsync after every commit. On Windows
	// each fsync is multi-millisecond; for a 50K file run this saves
	// minutes. Combined with WAL it remains crash-safe for committed
	// pages still in the WAL — only an OS-level crash mid-write can
	// corrupt, and we resync after such a crash anyway.
	if _, err := s.writeDB.ExecContext(ctx, "PRAGMA synchronous=OFF"); err != nil {
		return err
	}
	s.bulkMode = true
	return nil
}

// endBulkMode re-enables automatic merging, forces one big merge to
// consolidate all segments accumulated during bulk load, and truncates the
// WAL. Returns after all maintenance commits land on disk so subsequent
// reads see a tidy index. Safe to call when bulkMode is already false.
func (s *chunkShard) endBulkMode(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !s.bulkMode {
		return nil
	}
	s.bulkMode = false
	s.commitsSinceMaint = 0

	// Restore durability before the post-bulk merge so any errors there
	// land on disk with full fsync protection.
	_, _ = s.writeDB.ExecContext(ctx, "PRAGMA synchronous=NORMAL")

	// Restore normal automerge so future incremental writes self-maintain.
	_, _ = s.writeDB.ExecContext(ctx,
		"INSERT INTO chunk_fts(chunk_fts, rank) VALUES('automerge', 16)")
	_, _ = s.writeDB.ExecContext(ctx,
		"INSERT INTO "+cjkLayeredFTSTable+"("+cjkLayeredFTSTable+", rank) VALUES('automerge', 16)")

	// Keep indexing completion fast. Heavy FTS consolidation is handled by
	// periodic/manual Optimize; doing it synchronously here is exactly what
	// makes long full-disk runs feel slower at the end.
	_, _ = s.writeDB.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	return nil
}

// writeStreamingChunks consumes a ChunkSource and applies a diff against
// existing chunks for the same item. The accumulated SHA-1 of chunk hashes
// is returned so the main-DB fingerprint write can stamp the final value.
func writeStreamingChunks(ctx context.Context, tx *sql.Tx, entry PreparedItem) (string, error) {
	oldChunks, err := loadChunks(ctx, tx, entry.Item.Source, entry.Item.ID)
	if err != nil {
		return "", err
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
		return "", err
	}
	for ordinal, old := range oldChunks {
		if _, ok := seen[ordinal]; !ok {
			if err := deleteChunk(ctx, tx, old); err != nil {
				return "", err
			}
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
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
