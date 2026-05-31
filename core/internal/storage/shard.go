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

// shardCount 是用于存储分块和 FTS 索引的独立 SQLite 文件数量。
// 每个分片都有自己的写连接和互斥锁，因此最多 N 路插入可以真正并行
// （SQLite 每个数据库文件只允许一个写入器，但这里有 N 个数据库文件）。
//
// 8 个分片能给批量索引器足够多的独立 SQLite 写入器，让 SSD 保持忙碌。
// 这个规模下搜索扇出成本仍然很低，而且项目足够新，无需兼容旧的 4 分片布局。
const shardCount = 8

// rowIDShardShift 将分片索引打包进 64 位 rowid 的高字节，
// 让调用方（如 React UI）可以用单个整数作为稳定 key。
// 56 位足以支撑每个分片数十亿个分块。
const rowIDShardShift = 56
const rowIDLocalMask = (int64(1) << rowIDShardShift) - 1

// encodeGlobalRowID 将分片索引和本地 rowid 打包为一个 int64。
func encodeGlobalRowID(shardIdx int, localRowID int64) int64 {
	return (int64(shardIdx) << rowIDShardShift) | (localRowID & rowIDLocalMask)
}

// decodeGlobalRowID 提取分片索引和本地 rowid。
// 保留给未来功能使用，例如按 id 打开分块。
func decodeGlobalRowID(global int64) (int, int64) {
	return int(global>>rowIDShardShift) & 0xFF, global & rowIDLocalMask
}

var _ = decodeGlobalRowID

// pickShard 返回条目的确定性分片索引。
func pickShard(source, itemID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(source))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(itemID))
	return int(h.Sum32() % shardCount)
}

// chunkShard 拥有一个包含分块和 FTS 索引的 SQLite 文件。
type chunkShard struct {
	idx     int
	path    string
	db      *sql.DB
	writeDB *sql.DB
	writeMu sync.Mutex
	// commitsSinceMaint 记录自上次增量 FTS 合并 / WAL checkpoint 后
	// 已提交的写事务数量，用于在长时间索引中维持稳定吞吐。
	commitsSinceMaint int
	// bulkMode 会禁用逐提交维护（FTS5 增量合并、WAL checkpoint），
	// 并关闭同步写盘，让批量导入阶段优先保证吞吐。
	// endBulkMode 会恢复普通设置并做轻量 checkpoint；
	// 重型 FTS 合并交给周期性或手动 Optimize，避免收尾阶段长时间阻塞。
	bulkMode bool
}

// shardPath 返回主库旁边第 N 个分片的磁盘路径。
func shardPath(mainPath string, idx int) string {
	dir := filepath.Dir(mainPath)
	base := strings.TrimSuffix(filepath.Base(mainPath), filepath.Ext(mainPath))
	return filepath.Join(dir, fmt.Sprintf("%s.shard-%d.db", base, idx))
}

// openShards 打开并迁移主库旁边的所有分块分片。
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
	// chunks 故意不加 UNIQUE 约束。旧版本有 `chunk_id UNIQUE` 和
	// `UNIQUE(source, item_id, ordinal)`，但查询从未使用它们；
	// 唯一性由应用层保证（loadChunks + diff）。每个 UNIQUE 都会创建隐藏 b-tree，
	// 每次插入都要探测和更新；当这棵 b-tree 放不进 SQLite 页缓存后，
	// 吞吐会随着索引增长而崩掉。idx_chunks_source_time / idx_chunks_file_type
	// 也是同理：查询规划器从未选择它们（FTS5 MATCH 占主导），因此只是写放大。
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
		// file_fingerprints 放在分片里，让每个文件的分块和指纹在同一个事务中提交。
		// 路径会通过 pickShard() 哈希到与该文件分块相同的分片。
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
		// 为稳定状态（批量之后）的工作负载设置普通 automerge。
		// 批量索引期间 BulkSession 会把它切到 0，以减少写入时的合并成本；
		// endBulkMode 随后恢复这里的 automerge。
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

// writeEntries 在一个事务中持久化给定条目的所有分块。
// bulkMode 下，已物化条目使用多行 INSERT 语句，比 modernc.org/sqlite 的逐行插入快得多。
// 非新条目会先删除旧分块再整体写入；带 ChunkSource 的流式条目仍走分块 diff。
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
						// 易变文件在读取中途发生变化；忽略它，让批次其余部分仍可提交。
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
				// 防御性清理，处理上一次运行在写入中途崩溃的情况。
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

// runWriteMaintenance 通过偶尔截断 WAL，并给 FTS5 增量合并器喂一点工作，
// 防止长时间索引逐渐变慢。这两个操作都不会阻塞读取器。
// 必须在持有 writeMu 时调用。bulkMode 启用时会完全跳过，
// 让高速摄入保持每次提交 O(1) 的成本。
func (s *chunkShard) runWriteMaintenance(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if s.bulkMode {
		return
	}
	s.commitsSinceMaint++
	// 每隔几次提交，请 FTS5 从最大段层级合并有限页数。
	// automerge=16 时，段会在合并之间自由累积；这里渐进喂入工作，
	// 避免未来某次提交被“危机合并”卡住。
	if s.commitsSinceMaint%4 == 0 {
		// 负参数表示增量、受页数限制的合并（FTS5 文档）。
		_, _ = s.writeDB.ExecContext(ctx,
			"INSERT INTO chunk_fts(chunk_fts, rank) VALUES('merge', -64)")
		_, _ = s.writeDB.ExecContext(ctx,
			"INSERT INTO "+cjkLayeredFTSTable+"("+cjkLayeredFTSTable+", rank) VALUES('merge', -64)")
	}
	// 约每 16 次提交截断一次 WAL。PASSIVE 不会阻塞读取器或其他写入器；
	// 如果仍有读取器固定 WAL，它会静默无操作。
	if s.commitsSinceMaint >= 16 {
		s.commitsSinceMaint = 0
		_, _ = s.writeDB.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	}
}

// beginBulkMode 禁用 FTS5 自动段合并、逐提交维护和 fsync（synchronous=OFF），
// 让高速摄入优先保证吞吐。必须与 endBulkMode 配对使用，后者会恢复正常持久性。
//
// 安全性：批量索引期间崩溃可能损坏分片。这里可以接受，因为批量运行可通过重建索引恢复。
// endBulkMode 会先把 synchronous 恢复为 NORMAL，再执行 PASSIVE checkpoint；
// 后续写入会回到普通 WAL 持久性策略。
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
	// synchronous=OFF 移除每次提交后的 fsync。Windows 上每次 fsync 都是数毫秒级；
	// 对 5 万文件的索引运行来说能节省数分钟。代价是批量阶段崩溃恢复能力变弱，
	// 但该阶段可通过重新索引恢复。
	if _, err := s.writeDB.ExecContext(ctx, "PRAGMA synchronous=OFF"); err != nil {
		return err
	}
	s.bulkMode = true
	return nil
}

// endBulkMode 重新启用自动合并，恢复 synchronous=NORMAL，并执行一次 PASSIVE checkpoint。
// bulkMode 已经为 false 时调用也是安全的。
func (s *chunkShard) endBulkMode(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !s.bulkMode {
		return nil
	}
	s.bulkMode = false
	s.commitsSinceMaint = 0

	// 在批量后的合并前恢复持久性，确保后续任何错误都在完整 fsync 保护下落盘。
	_, _ = s.writeDB.ExecContext(ctx, "PRAGMA synchronous=NORMAL")

	// 恢复普通 automerge，让未来增量写入可以自维护。
	_, _ = s.writeDB.ExecContext(ctx,
		"INSERT INTO chunk_fts(chunk_fts, rank) VALUES('automerge', 16)")
	_, _ = s.writeDB.ExecContext(ctx,
		"INSERT INTO "+cjkLayeredFTSTable+"("+cjkLayeredFTSTable+", rank) VALUES('automerge', 16)")

	// 保持索引完成阶段快速。这里只做 PASSIVE checkpoint；
	// 重型 FTS 整理由周期性 / 手动 Optimize 处理。
	_, _ = s.writeDB.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	return nil
}

// writeStreamingChunks 消费 ChunkSource，并与同一条目的既有分块做 diff。
// 它返回分块哈希累积得到的 SHA-1，让文件指纹可以记录最终内容哈希。
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
	// 查询既有分块 rowid，以便清理对应的 FTS 条目。
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

func (s *chunkShard) deleteFilePath(ctx context.Context, normalizedPath string) error {
	if normalizedPath == "" {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	return retryBusy(ctx, func() (err error) {
		tx, err := s.writeDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer rollbackUnlessDone(tx, &err)

		pathExpr := `lower(replace(path, '\', '/'))`
		likePath := escapeLikePath(strings.TrimRight(normalizedPath, "/")) + "/%"
		rows, err := tx.QueryContext(ctx, `
SELECT rowid, chunk_id, item_id, source, title, content, preview, ordinal, hash,
       path, file_type, metadata_json, created_at, updated_at
FROM chunks
WHERE source = 'file' AND (`+pathExpr+` = ? OR `+pathExpr+` LIKE ? ESCAPE '\')`,
			normalizedPath, likePath)
		if err != nil {
			return err
		}
		var existing []model.Chunk
		for rows.Next() {
			chunk, err := scanChunk(rows)
			if err != nil {
				rows.Close()
				return err
			}
			existing = append(existing, chunk)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		for _, chunk := range existing {
			if err := deleteChunk(ctx, tx, chunk); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `
DELETE FROM file_fingerprints
WHERE path = ? OR path LIKE ? ESCAPE '\'`,
			normalizedPath, likePath)
		if err != nil {
			return err
		}
		return tx.Commit()
	})
}

func escapeLikePath(path string) string {
	var b strings.Builder
	b.Grow(len(path))
	for _, r := range path {
		switch r {
		case '\\', '%', '_':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// searchShard 在该分片上执行 FTS 查询，并返回已编码分片索引的候选 rowid。
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

// runShardsParallel 在每个分片上并发调用 fn，并返回遇到的首个非 nil 错误。
// 即使某个分片失败，其他分片也会继续运行，避免无关分片的部分工作被静默丢失。
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

// applyPragmas 将生产环境 SQLite pragma 应用到连接池。
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
