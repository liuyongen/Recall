package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"recall/core/internal/model"
)

// bulkChunkCols is the column count of the chunks table insert below.
// Keep in sync with bulkInsertChunks.
const bulkChunkCols = 13

// bulkInsertEntries performs the absolute minimum SQL work for a batch of
// brand-new items inside an open transaction:
//   - One multi-row INSERT into chunks per ~500 chunk window.
//   - One multi-row INSERT into chunk_fts per same window (with rowids
//     captured from the chunks insert via SELECT max(rowid)).
//   - One multi-row INSERT into cjk_layered_fts only for chunks whose
//     CJKGrams string is non-empty.
//   - One multi-row INSERT into file_fingerprints per ~500 row window.
//
// Multi-row inserts cut modernc.org/sqlite per-statement overhead from
// thousands of parse/prepare cycles to a handful. Combined with bulk
// mode (synchronous=OFF + automerge=0) this brings shard throughput up
// to the levels the workers can actually feed.
func bulkInsertEntries(ctx context.Context, tx *sql.Tx, entries []PreparedItem) error {
	// 1. Gather and persist chunks; capture starting rowid so the matching
	//    FTS rows can reference rowid=startRowID+offset without round-trips.
	type rowMeta struct {
		startRow int64
		chunks   []model.Chunk
	}

	allChunks := make([]model.Chunk, 0, len(entries)*2)
	for _, e := range entries {
		for _, c := range e.Chunks {
			if strings.TrimSpace(c.Content) == "" {
				continue
			}
			allChunks = append(allChunks, c)
		}
	}

	if len(allChunks) > 0 {
		// modernc default SQLITE_LIMIT_VARIABLE_NUMBER ~32k; 500 rows * 13
		// cols = 6500 params per statement leaves plenty of headroom.
		const maxRows = 500
		for start := 0; start < len(allChunks); start += maxRows {
			end := start + maxRows
			if end > len(allChunks) {
				end = len(allChunks)
			}
			window := allChunks[start:end]
			firstRow, err := insertChunksBulk(ctx, tx, window)
			if err != nil {
				return err
			}
			meta := rowMeta{startRow: firstRow, chunks: window}
			if err := insertChunkFTSBulk(ctx, tx, meta); err != nil {
				return err
			}
			if err := insertCJKBulk(ctx, tx, meta); err != nil {
				return err
			}
		}
	}

	// 2. Persist fingerprints (one per file, not per chunk).
	fps := make([]FileFingerprint, 0, len(entries))
	for _, e := range entries {
		if e.Fingerprint == nil || e.Fingerprint.Path == "" {
			continue
		}
		fps = append(fps, *e.Fingerprint)
	}
	if len(fps) > 0 {
		const maxRows = 800
		for start := 0; start < len(fps); start += maxRows {
			end := start + maxRows
			if end > len(fps) {
				end = len(fps)
			}
			if err := insertFingerprintsBulk(ctx, tx, fps[start:end]); err != nil {
				return err
			}
		}
	}

	return nil
}

func insertChunksBulk(ctx context.Context, tx *sql.Tx, chunks []model.Chunk) (int64, error) {
	if len(chunks) == 0 {
		return 0, nil
	}
	var sb strings.Builder
	sb.Grow(256 + 30*len(chunks))
	sb.WriteString(`INSERT INTO chunks(chunk_id, item_id, source, title, content, preview,
ordinal, hash, path, file_type, metadata_json, created_at, updated_at) VALUES `)
	args := make([]any, 0, bulkChunkCols*len(chunks))
	for i, c := range chunks {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?)")
		meta, err := encodeMetadata(c.Metadata)
		if err != nil {
			return 0, err
		}
		args = append(args,
			c.ChunkID, c.ItemID, c.Source, c.Title, c.Content, c.Preview,
			c.Ordinal, c.Hash, c.Path, c.FileType, meta, c.CreatedAt, c.UpdatedAt,
		)
	}
	res, err := tx.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("bulk insert chunks: %w", err)
	}
	last, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// AUTOINCREMENT assigns sequential rowids per statement, so the first
	// rowid in this batch is last - (len-1).
	return last - int64(len(chunks)-1), nil
}

func insertChunkFTSBulk(ctx context.Context, tx *sql.Tx, m struct {
	startRow int64
	chunks   []model.Chunk
}) error {
	if len(m.chunks) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.Grow(64 + 12*len(m.chunks))
	sb.WriteString("INSERT INTO chunk_fts(rowid, title, content) VALUES ")
	args := make([]any, 0, 3*len(m.chunks))
	for i, c := range m.chunks {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?)")
		args = append(args, m.startRow+int64(i), c.Title, c.Content)
	}
	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("bulk insert chunk_fts: %w", err)
	}
	return nil
}

func insertCJKBulk(ctx context.Context, tx *sql.Tx, m struct {
	startRow int64
	chunks   []model.Chunk
}) error {
	// Collect only non-empty bigrams so the FTS5 index stays compact.
	type row struct {
		rowID int64
		grams string
	}
	rows := make([]row, 0, len(m.chunks))
	for i, c := range m.chunks {
		grams := c.CJKGrams
		if grams == noCJKGrams {
			continue
		}
		if grams == "" {
			grams = cjkGrams(cjkIndexText(c))
		}
		if grams == "" {
			continue
		}
		rows = append(rows, row{rowID: m.startRow + int64(i), grams: grams})
	}
	if len(rows) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.Grow(64 + 8*len(rows))
	sb.WriteString("INSERT INTO " + cjkLayeredFTSTable + "(rowid, grams) VALUES ")
	args := make([]any, 0, 2*len(rows))
	for i, r := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?)")
		args = append(args, r.rowID, r.grams)
	}
	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("bulk insert cjk fts: %w", err)
	}
	return nil
}

func insertFingerprintsBulk(ctx context.Context, tx *sql.Tx, fps []FileFingerprint) error {
	if len(fps) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.Grow(128 + 20*len(fps))
	sb.WriteString(`INSERT INTO file_fingerprints(path, size, mod_time_ns, content_hash, updated_at) VALUES `)
	now := time.Now().Unix()
	args := make([]any, 0, 5*len(fps))
	for i, fp := range fps {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?)")
		args = append(args, normalizeFingerprintPath(fp.Path), fp.Size, fp.ModTimeNS, fp.ContentHash, now)
	}
	sb.WriteString(` ON CONFLICT(path) DO UPDATE SET
  size = excluded.size,
  mod_time_ns = excluded.mod_time_ns,
  content_hash = excluded.content_hash,
  updated_at = excluded.updated_at`)
	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("bulk insert fingerprints: %w", err)
	}
	return nil
}

// upsertFingerprintTx writes a single fingerprint inside an open transaction.
// Used by the non-bulk (diff / streaming) paths.
func upsertFingerprintTx(ctx context.Context, tx *sql.Tx, fp FileFingerprint) error {
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
