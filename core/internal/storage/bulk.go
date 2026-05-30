package storage

import (
	"context"
	"sync"
)

// Tunable for the per-shard bulk batch size. With multi-row INSERTs in
// bulk mode each commit produces ~2048 chunks worth of work at constant
// cost regardless of how big the index has already grown.
const bulkShardBatchSize = 2048

// BulkSession streams prepared items into one writer goroutine per shard.
//
// Each shard's transaction now also holds the file_fingerprints for the
// files it just wrote — fingerprints were moved out of the main DB into
// the shards (sharded by the same key as the file's chunks). This kills
// the previous serialization point where every batch had to wait for a
// single main-DB writer.
//
// There is no per-batch barrier across shards: each shard commits at its
// own pace, so a momentarily slow shard never stalls the fast ones.
type BulkSession struct {
	store     *Store
	ctx       context.Context
	cancel    context.CancelFunc
	shardCh   []chan PreparedItem
	shardWG   sync.WaitGroup
	errOnce   sync.Once
	firstErr  error
	onWritten func(int)
}

// BeginBulk starts one writer goroutine per shard. onWritten, if non-nil,
// is invoked after each shard commit with the number of items that landed
// on disk so callers can drive a progress UI.
//
// Every shard is switched into bulk mode (FTS5 automerge=0,
// synchronous=OFF) so commits stay at constant cost regardless of how
// large the index has already grown. Close() restores normal mode and
// performs a one-shot merge + WAL truncate on every shard.
func (s *Store) BeginBulk(ctx context.Context, onWritten func(int)) *BulkSession {
	bctx, cancel := context.WithCancel(ctx)
	sess := &BulkSession{
		store:     s,
		ctx:       bctx,
		cancel:    cancel,
		shardCh:   make([]chan PreparedItem, len(s.shards)),
		onWritten: onWritten,
	}
	for _, sh := range s.shards {
		if err := sh.beginBulkMode(ctx); err != nil {
			sess.recordErr(err)
		}
	}
	for i := range s.shards {
		ch := make(chan PreparedItem, bulkShardBatchSize*2)
		sess.shardCh[i] = ch
		sess.shardWG.Add(1)
		go sess.runShard(s.shards[i], ch)
	}
	return sess
}

// Submit routes an item to its assigned shard. Blocks if that shard's
// queue is full so back-pressure naturally flows back to extraction workers.
// Returns the recorded error once any writer has failed.
func (b *BulkSession) Submit(item PreparedItem) error {
	if err := b.ctx.Err(); err != nil {
		return b.combinedErr()
	}
	idx := pickShard(item.Item.Source, item.Item.ID)
	select {
	case <-b.ctx.Done():
		return b.combinedErr()
	case b.shardCh[idx] <- item:
		return nil
	}
}

// Close flushes pending items, waits for all writers, runs the post-bulk
// FTS5 merge + WAL truncate on every shard in parallel, and returns the
// first error any writer encountered. Must be called exactly once.
func (b *BulkSession) Close() error {
	for _, ch := range b.shardCh {
		close(ch)
	}
	b.shardWG.Wait()

	finCtx := context.Background()
	var fwg sync.WaitGroup
	for _, sh := range b.store.shards {
		fwg.Add(1)
		go func(sh *chunkShard) {
			defer fwg.Done()
			if err := sh.endBulkMode(finCtx); err != nil {
				b.recordErr(err)
			}
		}(sh)
	}
	fwg.Wait()
	b.cancel()
	return b.firstErr
}

func (b *BulkSession) combinedErr() error {
	if b.firstErr != nil {
		return b.firstErr
	}
	return b.ctx.Err()
}

func (b *BulkSession) recordErr(err error) {
	if err == nil {
		return
	}
	b.errOnce.Do(func() {
		b.firstErr = err
		b.cancel()
	})
}

func (b *BulkSession) runShard(sh *chunkShard, ch chan PreparedItem) {
	defer b.shardWG.Done()
	batch := make([]PreparedItem, 0, bulkShardBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if b.ctx.Err() != nil {
			batch = batch[:0]
			return
		}
		if err := sh.writeEntries(b.ctx, batch); err != nil {
			b.recordErr(err)
			batch = batch[:0]
			return
		}
		n := len(batch)
		batch = batch[:0]
		if b.onWritten != nil {
			b.onWritten(n)
		}
	}

	for item := range ch {
		if b.ctx.Err() != nil {
			continue // drain to unblock producers; do not commit anything
		}
		batch = append(batch, item)
		if len(batch) >= bulkShardBatchSize {
			flush()
		}
	}
	flush()
}
