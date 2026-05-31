package storage

import (
	"context"
	"sync"
)

// bulkShardBatchSize 控制每个分片写入 goroutine 一次提交的条目数上限。
// 每个条目可能包含多个分块，真正的 SQL 插入窗口由 shard_bulk.go 再拆分。
const bulkShardBatchSize = 2048

// BulkSession 将准备好的条目流式送入每个分片各自的写入 goroutine。
//
// 每个分片的事务现在也包含刚写入文件的 file_fingerprints；
// 指纹已经从主库移入分片库，并按与文件分块相同的 key 分片。
// 这样消除了旧设计里每个批次都必须等待单个主库写入器的串行点。
//
// 分片之间没有每批次屏障：每个分片按自己的节奏提交，
// 因而瞬时变慢的分片不会拖住更快的分片。
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

// BeginBulk 为每个分片启动一个写入 goroutine。onWritten 非 nil 时，
// 会在每个分片提交后用已落盘条目数调用，方便调用方驱动进度 UI。
//
// 每个分片都会切换到批量模式（FTS5 automerge=0、synchronous=OFF），
// 降低批量写入期间的提交成本。Close() 会恢复普通模式，
// 并执行轻量 checkpoint；重型 FTS 整理由周期性或手动 Optimize 处理。
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

// Submit 将条目路由到所属分片。如果该分片队列已满会阻塞，
// 让背压自然传回提取工作线程。任一写入器失败后会返回已记录的错误。
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

// Close 刷新待写入条目，等待所有写入器完成，并行恢复每个分片的批量模式设置，
// 然后返回任一写入器遇到的首个错误。
// 必须且只能调用一次。
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
			continue // 继续排空以解除生产者阻塞，但不提交任何内容
		}
		batch = append(batch, item)
		if len(batch) >= bulkShardBatchSize {
			flush()
		}
	}
	flush()
}
