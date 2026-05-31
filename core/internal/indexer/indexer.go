package indexer

import (
	"context"
	"strings"

	"recall/core/internal/model"
	"recall/core/internal/storage"
)

// Indexer 负责文本清理和分块流水线。
type Indexer struct {
	store        *storage.Store
	preprocessor *Preprocessor
	chunker      *Chunker
}

// New 创建由 SQLite 支撑的索引流水线。
func New(store *storage.Store) *Indexer {
	return &Indexer{
		store:        store,
		preprocessor: NewPreprocessor(),
		chunker:      NewChunker(),
	}
}

// IndexItem 清理、分块并写入或更新一个条目。
func (i *Indexer) IndexItem(ctx context.Context, item model.DataItem) error {
	item.Content = i.preprocessor.Clean(item.Content)
	item.Preview = strings.TrimSpace(item.Preview)
	if item.Preview == "" {
		item.Preview = previewText(item.Content, 220)
	}
	if item.Content == "" {
		return nil
	}
	return i.store.UpsertItem(ctx, item, i.chunker.Split(item))
}

// PrepareItem 预处理并切分条目，但不写入数据库。
// 如果条目没有可索引内容，则返回 nil 分块。
func (i *Indexer) PrepareItem(item *model.DataItem) []model.Chunk {
	item.Content = i.preprocessor.Clean(item.Content)
	item.Preview = strings.TrimSpace(item.Preview)
	if item.Preview == "" {
		item.Preview = previewText(item.Content, 220)
	}
	if item.Content == "" {
		return nil
	}
	return i.chunker.Split(*item)
}
