package indexer

import (
	"context"
	"strings"

	"phantasm/core/internal/model"
	"phantasm/core/internal/storage"
)

// Indexer owns the text cleanup and chunking pipeline.
type Indexer struct {
	store        *storage.Store
	preprocessor *Preprocessor
	chunker      *Chunker
}

// New creates an indexing pipeline backed by SQLite.
func New(store *storage.Store) *Indexer {
	return &Indexer{
		store:        store,
		preprocessor: NewPreprocessor(),
		chunker:      NewChunker(),
	}
}

// IndexItem cleans, chunks, and upserts one item.
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

// PrepareItem preprocesses and splits an item into chunks without writing to
// the database. Returns nil chunks if the item has no indexable content.
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
