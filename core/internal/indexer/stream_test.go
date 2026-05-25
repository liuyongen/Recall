package indexer

import (
	"context"
	"strings"
	"testing"

	"phantasm/core/internal/model"
)

func TestStreamItemChunksMatchesPreparedChunks(t *testing.T) {
	idx := New(nil)
	item := model.DataItem{
		ID:        "item-1",
		Source:    "test",
		Title:     "stream.txt",
		Content:   strings.Repeat("alpha beta gamma. ", 120) + "中文召回测试",
		Metadata:  map[string]any{"path": "stream.txt", "file_type": "txt"},
		CreatedAt: 1,
		UpdatedAt: 2,
	}
	expectedItem := item
	expected := idx.PrepareItem(&expectedItem)

	streamItem := item
	streamItem.Content = ""
	var actual []model.Chunk
	err := idx.StreamItemChunks(context.Background(), streamItem, strings.NewReader(item.Content), func(chunk model.Chunk) error {
		actual = append(actual, chunk)
		return nil
	})
	if err != nil {
		t.Fatalf("stream chunks: %v", err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("chunk count mismatch: got %d want %d", len(actual), len(expected))
	}
	for i := range expected {
		if actual[i].Ordinal != expected[i].Ordinal {
			t.Fatalf("chunk %d ordinal mismatch: got %d want %d", i, actual[i].Ordinal, expected[i].Ordinal)
		}
		if actual[i].Content != expected[i].Content {
			t.Fatalf("chunk %d content mismatch", i)
		}
		if actual[i].Hash != expected[i].Hash {
			t.Fatalf("chunk %d hash mismatch", i)
		}
	}
}

func TestStreamItemChunksDoesNotDuplicateExactChunkSize(t *testing.T) {
	idx := New(nil)
	item := model.DataItem{
		ID:        "item-2",
		Source:    "test",
		Title:     "exact.txt",
		Metadata:  map[string]any{"path": "exact.txt", "file_type": "txt"},
		CreatedAt: 1,
		UpdatedAt: 2,
	}
	content := strings.Repeat("a", defaultChunkSize)

	var chunks []model.Chunk
	err := idx.StreamItemChunks(context.Background(), item, strings.NewReader(content), func(chunk model.Chunk) error {
		chunks = append(chunks, chunk)
		return nil
	})
	if err != nil {
		t.Fatalf("stream chunks: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1", len(chunks))
	}
	if chunks[0].Content != content {
		t.Fatalf("chunk content changed")
	}
}

func TestStreamItemChunksKeepsUnclosedAngleText(t *testing.T) {
	idx := New(nil)
	item := model.DataItem{
		ID:        "item-3",
		Source:    "test",
		Title:     "code.txt",
		Metadata:  map[string]any{"path": "code.txt", "file_type": "txt"},
		CreatedAt: 1,
		UpdatedAt: 2,
	}
	content := "if a < b keep this searchable"

	var chunks []model.Chunk
	err := idx.StreamItemChunks(context.Background(), item, strings.NewReader(content), func(chunk model.Chunk) error {
		chunks = append(chunks, chunk)
		return nil
	})
	if err != nil {
		t.Fatalf("stream chunks: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1", len(chunks))
	}
	if chunks[0].Content != content {
		t.Fatalf("got %q, want %q", chunks[0].Content, content)
	}
}
