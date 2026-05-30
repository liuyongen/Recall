package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"recall/core/internal/adapters"
	"recall/core/internal/model"
)

func TestContentBackfillReplacesPathOnlyIndex(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "note.txt")
	content := "needle-content-backfill-only"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	ctx := context.Background()
	eng, err := New(ctx, Config{DBPath: filepath.Join(t.TempDir(), "recall.db")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer eng.Close()

	adapter := adapters.NewFileAdapter([]string{root}, eng.extractor, eng.maxBytes)
	adapter.PathOnly = true
	if _, err := eng.syncFileAdapter(ctx, adapter, 0, pathSyncKey(root)); err != nil {
		t.Fatalf("path-only sync: %v", err)
	}

	before, err := eng.Search(ctx, model.SearchRequest{Query: content, Limit: 10})
	if err != nil {
		t.Fatalf("search before backfill: %v", err)
	}
	if before.Total != 0 {
		t.Fatalf("path-only index unexpectedly found content: %d hits", before.Total)
	}

	eng.backfillContent(root, eng.maxBytes)

	after, err := eng.Search(ctx, model.SearchRequest{Query: content, Limit: 10})
	if err != nil {
		t.Fatalf("search after backfill: %v", err)
	}
	if after.Total == 0 {
		t.Fatalf("content backfill did not make file content searchable")
	}
}
