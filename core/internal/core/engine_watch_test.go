package core_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"recall/core/internal/core"
	"recall/core/internal/model"
)

// TestIncrementalIndex verifies that a second IndexPath call only processes
// files modified after the first call, and that the indexed count reflects
// the incremental behavior.
func TestIncrementalIndex(t *testing.T) {
	dir := t.TempDir()

	// Write first file before initial index.
	writeFile(t, filepath.Join(dir, "a.txt"), "hello world incremental")

	dbPath := filepath.Join(t.TempDir(), "test.db")
	cfg := core.Config{DBPath: dbPath}

	ctx := context.Background()
	eng, err := core.New(ctx, cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer eng.Close()

	// First full index: should find a.txt.
	summary1, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: dir})
	if err != nil {
		t.Fatalf("IndexPath #1: %v", err)
	}
	if summary1.Indexed == 0 {
		t.Fatalf("expected >=1 indexed on first pass, got %d", summary1.Indexed)
	}
	t.Logf("Pass 1: scanned=%d indexed=%d skipped=%d", summary1.Scanned, summary1.Indexed, summary1.Skipped)

	// Second pass immediately: no new/changed files → nothing new indexed.
	summary2, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: dir})
	if err != nil {
		t.Fatalf("IndexPath #2: %v", err)
	}
	if summary2.Indexed > 0 {
		t.Errorf("expected 0 newly indexed on unchanged second pass, got %d", summary2.Indexed)
	}
	t.Logf("Pass 2 (incremental, no changes): scanned=%d indexed=%d", summary2.Scanned, summary2.Indexed)

	// Add a new file and sleep 1s so its mtime is strictly after the stored sync time.
	time.Sleep(1100 * time.Millisecond)
	writeFile(t, filepath.Join(dir, "b.txt"), "new file after first sync")

	summary3, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: dir})
	if err != nil {
		t.Fatalf("IndexPath #3: %v", err)
	}
	if summary3.Indexed == 0 {
		t.Errorf("expected >=1 indexed after adding b.txt, got %d", summary3.Indexed)
	}
	t.Logf("Pass 3 (incremental, new file): scanned=%d indexed=%d", summary3.Scanned, summary3.Indexed)

	// Verify both files are searchable.
	for _, query := range []string{"hello world", "new file after"} {
		res, err := eng.Search(ctx, model.SearchRequest{Query: query, Limit: 5})
		if err != nil {
			t.Fatalf("Search(%q): %v", query, err)
		}
		if res.Total == 0 {
			t.Errorf("Search(%q): expected results, got 0", query)
		}
		t.Logf("Search(%q): %d results", query, res.Total)
	}
}

// TestAutoWatch verifies that file changes are automatically detected and
// indexed without a manual IndexPath call.
func TestAutoWatch(t *testing.T) {
	dir := t.TempDir()

	// Write initial file.
	writeFile(t, filepath.Join(dir, "init.txt"), "initial content")

	dbPath := filepath.Join(t.TempDir(), "watch.db")
	cfg := core.Config{DBPath: dbPath}

	ctx := context.Background()
	eng, err := core.New(ctx, cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer eng.Close()

	// Index the directory — this also starts watching it.
	if _, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: dir}); err != nil {
		t.Fatalf("IndexPath: %v", err)
	}

	// Search for initial file.
	assertSearchHits(t, eng, ctx, "initial content", 1)

	// Write a new file directly to the watched directory.
	writeFile(t, filepath.Join(dir, "auto.txt"), "auto watched file content")

	// Wait for the watcher's debounce ticker (1s) + processing time.
	time.Sleep(2500 * time.Millisecond)

	// The new file should now be searchable without any manual IndexPath call.
	assertSearchHits(t, eng, ctx, "auto watched file content", 1)
	t.Log("Auto-watch: new file detected and indexed automatically ✓")

	// Modify the initial file.
	writeFile(t, filepath.Join(dir, "init.txt"), "modified initial content")
	time.Sleep(2500 * time.Millisecond)

	assertSearchHits(t, eng, ctx, "modified initial content", 1)
	t.Log("Auto-watch: modified file re-indexed automatically ✓")
}

func TestAutoWatchRemovesDeletedFiles(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "delete-me.txt")
	writeFile(t, filePath, "auto watch deleted file content")

	dbPath := filepath.Join(t.TempDir(), "watch-delete.db")
	ctx := context.Background()
	eng, err := core.New(ctx, core.Config{DBPath: dbPath})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer eng.Close()

	if _, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: dir}); err != nil {
		t.Fatalf("IndexPath: %v", err)
	}
	assertSearchHits(t, eng, ctx, "auto watch deleted file content", 1)

	if err := os.Remove(filePath); err != nil {
		t.Fatalf("Remove(%s): %v", filePath, err)
	}
	time.Sleep(2500 * time.Millisecond)

	assertSearchTotal(t, eng, ctx, "auto watch deleted file content", 0)
}

func TestAutoWatchRemovesDeletedDirectoryTree(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	writeFile(t, filepath.Join(nested, "inside.txt"), "auto watch deleted directory content")

	dbPath := filepath.Join(t.TempDir(), "watch-delete-dir.db")
	ctx := context.Background()
	eng, err := core.New(ctx, core.Config{DBPath: dbPath})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer eng.Close()

	if _, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: dir}); err != nil {
		t.Fatalf("IndexPath: %v", err)
	}
	assertSearchHits(t, eng, ctx, "auto watch deleted directory content", 1)

	if err := os.RemoveAll(nested); err != nil {
		t.Fatalf("RemoveAll(%s): %v", nested, err)
	}
	time.Sleep(2500 * time.Millisecond)

	assertSearchTotal(t, eng, ctx, "auto watch deleted directory content", 0)
}

// TestWatchPersistence verifies that watched paths survive an engine restart.
func TestWatchPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "persist.db")

	writeFile(t, filepath.Join(dir, "first.txt"), "persistence test content")

	ctx := context.Background()

	// First engine instance: index and watch.
	eng1, err := core.New(ctx, core.Config{DBPath: dbPath})
	if err != nil {
		t.Fatalf("engine1.New: %v", err)
	}
	if _, err := eng1.IndexPath(ctx, model.IndexPathRequest{Path: dir}); err != nil {
		t.Fatalf("IndexPath: %v", err)
	}
	eng1.Close()

	// Add a file while engine is offline.
	time.Sleep(100 * time.Millisecond)
	writeFile(t, filepath.Join(dir, "second.txt"), "added while engine was offline")

	// Second engine instance: should re-sync watched paths on startup.
	eng2, err := core.New(ctx, core.Config{DBPath: dbPath})
	if err != nil {
		t.Fatalf("engine2.New: %v", err)
	}
	defer eng2.Close()

	// Give background resync goroutine time to complete.
	time.Sleep(3 * time.Second)

	assertSearchHits(t, eng2, ctx, "added while engine was offline", 1)
	t.Log("Watch persistence: offline file indexed on engine restart ✓")
}

func TestConcurrentIndexPathDoesNotBusy(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 20; i++ {
		writeFile(t, filepath.Join(dir, "file-"+string(rune('a'+i))+".txt"), "concurrent indexing content")
	}

	dbPath := filepath.Join(t.TempDir(), "concurrent.db")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := core.New(ctx, core.Config{DBPath: dbPath})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer eng.Close()

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: dir})
			errs <- err
		}()
	}

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "locked") ||
				strings.Contains(strings.ToLower(err.Error()), "busy") {
				t.Fatalf("concurrent IndexPath returned SQLite lock error: %v", err)
			}
			t.Fatalf("concurrent IndexPath: %v", err)
		}
	}

	assertSearchHits(t, eng, ctx, "concurrent indexing content", 1)
}

func TestSearchFolderAndFilename(t *testing.T) {
	root := t.TempDir()
	folderPath := filepath.Join(root, "Design Docs")
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	nameOnlyFile := filepath.Join(root, "Budget 2026.xlsx")
	writeFile(t, nameOnlyFile, "")

	emptyTextFile := filepath.Join(root, "todo-list.txt")
	writeFile(t, emptyTextFile, "")

	dbPath := filepath.Join(t.TempDir(), "name-search.db")
	ctx := context.Background()

	eng, err := core.New(ctx, core.Config{DBPath: dbPath})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer eng.Close()

	if _, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: root}); err != nil {
		t.Fatalf("IndexPath: %v", err)
	}

	folderSearch, err := eng.Search(ctx, model.SearchRequest{Query: "Design Docs", Limit: 20})
	if err != nil {
		t.Fatalf("Search folder: %v", err)
	}
	folderHit := false
	for _, result := range folderSearch.Results {
		if strings.EqualFold(filepath.Clean(result.Path), filepath.Clean(folderPath)) {
			folderHit = true
			if result.Source != "file" {
				t.Fatalf("folder source = %q, want file", result.Source)
			}
			if result.FileType != "folder" {
				t.Fatalf("folder file_type = %q, want folder", result.FileType)
			}
			break
		}
	}
	if !folderHit {
		t.Fatalf("expected folder result for %q", folderPath)
	}

	fileSearch, err := eng.Search(ctx, model.SearchRequest{Query: "Budget 2026", Limit: 20})
	if err != nil {
		t.Fatalf("Search file name: %v", err)
	}
	fileHit := false
	for _, result := range fileSearch.Results {
		if strings.EqualFold(filepath.Clean(result.Path), filepath.Clean(nameOnlyFile)) {
			fileHit = true
			if result.Source != "file" {
				t.Fatalf("file source = %q, want file", result.Source)
			}
			if result.FileType == "" {
				t.Fatalf("file file_type should not be empty")
			}
			break
		}
	}
	if !fileHit {
		t.Fatalf("expected filename result for %q", nameOnlyFile)
	}

	emptyTextSearch, err := eng.Search(ctx, model.SearchRequest{Query: "todo-list", Limit: 20})
	if err != nil {
		t.Fatalf("Search empty text file name: %v", err)
	}
	emptyHit := false
	for _, result := range emptyTextSearch.Results {
		if strings.EqualFold(filepath.Clean(result.Path), filepath.Clean(emptyTextFile)) {
			emptyHit = true
			if result.Source != "file" {
				t.Fatalf("empty text file source = %q, want file", result.Source)
			}
			break
		}
	}
	if !emptyHit {
		t.Fatalf("expected filename result for %q", emptyTextFile)
	}
}

// helpers

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeFile(%s): %v", path, err)
	}
}

func assertSearchHits(t *testing.T, eng *core.Engine, ctx context.Context, query string, minHits int) {
	t.Helper()
	res, err := eng.Search(ctx, model.SearchRequest{Query: query, Limit: 10})
	if err != nil {
		t.Fatalf("Search(%q): %v", query, err)
	}
	if res.Total < minHits {
		t.Errorf("Search(%q): expected >=%d results, got %d", query, minHits, res.Total)
	}
}

func assertSearchTotal(t *testing.T, eng *core.Engine, ctx context.Context, query string, want int) {
	t.Helper()
	res, err := eng.Search(ctx, model.SearchRequest{Query: query, Limit: 10})
	if err != nil {
		t.Fatalf("Search(%q): %v", query, err)
	}
	if res.Total != want {
		t.Errorf("Search(%q): expected %d results, got %d", query, want, res.Total)
	}
}
