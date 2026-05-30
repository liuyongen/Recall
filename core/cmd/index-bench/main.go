package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"recall/core/internal/core"
	"recall/core/internal/model"
)

func main() {
	root := flag.String("root", `D:\`, "root path to index")
	dbPath := flag.String("db", "", "database path; empty uses a temporary DB")
	maxBytes := flag.Int64("max-bytes", 0, "maximum file size to extract; 0 uses engine default")
	sampleEvery := flag.Duration("sample", 5*time.Second, "progress sample interval")
	keepDB := flag.Bool("keep-db", false, "keep the temporary benchmark database")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	path := *dbPath
	if path == "" {
		path = filepath.Join(os.TempDir(), fmt.Sprintf("recall-index-bench-%d.db", time.Now().UnixNano()))
	}
	if !*keepDB && *dbPath == "" {
		defer removeDBFiles(path)
	}

	fmt.Printf("index-bench root=%s db=%s sample=%s\n", *root, path, sampleEvery.String())

	eng, err := core.New(ctx, core.Config{DBPath: path, MaxBytes: *maxBytes})
	if err != nil {
		fatalf("engine.New: %v", err)
	}
	defer eng.Close()

	done := make(chan struct{})
	go sampleProgress(ctx, eng, *sampleEvery, done)

	start := time.Now()
	summary, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: *root, MaxBytes: *maxBytes})
	close(done)
	elapsed := time.Since(start)
	if err != nil {
		fatalf("IndexPath: %v", err)
	}

	avgIndexed := 0.0
	if elapsed > 0 {
		avgIndexed = float64(summary.Indexed) / elapsed.Seconds()
	}
	fmt.Printf("done elapsed=%s scanned=%d indexed=%d skipped=%d avg_indexed_files_s=%.1f\n",
		elapsed.Round(time.Millisecond), summary.Scanned, summary.Indexed, summary.Skipped, avgIndexed)
}

func sampleProgress(ctx context.Context, eng *core.Engine, every time.Duration, done <-chan struct{}) {
	if every <= 0 {
		return
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	var last model.IndexProgress
	var lastAt time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			printProgress(ctx, eng, last, lastAt)
			return
		case now := <-ticker.C:
			last = printProgress(ctx, eng, last, lastAt)
			lastAt = now
		}
	}
}

func printProgress(ctx context.Context, eng *core.Engine, last model.IndexProgress, lastAt time.Time) model.IndexProgress {
	p, err := eng.IndexProgress(ctx)
	if err != nil {
		fmt.Printf("sample error=%v\n", err)
		return last
	}
	now := time.Now()
	window := now.Sub(lastAt).Seconds()
	if lastAt.IsZero() || window <= 0 {
		window = 0
	}

	scannedRate := 0.0
	indexedRate := 0.0
	writtenRate := 0.0
	if window > 0 {
		scannedRate = float64(p.Scanned-last.Scanned) / window
		indexedRate = float64(p.Indexed-last.Indexed) / window
		writtenRate = float64(p.Written-last.Written) / window
	}

	fmt.Printf("sample elapsed=%.1fs phase=%s total=%d scanned=%d indexed=%d skipped=%d written=%d scan_s=%.1f index_s=%.1f write_s=%.1f avg_scan_s=%.1f current=%q\n",
		p.ElapsedMS/1000, p.Phase, p.Total, p.Scanned, p.Indexed, p.Skipped, p.Written,
		scannedRate, indexedRate, writtenRate, p.FilesPerSec, p.Current)
	return p
}

func removeDBFiles(path string) {
	_ = os.Remove(path)
	_ = os.Remove(path + "-shm")
	_ = os.Remove(path + "-wal")
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	stem := base[:len(base)-len(ext)]
	dir := filepath.Dir(path)
	matches, _ := filepath.Glob(filepath.Join(dir, stem+".shard-*.db*"))
	for _, match := range matches {
		_ = os.Remove(match)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
