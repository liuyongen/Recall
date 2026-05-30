package core_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"recall/core/internal/core"
	"recall/core/internal/model"
)

const benchmarkSmallFileCount = 20000

func BenchmarkIndexPathSmallTextFiles(b *testing.B) {
	root := filepath.Join(b.TempDir(), "corpus")
	if err := os.MkdirAll(root, 0o755); err != nil {
		b.Fatalf("mkdir corpus: %v", err)
	}
	for i := 0; i < benchmarkSmallFileCount; i++ {
		name := filepath.Join(root, fmt.Sprintf("doc-%05d.txt", i))
		content := fmt.Sprintf("recall bulk index benchmark file %05d\nfast local search needs sustained write throughput\n", i)
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			b.Fatalf("write corpus file: %v", err)
		}
	}

	ctx := context.Background()
	var totalIndexed int
	var totalElapsed time.Duration
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dbPath := filepath.Join(b.TempDir(), fmt.Sprintf("bench-%d.db", i))
		eng, err := core.New(ctx, core.Config{DBPath: dbPath, MaxBytes: 1024 * 1024})
		if err != nil {
			b.Fatalf("engine.New: %v", err)
		}

		b.StartTimer()
		start := time.Now()
		summary, err := eng.IndexPath(ctx, model.IndexPathRequest{Path: root})
		elapsed := time.Since(start)
		b.StopTimer()

		if closeErr := eng.Close(); closeErr != nil {
			b.Fatalf("engine.Close: %v", closeErr)
		}
		if err != nil {
			b.Fatalf("IndexPath: %v", err)
		}
		if summary.Indexed < benchmarkSmallFileCount {
			b.Fatalf("indexed %d files, want at least %d", summary.Indexed, benchmarkSmallFileCount)
		}
		totalIndexed += summary.Indexed
		totalElapsed += elapsed
	}

	if totalElapsed > 0 {
		b.ReportMetric(float64(totalIndexed)/totalElapsed.Seconds(), "files/s")
	}
}
