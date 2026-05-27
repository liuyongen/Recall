package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"phantasm/core/internal/core"
	"phantasm/core/internal/model"
	"phantasm/core/internal/protocol"
)

// main starts the stdin/stdout JSON-line core server.
func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	engine, err := core.New(ctx, core.DefaultConfig())
	if err != nil {
		logger.Fatalf("create engine: %v", err)
	}
	defer engine.Close()

	server := protocol.NewServer(os.Stdin, os.Stdout, logger)
	registerHandlers(server, engine)

	if err := server.Serve(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Printf("server stopped: %v", err)
	}
}

// registerHandlers maps JSON-RPC-style methods to core engine operations.
func registerHandlers(server *protocol.Server, engine *core.Engine) {
	server.Handle("health", func(ctx context.Context, raw json.RawMessage) (any, error) {
		return engine.Health(ctx)
	})

	server.Handle("search", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var request model.SearchRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		return engine.Search(ctx, request)
	})
	server.Handle("cancel_search", func(ctx context.Context, raw json.RawMessage) (any, error) {
		return engine.CancelSearch(ctx)
	})

	server.Handle("index_path", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var request model.IndexPathRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		return engine.IndexPath(ctx, request)
	})

	server.Handle("index_progress", func(ctx context.Context, raw json.RawMessage) (any, error) {
		return engine.IndexProgress(ctx)
	})

	server.Handle("cancel_index", func(ctx context.Context, raw json.RawMessage) (any, error) {
		return engine.CancelIndexPath(ctx)
	})

	server.Handle("sync_browsers", func(ctx context.Context, raw json.RawMessage) (any, error) {
		return engine.SyncBrowsers(ctx)
	})
	server.Handle("cancel_sync_browsers", func(ctx context.Context, raw json.RawMessage) (any, error) {
		return engine.CancelSyncBrowsers(ctx)
	})

	server.Handle("optimize", func(ctx context.Context, raw json.RawMessage) (any, error) {
		return engine.Optimize(ctx)
	})
}
