package model

import (
	"context"
	"io"
)

// DataAdapter is the common interface implemented by every local data source.
type DataAdapter interface {
	ID() string
	Name() string
	IsAvailable() bool
	StartSync() error
	StopSync() error
	GetIncrementalData(lastSyncTime int64) ([]DataItem, error)
}

// ContentSource opens an item's raw text stream. It must be repeatable because
// SQLite busy retries may replay an indexing transaction.
type ContentSource func(context.Context) (io.ReadCloser, error)

// DataItem is the normalized document shape passed from adapters to the indexer.
type DataItem struct {
	ID            string         `json:"id"`
	Source        string         `json:"source"`
	Title         string         `json:"title"`
	Content       string         `json:"content,omitempty"`
	ContentSource ContentSource  `json:"-"`
	Preview       string         `json:"preview"`
	Metadata      map[string]any `json:"metadata"`
	CreatedAt     int64          `json:"created_at"`
	UpdatedAt     int64          `json:"updated_at"`
}

// Chunk is a searchable text block derived from a DataItem.
type Chunk struct {
	RowID     int64
	ChunkID   string
	ItemID    string
	Source    string
	Title     string
	Content   string
	Preview   string
	Ordinal   int
	Hash      string
	Path      string
	FileType  string
	Metadata  map[string]any
	CreatedAt int64
	UpdatedAt int64
	// CJKGrams, when non-empty, is the pre-computed space-separated CJK bigram
	// string for this chunk. If empty, the store will generate it at write
	// time. Pre-computing in worker threads moves CPU work out of the writer's
	// critical section.
	CJKGrams string
}

// SearchRequest contains filters for a full-text search.
type SearchRequest struct {
	Query    string `json:"query"`
	Source   string `json:"source,omitempty"`
	FileType string `json:"file_type,omitempty"`
	Since    int64  `json:"since,omitempty"`
	Until    int64  `json:"until,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	Offset   int    `json:"offset,omitempty"`
}

// SearchResult is one ranked result returned to Electron.
type SearchResult struct {
	RowID     int64          `json:"rowid"`
	ItemID    string         `json:"item_id"`
	Source    string         `json:"source"`
	Title     string         `json:"title"`
	Preview   string         `json:"preview"`
	Path      string         `json:"path,omitempty"`
	FileType  string         `json:"file_type,omitempty"`
	UpdatedAt int64          `json:"updated_at"`
	Score     float64        `json:"score"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// SearchResponse contains results and timing metadata for a query.
type SearchResponse struct {
	Query     string         `json:"query"`
	ElapsedMS float64        `json:"elapsed_ms"`
	Total     int            `json:"total"`
	Results   []SearchResult `json:"results"`
	HasMore   bool           `json:"has_more,omitempty"`
}

// IndexPathRequest asks the core to index a local file or directory.
type IndexPathRequest struct {
	Path     string `json:"path"`
	MaxBytes int64  `json:"max_bytes,omitempty"`
}

// SyncSummary reports how many items were observed and indexed.
type SyncSummary struct {
	AdapterID string  `json:"adapter_id"`
	Scanned   int     `json:"scanned"`
	Indexed   int     `json:"indexed"`
	Skipped   int     `json:"skipped"`
	ElapsedMS float64 `json:"elapsed_ms"`
}

// IndexProgress is a live snapshot of the current indexing task.
type IndexProgress struct {
	Active        bool    `json:"active"`
	Kind          string  `json:"kind,omitempty"`
	Phase         string  `json:"phase"`
	Path          string  `json:"path,omitempty"`
	Current       string  `json:"current,omitempty"`
	Total         int     `json:"total"`
	Scanned       int     `json:"scanned"`
	Indexed       int     `json:"indexed"`
	Skipped       int     `json:"skipped"`
	Written       int     `json:"written"`
	Workers       int     `json:"workers"`
	FilesPerSec   float64 `json:"files_per_sec"`
	EtaMS         float64 `json:"eta_ms"`
	StartedAt     int64   `json:"started_at,omitempty"`
	UpdatedAt     int64   `json:"updated_at,omitempty"`
	ElapsedMS     float64 `json:"elapsed_ms"`
	LastError     string  `json:"last_error,omitempty"`
	LastCompleted int64   `json:"last_completed,omitempty"`
}
