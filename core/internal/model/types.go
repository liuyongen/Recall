package model

import (
	"context"
	"io"
)

// DataAdapter 是所有本地数据源实现的通用接口。
type DataAdapter interface {
	ID() string
	Name() string
	IsAvailable() bool
	StartSync() error
	StopSync() error
	GetIncrementalData(lastSyncTime int64) ([]DataItem, error)
}

// ContentSource 打开条目的原始文本流。它必须可重复读取，因为 SQLite 忙重试可能会重放索引事务。
type ContentSource func(context.Context) (io.ReadCloser, error)

// DataItem 是适配器传给索引器的标准化文档结构。
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

// Chunk 是从 DataItem 派生出的可搜索文本块。
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
	// CJKGrams 非空时，是该分块预先计算好的、以空格分隔的 CJK 二元词串。
	// 如果为空，存储层会在写入时生成。放在工作线程里预计算，可以把 CPU 工作移出写入器的临界区。
	CJKGrams string
}

// SearchRequest 包含全文搜索的过滤条件。
type SearchRequest struct {
	Query    string `json:"query"`
	Source   string `json:"source,omitempty"`
	FileType string `json:"file_type,omitempty"`
	Since    int64  `json:"since,omitempty"`
	Until    int64  `json:"until,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	Offset   int    `json:"offset,omitempty"`
}

// SearchResult 是返回给 Electron 的一条排序结果。
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

// SearchResponse 包含查询结果和耗时元数据。
type SearchResponse struct {
	Query     string         `json:"query"`
	ElapsedMS float64        `json:"elapsed_ms"`
	Total     int            `json:"total"`
	Results   []SearchResult `json:"results"`
	HasMore   bool           `json:"has_more,omitempty"`
}

// IndexPathRequest 请求核心索引本地文件或目录。
type IndexPathRequest struct {
	Path     string `json:"path"`
	MaxBytes int64  `json:"max_bytes,omitempty"`
}

// SyncSummary 汇报观察到和已索引的条目数量。
type SyncSummary struct {
	AdapterID string  `json:"adapter_id"`
	Scanned   int     `json:"scanned"`
	Indexed   int     `json:"indexed"`
	Skipped   int     `json:"skipped"`
	ElapsedMS float64 `json:"elapsed_ms"`
}

// IndexProgress 是当前索引任务的实时快照。
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
