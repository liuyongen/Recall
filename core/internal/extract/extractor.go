package extract

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/google/go-tika/tika"
)

// Extractor extracts UTF-8 text and metadata from a local file.
type Extractor interface {
	Extract(context.Context, string) (string, map[string]any, error)
	Supports(string) bool
}

// Config controls local file extraction limits.
type Config struct {
	MaxBytes int64
	TikaURL  string
}

// DefaultMaxBytes is the default file size ceiling for indexing.
const DefaultMaxBytes int64 = 100 * 1024 * 1024

var textExtensions = map[string]struct{}{
	".c": {}, ".cpp": {}, ".cs": {}, ".css": {}, ".csv": {},
	".go": {}, ".h": {}, ".html": {}, ".java": {}, ".js": {},
	".json": {}, ".jsx": {}, ".md": {}, ".py": {}, ".sql": {},
	".ts": {}, ".tsx": {}, ".txt": {}, ".yaml": {}, ".yml": {},
}

var tikaExtensions = map[string]struct{}{
	".docx": {}, ".pdf": {}, ".pptx": {}, ".rtf": {}, ".xlsx": {},
}

// SupportsPlainText reports whether a file can be streamed as text directly.
func SupportsPlainText(path string) bool {
	_, ok := textExtensions[strings.ToLower(filepath.Ext(path))]
	return ok
}

// Default returns an extractor that reads plain text directly and uses local Tika when configured.
func Default(cfg Config) Extractor {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	plain := &PlainTextExtractor{MaxBytes: cfg.MaxBytes}
	if cfg.TikaURL == "" {
		return plain
	}
	return &TikaExtractor{
		Client:   tika.NewClient(nil, cfg.TikaURL),
		Fallback: plain,
		MaxBytes: cfg.MaxBytes,
	}
}

// PlainTextExtractor reads common text and code formats without external services.
type PlainTextExtractor struct {
	MaxBytes int64
}

// Supports reports whether the file extension is a direct text format.
func (e *PlainTextExtractor) Supports(path string) bool {
	return SupportsPlainText(path)
}

// Extract reads a UTF-8-ish file into memory within the configured size limit.
func (e *PlainTextExtractor) Extract(ctx context.Context, path string) (string, map[string]any, error) {
	if !e.Supports(path) {
		return "", nil, fmt.Errorf("unsupported text type: %s", filepath.Ext(path))
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", nil, err
	}
	if e.MaxBytes > 0 && info.Size() > e.MaxBytes {
		return "", nil, fmt.Errorf("file exceeds %d bytes", e.MaxBytes)
	}

	file, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()

	text, err := readTextWithContext(ctx, file, info.Size())
	if err != nil {
		return "", nil, err
	}
	return text, fileMetadata(path, info), nil
}

// OpenPlainText opens a supported text file for streaming extraction.
func OpenPlainText(ctx context.Context, path string, maxBytes int64) (io.ReadCloser, map[string]any, os.FileInfo, error) {
	if !SupportsPlainText(path) {
		return nil, nil, nil, fmt.Errorf("unsupported text type: %s", filepath.Ext(path))
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, nil, err
	}
	if maxBytes > 0 && info.Size() > maxBytes {
		return nil, nil, nil, fmt.Errorf("file exceeds %d bytes", maxBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return nil, nil, nil, err
	}
	return file, fileMetadata(path, info), info, nil
}

// TikaExtractor calls a user-managed local Apache Tika server for rich document formats.
type TikaExtractor struct {
	Client   *tika.Client
	Fallback Extractor
	MaxBytes int64
}

// Supports reports whether direct text or local Tika can handle the extension.
func (e *TikaExtractor) Supports(path string) bool {
	if e.Fallback.Supports(path) {
		return true
	}
	_, ok := tikaExtensions[strings.ToLower(filepath.Ext(path))]
	return ok
}

// Extract parses rich documents through a local Tika server and text files directly.
func (e *TikaExtractor) Extract(ctx context.Context, path string) (string, map[string]any, error) {
	if e.Fallback.Supports(path) {
		return e.Fallback.Extract(ctx, path)
	}
	if !e.Supports(path) {
		return "", nil, fmt.Errorf("unsupported document type: %s", filepath.Ext(path))
	}

	info, err := os.Stat(path)
	if err != nil {
		return "", nil, err
	}
	if e.MaxBytes > 0 && info.Size() > e.MaxBytes {
		return "", nil, fmt.Errorf("file exceeds %d bytes", e.MaxBytes)
	}

	file, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()

	text, err := e.Client.Parse(ctx, file)
	if err != nil {
		return "", nil, err
	}
	return text, fileMetadata(path, info), nil
}

// fileMetadata returns normalized metadata for an extracted file.
func fileMetadata(path string, info os.FileInfo) map[string]any {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
	return map[string]any{
		"path":      path,
		"file_type": ext,
		"size":      info.Size(),
		"modified":  info.ModTime().Unix(),
	}
}

// readTextWithContext reads text in chunks to avoid holding both []byte and string copies.
func readTextWithContext(ctx context.Context, reader io.Reader, size int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var builder strings.Builder
	if size > 0 && size < int64(int(^uint(0)>>1)) {
		builder.Grow(int(size))
	}

	buf := make([]byte, 64*1024)
	var pending []byte
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := reader.Read(buf)
		if n > 0 {
			data := append(pending, buf[:n]...)
			cut := validUTF8Prefix(data)
			if cut > 0 {
				builder.WriteString(string(data[:cut]))
				pending = append(pending[:0], data[cut:]...)
			} else {
				builder.WriteString(strings.ToValidUTF8(string(data), ""))
				pending = pending[:0]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	if len(pending) > 0 {
		builder.WriteString(strings.ToValidUTF8(string(pending), ""))
	}
	return builder.String(), ctx.Err()
}

func validUTF8Prefix(data []byte) int {
	if utf8.Valid(data) {
		return len(data)
	}
	for tail := 1; tail <= min(utf8.UTFMax, len(data)); tail++ {
		cut := len(data) - tail
		if utf8.Valid(data[:cut]) {
			return cut
		}
	}
	return 0
}
