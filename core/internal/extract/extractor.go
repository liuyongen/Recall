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

// Extractor 从本地文件提取 UTF-8 文本和元数据。
type Extractor interface {
	Extract(context.Context, string) (string, map[string]any, error)
	Supports(string) bool
}

// Config 控制本地文件提取限制。
type Config struct {
	MaxBytes int64
	TikaURL  string
}

// DefaultMaxBytes 是索引文件大小的默认上限。
const DefaultMaxBytes int64 = 100 * 1024 * 1024

var textExtensions = map[string]struct{}{
	// Web / 前端
	".astro": {}, ".css": {}, ".ejs": {}, ".elm": {}, ".graphql": {},
	".gql": {}, ".htm": {}, ".html": {}, ".js": {}, ".jsx": {},
	".less": {}, ".mdx": {}, ".pug": {}, ".sass": {}, ".scss": {},
	".svelte": {}, ".ts": {}, ".tsx": {}, ".vue": {},
	// 系统 / 编译型
	".c": {}, ".cc": {}, ".cpp": {}, ".cxx": {},
	".h": {}, ".hpp": {}, ".hxx": {},
	".cs": {}, ".fs": {}, ".fsx": {},
	".go": {}, ".java": {}, ".kt": {}, ".kts": {},
	".nim": {}, ".rs": {}, ".swift": {}, ".zig": {},
	// 脚本 / 解释型
	".bash": {}, ".dart": {}, ".ex": {}, ".exs": {},
	".jl": {}, ".lua": {}, ".php": {}, ".ps1": {},
	".py": {}, ".r": {}, ".rb": {}, ".sh": {},
	// JVM / 函数式
	".clj": {}, ".cljs": {}, ".scala": {},
	// 数据 / 配置 / 标记
	".cfg": {}, ".conf": {}, ".csv": {}, ".env": {},
	".ini": {}, ".json": {}, ".md": {}, ".proto": {},
	".sol": {}, ".sql": {}, ".tf": {}, ".toml": {},
	".txt": {}, ".xml": {}, ".yaml": {}, ".yml": {},
	// 模式 / 查询
	".prisma": {},
}

var tikaExtensions = map[string]struct{}{
	".docx": {}, ".pdf": {}, ".pptx": {}, ".rtf": {}, ".xlsx": {},
}

// SupportsIndexedPath 判断文件扩展名是否在索引白名单中。
func SupportsIndexedPath(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	if _, ok := textExtensions[ext]; ok {
		return true
	}
	_, ok := tikaExtensions[ext]
	return ok
}

// SupportsPlainText 判断文件是否可以直接按文本流读取。
func SupportsPlainText(path string) bool {
	_, ok := textExtensions[strings.ToLower(filepath.Ext(path))]
	return ok
}

// Default 返回一个提取器：直接读取纯文本，并在配置后使用本地 Tika。
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

// PlainTextExtractor 无需外部服务即可读取常见文本和代码格式。
type PlainTextExtractor struct {
	MaxBytes int64
}

// Supports 判断文件扩展名是否属于可直接读取的文本格式。
func (e *PlainTextExtractor) Supports(path string) bool {
	return SupportsPlainText(path)
}

// Extract 在配置的大小限制内将近似 UTF-8 的文件读入内存。
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

// OpenPlainText 打开受支持的文本文件，用于流式提取。
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

// TikaExtractor 调用用户自行管理的本地 Apache Tika 服务处理富文档格式。
type TikaExtractor struct {
	Client   *tika.Client
	Fallback Extractor
	MaxBytes int64
}

// Supports 判断扩展名能否由直接文本读取或本地 Tika 处理。
func (e *TikaExtractor) Supports(path string) bool {
	if e.Fallback.Supports(path) {
		return true
	}
	_, ok := tikaExtensions[strings.ToLower(filepath.Ext(path))]
	return ok
}

// Extract 通过本地 Tika 服务解析富文档，并直接读取文本文件。
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

// fileMetadata 返回提取文件的规范化元数据。
func fileMetadata(path string, info os.FileInfo) map[string]any {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
	return map[string]any{
		"path":      path,
		"file_type": ext,
		"size":      info.Size(),
		"modified":  info.ModTime().Unix(),
	}
}

// readTextWithContext 分块读取文本，避免同时持有 []byte 和 string 副本。
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
