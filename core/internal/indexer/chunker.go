package indexer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"recall/core/internal/model"
)

const (
	defaultChunkSize    = 1024
	defaultChunkOverlap = 100
)

// Chunker 按自然文本边界切分文档。
type Chunker struct {
	Size    int
	Overlap int
}

// NewChunker 返回使用默认尺寸的生产分块器。
func NewChunker() *Chunker {
	return &Chunker{Size: defaultChunkSize, Overlap: defaultChunkOverlap}
}

// Split 将一个 DataItem 转换为带哈希的可搜索分块。
func (c *Chunker) Split(item model.DataItem) []model.Chunk {
	text := strings.TrimSpace(item.Content)
	if text == "" {
		return nil
	}

	runes := []rune(text)
	chunks := make([]model.Chunk, 0, len(runes)/c.Size+1)
	for start, ordinal := 0, 0; start < len(runes); ordinal++ {
		end := c.chooseEnd(runes, start)
		content := strings.TrimSpace(string(runes[start:end]))
		if content != "" {
			chunks = append(chunks, c.makeChunk(item, ordinal, content))
		}
		if end >= len(runes) {
			break
		}
		start = max(0, end-c.Overlap)
	}
	return chunks
}

// chooseEnd 在目标大小附近寻找最佳分块边界。
func (c *Chunker) chooseEnd(runes []rune, start int) int {
	target := min(start+c.Size, len(runes))
	if target >= len(runes) {
		return len(runes)
	}

	floor := max(start+c.Size/2, target-220)
	for i := target; i > floor; i-- {
		if isBoundary(runes[i-1]) {
			return i
		}
	}
	return target
}

func (c *Chunker) chooseEndBytes(text []byte, start int) int {
	target := min(start+c.Size, len(text))
	if target >= len(text) {
		return len(text)
	}

	floor := max(start+c.Size/2, target-220)
	for i := target; i > floor; i-- {
		if isBoundaryByte(text[i-1]) {
			return i
		}
	}
	return target
}

// makeChunk 构造带稳定元数据的可搜索分块。
func (c *Chunker) makeChunk(item model.DataItem, ordinal int, content string) model.Chunk {
	hash := hashText(content)
	pathValue, _ := item.Metadata["path"].(string)
	fileType, _ := item.Metadata["file_type"].(string)
	return model.Chunk{
		ChunkID:   fmt.Sprintf("%s:%s:%04d:%s", item.Source, item.ID, ordinal, hash[:16]),
		ItemID:    item.ID,
		Source:    item.Source,
		Title:     item.Title,
		Content:   content,
		Preview:   previewText(content, 220),
		Ordinal:   ordinal,
		Hash:      hash,
		Path:      pathValue,
		FileType:  fileType,
		Metadata:  item.Metadata,
		CreatedAt: item.CreatedAt,
		UpdatedAt: item.UpdatedAt,
	}
}

// hashText 返回用于分块差异比较的 SHA-256 哈希。
func hashText(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// isBoundary 判断一个 rune 是否是自然切分点。
func isBoundary(r rune) bool {
	switch r {
	case '\n', '.', '!', '?', ';', ':', '。', '！', '？', '；', '：':
		return true
	default:
		return false
	}
}

func isBoundaryByte(b byte) bool {
	switch b {
	case '\n', '.', '!', '?', ';', ':':
		return true
	default:
		return false
	}
}

// previewText 将文本截断为适合 UI 展示的预览。
func previewText(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	runes := []rune(text)
	return strings.TrimSpace(string(runes[:limit])) + "..."
}
