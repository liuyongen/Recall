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

// Chunker splits documents on natural text boundaries.
type Chunker struct {
	Size    int
	Overlap int
}

// NewChunker returns a production chunker with default sizing.
func NewChunker() *Chunker {
	return &Chunker{Size: defaultChunkSize, Overlap: defaultChunkOverlap}
}

// Split converts one DataItem into hashed searchable chunks.
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

// chooseEnd finds the best chunk boundary near the target size.
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

// makeChunk constructs a searchable chunk with stable metadata.
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

// hashText returns a SHA-256 hash for chunk diffing.
func hashText(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// isBoundary reports whether a rune is a natural split point.
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

// previewText truncates text to a UI-friendly preview.
func previewText(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	runes := []rune(text)
	return strings.TrimSpace(string(runes[:limit])) + "..."
}
