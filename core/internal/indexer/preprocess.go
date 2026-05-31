package indexer

import (
	"regexp"
	"strings"
	"sync"
	"unicode"
)

var tagPattern = regexp.MustCompile(`<[^>]+>`)

// Preprocessor 在索引前规范化提取出的文本。
type Preprocessor struct {
	builderPool sync.Pool
}

// NewPreprocessor 创建可复用的文本预处理器。
func NewPreprocessor() *Preprocessor {
	return &Preprocessor{
		builderPool: sync.Pool{New: func() any { return new(strings.Builder) }},
	}
}

// Clean 移除标记、控制字符和重复空白。
func (p *Preprocessor) Clean(input string) string {
	if input == "" {
		return ""
	}

	input = tagPattern.ReplaceAllString(input, " ")
	builder := p.builderPool.Get().(*strings.Builder)
	builder.Reset()
	defer p.builderPool.Put(builder)

	lastSpace := false
	for _, r := range input {
		r = normalizeRune(r)
		if r == 0 {
			continue
		}
		if unicode.IsSpace(r) {
			if !lastSpace {
				builder.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		builder.WriteRune(r)
		lastSpace = false
	}

	return strings.TrimSpace(builder.String())
}

// normalizeRune 移除控制字符并统一空白字符。
func normalizeRune(r rune) rune {
	switch {
	case r == '\n' || r == '\r' || r == '\t':
		return ' '
	case unicode.IsControl(r):
		return 0
	default:
		return r
	}
}
