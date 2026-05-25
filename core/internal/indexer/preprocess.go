package indexer

import (
	"regexp"
	"strings"
	"sync"
	"unicode"
)

var tagPattern = regexp.MustCompile(`<[^>]+>`)

// Preprocessor normalizes extracted text before indexing.
type Preprocessor struct {
	builderPool sync.Pool
}

// NewPreprocessor creates a reusable text preprocessor.
func NewPreprocessor() *Preprocessor {
	return &Preprocessor{
		builderPool: sync.Pool{New: func() any { return new(strings.Builder) }},
	}
}

// Clean removes markup, control characters, and repeated whitespace.
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

// normalizeRune removes control characters and standardizes whitespace.
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
