package indexer

import (
	"bufio"
	"context"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"recall/core/internal/model"
)

// StreamItemChunks 从 reader 清理并切分文本，无需把完整条目内容物化到内存。
// 分块会按序号顺序产出。
func (i *Indexer) StreamItemChunks(
	ctx context.Context,
	item model.DataItem,
	reader io.Reader,
	yield func(model.Chunk) error,
) error {
	stream := &chunkStream{
		ctx:       ctx,
		chunker:   i.chunker,
		item:      item,
		yield:     yield,
		asciiMode: true,
	}
	return stream.run(reader)
}

type chunkStream struct {
	ctx          context.Context
	chunker      *Chunker
	item         model.DataItem
	yield        func(model.Chunk) error
	current      []rune
	asciiCurrent []byte
	asciiMode    bool
	ordinal      int
	lastSpace    bool
	tagBuffer    []rune
}

func (s *chunkStream) run(reader io.Reader) error {
	bufReader := bufio.NewReaderSize(reader, 64*1024)
	buf := make([]byte, 64*1024)
	var pending []byte

	for {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		n, err := bufReader.Read(buf)
		if n > 0 {
			data := append(pending, buf[:n]...)
			cut := validUTF8Prefix(data)
			if cut > 0 {
				if err := s.writeText(string(data[:cut])); err != nil {
					return err
				}
				pending = append(pending[:0], data[cut:]...)
			} else {
				if err := s.writeText(strings.ToValidUTF8(string(data), "")); err != nil {
					return err
				}
				pending = pending[:0]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if len(pending) > 0 {
		if err := s.writeText(strings.ToValidUTF8(string(pending), "")); err != nil {
			return err
		}
	}
	if err := s.flushPendingTag(); err != nil {
		return err
	}
	return s.flushFinal()
}

func (s *chunkStream) writeText(text string) error {
	if s.asciiMode && isASCIIString(text) {
		return s.writeASCIIText(text)
	}
	if s.asciiMode {
		s.current = []rune(string(s.asciiCurrent))
		s.asciiCurrent = nil
		s.asciiMode = false
	}
	for _, r := range text {
		if len(s.tagBuffer) > 0 {
			s.tagBuffer = append(s.tagBuffer, r)
			if r == '>' {
				s.tagBuffer = s.tagBuffer[:0]
				if err := s.writeCleanRune(' '); err != nil {
					return err
				}
				continue
			}
			if len(s.tagBuffer) > 4096 {
				if err := s.flushPendingTag(); err != nil {
					return err
				}
			}
			continue
		}
		if r == '<' {
			s.tagBuffer = append(s.tagBuffer[:0], r)
			continue
		}
		if err := s.writeCleanRune(r); err != nil {
			return err
		}
	}
	return nil
}

func (s *chunkStream) writeASCIIText(text string) error {
	for i := 0; i < len(text); i++ {
		b := text[i]
		if len(s.tagBuffer) > 0 {
			s.tagBuffer = append(s.tagBuffer, rune(b))
			if b == '>' {
				s.tagBuffer = s.tagBuffer[:0]
				if err := s.writeCleanASCIIByte(' '); err != nil {
					return err
				}
				continue
			}
			if len(s.tagBuffer) > 4096 {
				if err := s.flushPendingTag(); err != nil {
					return err
				}
			}
			continue
		}
		if b == '<' {
			s.tagBuffer = append(s.tagBuffer[:0], rune(b))
			continue
		}
		if err := s.writeCleanASCIIByte(b); err != nil {
			return err
		}
	}
	return nil
}

func (s *chunkStream) flushPendingTag() error {
	for _, r := range s.tagBuffer {
		if s.asciiMode && r < utf8.RuneSelf {
			if err := s.writeCleanASCIIByte(byte(r)); err != nil {
				s.tagBuffer = s.tagBuffer[:0]
				return err
			}
			continue
		}
		if s.asciiMode {
			s.current = []rune(string(s.asciiCurrent))
			s.asciiCurrent = nil
			s.asciiMode = false
		}
		if err := s.writeCleanRune(r); err != nil {
			s.tagBuffer = s.tagBuffer[:0]
			return err
		}
	}
	s.tagBuffer = s.tagBuffer[:0]
	return nil
}

func (s *chunkStream) writeCleanASCIIByte(b byte) error {
	switch b {
	case '\n', '\r', '\t':
		b = ' '
	default:
		if b < 0x20 || b == 0x7f {
			return nil
		}
	}
	if b == ' ' {
		if s.lastSpace {
			return nil
		}
		s.asciiCurrent = append(s.asciiCurrent, ' ')
		s.lastSpace = true
		return s.flushASCIIReady()
	}
	s.asciiCurrent = append(s.asciiCurrent, b)
	s.lastSpace = false
	return s.flushASCIIReady()
}

func (s *chunkStream) writeCleanRune(r rune) error {
	r = normalizeRune(r)
	if r == 0 {
		return nil
	}
	if unicode.IsSpace(r) {
		if s.lastSpace {
			return nil
		}
		s.current = append(s.current, ' ')
		s.lastSpace = true
		return s.flushReady()
	}
	s.current = append(s.current, r)
	s.lastSpace = false
	return s.flushReady()
}

func (s *chunkStream) flushReady() error {
	for len(s.current) > s.chunker.Size {
		if err := s.emitPrefix(); err != nil {
			return err
		}
	}
	return nil
}

func (s *chunkStream) flushASCIIReady() error {
	for len(s.asciiCurrent) > s.chunker.Size {
		if err := s.emitASCIIPrefix(); err != nil {
			return err
		}
	}
	return nil
}

func (s *chunkStream) emitPrefix() error {
	end := s.chunker.chooseEnd(s.current, 0)
	content := strings.TrimSpace(string(s.current[:end]))
	nextStart := max(0, end-s.chunker.Overlap)
	next := append([]rune(nil), s.current[nextStart:]...)
	s.current = next
	if content == "" {
		return nil
	}
	chunk := s.chunker.makeChunk(s.item, s.ordinal, content)
	s.ordinal++
	return s.yield(chunk)
}

func (s *chunkStream) emitASCIIPrefix() error {
	end := s.chunker.chooseEndBytes(s.asciiCurrent, 0)
	content := strings.TrimSpace(string(s.asciiCurrent[:end]))
	nextStart := max(0, end-s.chunker.Overlap)
	next := append([]byte(nil), s.asciiCurrent[nextStart:]...)
	s.asciiCurrent = next
	if content == "" {
		return nil
	}
	chunk := s.chunker.makeChunk(s.item, s.ordinal, content)
	s.ordinal++
	return s.yield(chunk)
}

func (s *chunkStream) flushFinal() error {
	if s.asciiMode {
		content := strings.TrimSpace(string(s.asciiCurrent))
		if content == "" {
			return nil
		}
		chunk := s.chunker.makeChunk(s.item, s.ordinal, content)
		s.ordinal++
		return s.yield(chunk)
	}
	content := strings.TrimSpace(string(s.current))
	if content == "" {
		return nil
	}
	chunk := s.chunker.makeChunk(s.item, s.ordinal, content)
	s.ordinal++
	return s.yield(chunk)
}

func isASCIIString(text string) bool {
	for i := 0; i < len(text); i++ {
		if text[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
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
