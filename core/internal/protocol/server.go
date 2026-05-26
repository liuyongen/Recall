package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

// Handler processes one request payload and returns a JSON-serializable value.
type Handler func(context.Context, json.RawMessage) (any, error)

// Server reads request lines from stdin and writes response lines to stdout.
type Server struct {
	in       io.Reader
	out      io.Writer
	logger   *log.Logger
	handlers map[string]Handler
	writeMu  sync.Mutex
}

type request struct {
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type response struct {
	Type   string     `json:"type"`
	ID     string     `json:"id"`
	Result any        `json:"result,omitempty"`
	Error  *wireError `json:"error,omitempty"`
}

type wireError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// NewServer constructs a protocol server.
func NewServer(in io.Reader, out io.Writer, logger *log.Logger) *Server {
	return &Server{
		in:       in,
		out:      out,
		logger:   logger,
		handlers: make(map[string]Handler),
	}
}

// Handle registers a method handler.
func (s *Server) Handle(method string, handler Handler) {
	s.handlers[method] = handler
}

// Serve blocks until the input stream closes or the context is canceled.
func (s *Server) Serve(ctx context.Context) error {
	scanner := bufio.NewScanner(s.in)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	var wg sync.WaitGroup
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleLine(ctx, line)
		}()
	}
	wg.Wait()

	if err := scanner.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

// handleLine decodes and dispatches one request line.
func (s *Server) handleLine(parent context.Context, line []byte) {
	var req request
	if err := json.Unmarshal(line, &req); err != nil {
		s.logger.Printf("decode request: %v", err)
		return
	}

	if req.Type != "request" || req.ID == "" || req.Method == "" {
		s.write(req.ID, nil, errors.New("invalid request envelope"))
		return
	}

	handler, ok := s.handlers[req.Method]
	if !ok {
		s.write(req.ID, nil, fmt.Errorf("unknown method: %s", req.Method))
		return
	}

	timeout := timeoutFor(req.Method)
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if timeout <= 0 {
		ctx, cancel = context.WithCancel(parent)
	} else {
		ctx, cancel = context.WithTimeout(parent, timeout)
	}
	defer cancel()

	result, err := handler(ctx, req.Params)
	s.write(req.ID, result, err)
}

// write serializes one response while preserving line ordering.
func (s *Server) write(id string, result any, err error) {
	msg := response{Type: "response", ID: id, Result: result}
	if err != nil {
		msg.Error = &wireError{Code: "core_error", Message: err.Error()}
		msg.Result = nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	encoder := json.NewEncoder(s.out)
	if encodeErr := encoder.Encode(msg); encodeErr != nil {
		s.logger.Printf("encode response: %v", encodeErr)
	}
}

// timeoutFor returns the hard request deadline for a method.
func timeoutFor(method string) time.Duration {
	switch method {
	case "search":
		return 120 * time.Second
	case "index_path":
		return 0
	case "sync_browsers":
		return 0
	default:
		return 5 * time.Second
	}
}
