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

// Handler 处理一个请求载荷，并返回可 JSON 序列化的值。
type Handler func(context.Context, json.RawMessage) (any, error)

// Server 从 stdin 读取请求行，并向 stdout 写入响应行。
type Server struct {
	in       io.Reader
	out      io.Writer
	logger   *log.Logger
	handlers map[string]Handler
	writeMu  sync.Mutex
	encoder  *json.Encoder
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

// NewServer 构造一个协议服务器。
func NewServer(in io.Reader, out io.Writer, logger *log.Logger) *Server {
	return &Server{
		in:       in,
		out:      out,
		logger:   logger,
		handlers: make(map[string]Handler),
		encoder:  json.NewEncoder(out),
	}
}

// Handle 注册一个方法处理器。
func (s *Server) Handle(method string, handler Handler) {
	s.handlers[method] = handler
}

// Serve 会阻塞，直到输入流关闭或上下文被取消。
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

// handleLine 解码并分发一行请求。
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

// write 序列化一条响应，同时保持行顺序。
func (s *Server) write(id string, result any, err error) {
	msg := response{Type: "response", ID: id, Result: result}
	if err != nil {
		msg.Error = &wireError{Code: "core_error", Message: err.Error()}
		msg.Result = nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if encodeErr := s.encoder.Encode(msg); encodeErr != nil {
		s.logger.Printf("encode response: %v", encodeErr)
	}
}

// timeoutFor 返回某个方法的硬性请求截止时间。
func timeoutFor(method string) time.Duration {
	switch method {
	case "search":
		return 120 * time.Second
	case "index_path":
		return 0
	case "sync_browsers":
		return 0
	case "index_progress":
		return 0
	default:
		return 5 * time.Second
	}
}
