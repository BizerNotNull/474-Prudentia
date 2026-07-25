package publichttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type StreamSink interface {
	Write(context.Context, domain.StreamEvent) error
}

type SSESink struct {
	writer   http.ResponseWriter
	flusher  http.Flusher
	id       string
	started  bool
	terminal bool
}

func NewSSESink(w http.ResponseWriter, id string) (*SSESink, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, domain.NewPublicError(domain.ErrorInternal)
	}
	return &SSESink{writer: w, flusher: flusher, id: id}, nil
}

func (s *SSESink) Started() bool { return s.started }

func (s *SSESink) Write(ctx context.Context, event domain.StreamEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.terminal {
		return errors.New("stream already terminated")
	}
	var payload any
	switch event.Kind() {
	case domain.StreamEventDelta:
		payload = map[string]any{
			"id":      s.id,
			"object":  "chat.completion.chunk",
			"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": event.Delta()}, "finish_reason": nil}},
		}
	case domain.StreamEventUsage:
		payload = map[string]any{
			"id":      s.id,
			"object":  "chat.completion.chunk",
			"choices": []any{},
			"usage":   event.Usage(),
		}
	case domain.StreamEventTerminal:
		payload = map[string]any{
			"id":      s.id,
			"object":  "chat.completion.chunk",
			"choices": []any{map[string]any{"index": 0, "delta": map[string]string{}, "finish_reason": "stop"}},
		}
		s.terminal = true
	default:
		return errors.New("unknown stream event")
	}

	if !s.started {
		s.writer.Header().Set("Content-Type", "text/event-stream")
		s.writer.Header().Set("Cache-Control", "no-cache")
		s.writer.Header().Set("X-Request-Id", s.id)
		s.writer.WriteHeader(http.StatusOK)
		s.started = true
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.writer, "data: %s\n\n", encoded); err != nil {
		return err
	}
	if s.terminal {
		if _, err := fmt.Fprint(s.writer, "data: [DONE]\n\n"); err != nil {
			return err
		}
	}
	s.flusher.Flush()
	return nil
}

type NonStreamingCollector struct {
	maxBytes  int
	maxEvents int
	content   strings.Builder
	usage     domain.Usage
	events    int
	terminal  bool
}

func NewNonStreamingCollector(maxBytes, maxEvents int) *NonStreamingCollector {
	return &NonStreamingCollector{maxBytes: maxBytes, maxEvents: maxEvents}
}

func (c *NonStreamingCollector) Write(ctx context.Context, event domain.StreamEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.terminal || c.events >= c.maxEvents {
		return domain.NewPublicError(domain.ErrorUnavailable)
	}
	c.events++
	switch event.Kind() {
	case domain.StreamEventDelta:
		if len(event.Delta()) > c.maxBytes-c.content.Len() {
			return domain.NewPublicError(domain.ErrorUnavailable)
		}
		_, _ = c.content.WriteString(event.Delta())
	case domain.StreamEventUsage:
		c.usage = event.Usage()
	case domain.StreamEventTerminal:
		c.terminal = true
	default:
		return domain.NewPublicError(domain.ErrorInternal)
	}
	return nil
}

type CompletionResult struct {
	Content string
	Usage   domain.Usage
}

func (c *NonStreamingCollector) Result() (CompletionResult, error) {
	if !c.terminal {
		return CompletionResult{}, domain.NewPublicError(domain.ErrorUnavailable)
	}
	return CompletionResult{Content: c.content.String(), Usage: c.usage}, nil
}

func EncodeNonStreaming(w http.ResponseWriter, id, model string, result CompletionResult) error {
	response := map[string]any{
		"id":     id,
		"object": "chat.completion",
		"model":  model,
		"choices": []any{map[string]any{
			"index":         0,
			"message":       map[string]string{"role": "assistant", "content": result.Content},
			"finish_reason": "stop",
		}},
		"usage": result.Usage,
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", id)
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(response)
}

func WritePublicError(w http.ResponseWriter, id string, err error) {
	kind := domain.ErrorKindOf(err)
	status, code, message := http.StatusInternalServerError, "internal_error", "The request could not be completed."
	switch kind {
	case domain.ErrorInvalidRequest:
		status, code, message = http.StatusBadRequest, "invalid_request", "The request is invalid."
	case domain.ErrorUnauthenticated:
		status, code, message = http.StatusUnauthorized, "unauthenticated", "Authentication is required."
		w.Header().Set("WWW-Authenticate", "Bearer")
	case domain.ErrorForbidden:
		status, code, message = http.StatusForbidden, "forbidden", "The request is not permitted."
	case domain.ErrorUnavailable:
		status, code, message = http.StatusServiceUnavailable, "service_unavailable", "Inference is temporarily unavailable."
	}
	w.Header().Set("Content-Type", "application/json")
	if id != "" {
		w.Header().Set("X-Request-Id", id)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message, "type": code, "code": code}})
}
