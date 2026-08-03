package vllm

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func TestBackendSSERequiresDoneAndReturnsProof(t *testing.T) {
	backend := &Backend{config: BackendConfig{MaxEventBytes: 4096, MaxEvents: 8, MaxResponseBytes: 8192}}
	sink := &eventSink{}
	proof, err := backend.decodeSSE(context.Background(), &fragmentReader{parts: []string{"data: {\"choices\":[{\"delta\":{\"content\":\"hel", "lo\"}}]}\n\n", "data: [DONE]\n\n"}}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if proof != domain.TerminalProofProviderFinish || len(sink.events) != 2 || sink.events[0].Delta() != "hello" {
		t.Fatal("stream proof or events incorrect")
	}
	if _, err = backend.decodeSSE(context.Background(), strings.NewReader("data: {\"choices\":[]}\n\n"), &eventSink{}); err == nil {
		t.Fatal("bare EOF accepted")
	}
}

func TestBackendNonstreamRequiresBoundedCompleteJSON(t *testing.T) {
	backend := &Backend{config: BackendConfig{MaxResponseBytes: 256}}
	proof, err := backend.decodeJSON(context.Background(), strings.NewReader(`{"choices":[{"delta":{"content":"ok"}}]}`), &eventSink{})
	if err != nil || proof != domain.TerminalProofCompleteNonStreaming {
		t.Fatalf("proof=%v err=%v", proof, err)
	}
	if _, err = backend.decodeJSON(context.Background(), strings.NewReader(strings.Repeat("x", 257)), &eventSink{}); err == nil {
		t.Fatal("oversized JSON accepted")
	}
	if _, err = backend.decodeJSON(context.Background(), strings.NewReader(`{"choices":[]}{}`), &eventSink{}); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}

type fragmentReader struct{ parts []string }

func (r *fragmentReader) Read(p []byte) (int, error) {
	if len(r.parts) == 0 {
		return 0, io.EOF
	}
	part := r.parts[0]
	r.parts = r.parts[1:]
	return copy(p, part), nil
}
