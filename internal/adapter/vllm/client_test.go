package vllm

import (
	"context"
	"strings"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type eventSink struct{ events []domain.StreamEvent }

func (s *eventSink) Write(_ context.Context, event domain.StreamEvent) error {
	s.events = append(s.events, event)
	return nil
}

func TestDecodeStreamRequiresDoneAndPreservesEvents(t *testing.T) {
	client := &Client{maxEventBytes: 4096, maxEvents: 8}
	sink := &eventSink{}
	stream := "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\n" +
		"data: [DONE]\n\n"
	if err := client.decodeStream(context.Background(), strings.NewReader(stream), sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 3 {
		t.Fatalf("event count = %d, want 3", len(sink.events))
	}
	if sink.events[0].Kind() != domain.StreamEventDelta || sink.events[0].Delta() != "hello" {
		t.Fatalf("unexpected delta event")
	}
	if sink.events[1].Kind() != domain.StreamEventUsage || sink.events[1].Usage().TotalTokens != 3 {
		t.Fatalf("unexpected usage event")
	}
	if sink.events[2].Kind() != domain.StreamEventTerminal {
		t.Fatalf("missing terminal event")
	}
}

func TestDecodeStreamRejectsBareEOF(t *testing.T) {
	client := &Client{maxEventBytes: 4096, maxEvents: 8}
	err := client.decodeStream(context.Background(), strings.NewReader("data: {\"choices\":[]}\n\n"), &eventSink{})
	if err == nil {
		t.Fatal("bare EOF was accepted as terminal proof")
	}
}
