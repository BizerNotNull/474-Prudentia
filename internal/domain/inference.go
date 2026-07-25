package domain

import (
	"errors"
	"fmt"
	"strings"
)

type ResponseMode uint8

const (
	ResponseModeNonStreaming ResponseMode = iota + 1
	ResponseModeStreaming
)

type ErrorKind uint8

const (
	ErrorInvalidRequest ErrorKind = iota + 1
	ErrorUnauthenticated
	ErrorForbidden
	ErrorUnavailable
	ErrorInternal
)

type PublicError struct {
	kind ErrorKind
}

func (e *PublicError) Error() string   { return "request failed" }
func (e *PublicError) Kind() ErrorKind { return e.kind }

func NewPublicError(kind ErrorKind) error { return &PublicError{kind: kind} }

func ErrorKindOf(err error) ErrorKind {
	var publicErr *PublicError
	if errors.As(err, &publicErr) {
		return publicErr.Kind()
	}
	return ErrorInternal
}

type MessageParams struct {
	Role    string
	Content string
}

type Message struct {
	role    string
	content string
}

func (m Message) Role() string    { return m.role }
func (m Message) Content() string { return m.content }

type InferenceRequestParams struct {
	Model               string
	Messages            []MessageParams
	MaxCompletionTokens uint32
}

type InferenceRequest struct {
	model               string
	messages            []Message
	maxCompletionTokens uint32
}

func NewInferenceRequest(p InferenceRequestParams) (InferenceRequest, error) {
	model := strings.TrimSpace(p.Model)
	if model == "" || len(model) > 256 {
		return InferenceRequest{}, NewPublicError(ErrorInvalidRequest)
	}
	if len(p.Messages) == 0 || len(p.Messages) > 128 {
		return InferenceRequest{}, NewPublicError(ErrorInvalidRequest)
	}
	if p.MaxCompletionTokens == 0 || p.MaxCompletionTokens > 65536 {
		return InferenceRequest{}, NewPublicError(ErrorInvalidRequest)
	}

	messages := make([]Message, len(p.Messages))
	for i, input := range p.Messages {
		if input.Role != "system" && input.Role != "user" && input.Role != "assistant" {
			return InferenceRequest{}, NewPublicError(ErrorInvalidRequest)
		}
		if input.Content == "" || len(input.Content) > 1<<20 {
			return InferenceRequest{}, NewPublicError(ErrorInvalidRequest)
		}
		messages[i] = Message{role: input.Role, content: input.Content}
	}

	return InferenceRequest{
		model:               model,
		messages:            messages,
		maxCompletionTokens: p.MaxCompletionTokens,
	}, nil
}

func (r InferenceRequest) Model() string               { return r.model }
func (r InferenceRequest) MaxCompletionTokens() uint32 { return r.maxCompletionTokens }
func (r InferenceRequest) Messages() []Message         { return append([]Message(nil), r.messages...) }

type Principal struct {
	tenant string
	models map[string]struct{}
}

func NewPrincipal(tenant string, models []string) (Principal, error) {
	tenant = strings.TrimSpace(tenant)
	if tenant == "" || len(tenant) > 128 || len(models) == 0 || len(models) > 128 {
		return Principal{}, fmt.Errorf("invalid principal")
	}
	allowed := make(map[string]struct{}, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" || len(model) > 256 {
			return Principal{}, fmt.Errorf("invalid principal")
		}
		allowed[model] = struct{}{}
	}
	return Principal{tenant: tenant, models: allowed}, nil
}

func (p Principal) Tenant() string { return p.tenant }
func (p Principal) AllowsModel(model string) bool {
	_, ok := p.models[model]
	return ok
}

type AuthorizedRequest struct {
	principal Principal
	request   InferenceRequest
}

func NewAuthorizedRequest(p Principal, r InferenceRequest) AuthorizedRequest {
	return AuthorizedRequest{principal: p, request: r}
}

func (r AuthorizedRequest) Tenant() string            { return r.principal.Tenant() }
func (r AuthorizedRequest) Request() InferenceRequest { return r.request }

type Usage struct {
	PromptTokens     uint32 `json:"prompt_tokens"`
	CompletionTokens uint32 `json:"completion_tokens"`
	TotalTokens      uint32 `json:"total_tokens"`
}

type StreamEventKind uint8

const (
	StreamEventDelta StreamEventKind = iota + 1
	StreamEventUsage
	StreamEventTerminal
)

type StreamEvent struct {
	kind  StreamEventKind
	delta string
	usage Usage
}

func NewDeltaEvent(delta string) (StreamEvent, error) {
	if delta == "" {
		return StreamEvent{}, fmt.Errorf("empty delta")
	}
	return StreamEvent{kind: StreamEventDelta, delta: delta}, nil
}

func NewUsageEvent(usage Usage) (StreamEvent, error) {
	if usage.TotalTokens != usage.PromptTokens+usage.CompletionTokens {
		return StreamEvent{}, fmt.Errorf("invalid usage")
	}
	return StreamEvent{kind: StreamEventUsage, usage: usage}, nil
}

func NewTerminalEvent() StreamEvent         { return StreamEvent{kind: StreamEventTerminal} }
func (e StreamEvent) Kind() StreamEventKind { return e.kind }
func (e StreamEvent) Delta() string         { return e.delta }
func (e StreamEvent) Usage() Usage          { return e.usage }
