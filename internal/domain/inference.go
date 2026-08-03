package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxRequestIDBytes   = 128
	MaxAttemptIDBytes   = 128
	MaxTenantScopeBytes = 128
	MaxSecretBytes      = 512
	MaxModelKeyBytes    = 256
	MaxOutputDeltaBytes = 1 << 20
)

type RequestID struct{ value string }
type AttemptID struct{ value string }
type TenantScope struct{ value string }
type ModelKey struct{ value string }

type SecretString struct{ value []byte }

func NewRequestID(value string) (RequestID, error) {
	if !boundedValue(value, MaxRequestIDBytes) { return RequestID{}, fmt.Errorf("invalid request id") }
	return RequestID{value: value}, nil
}
func NewAttemptID(value string) (AttemptID, error) {
	if !boundedValue(value, MaxAttemptIDBytes) { return AttemptID{}, fmt.Errorf("invalid attempt id") }
	return AttemptID{value: value}, nil
}
func NewTenantScope(value string) (TenantScope, error) {
	if !boundedValue(value, MaxTenantScopeBytes) { return TenantScope{}, fmt.Errorf("invalid tenant scope") }
	return TenantScope{value: value}, nil
}
func NewModelKey(value string) (ModelKey, error) {
	if !boundedValue(value, MaxModelKeyBytes) { return ModelKey{}, fmt.Errorf("invalid model key") }
	return ModelKey{value: value}, nil
}
func NewSecretString(value []byte) (SecretString, error) {
	if len(value) == 0 || len(value) > MaxSecretBytes { return SecretString{}, fmt.Errorf("invalid secret") }
	return SecretString{value: append([]byte(nil), value...)}, nil
}
func NewSecretStringFromString(value string) (SecretString, error) { return NewSecretString([]byte(value)) }
func (v RequestID) String() string   { return v.value }
func (v RequestID) Value() string    { return v.value }
func (v AttemptID) String() string   { return v.value }
func (v AttemptID) Value() string    { return v.value }
func (v TenantScope) String() string { return v.value }
func (v TenantScope) Value() string  { return v.value }
func (v ModelKey) String() string    { return v.value }
func (v ModelKey) Value() string     { return v.value }
func (v SecretString) String() string { return "[redacted]" }
func (v SecretString) GoString() string { return "SecretString([redacted])" }
func (v SecretString) Bytes() []byte { return append([]byte(nil), v.value...) }
func (v SecretString) IsZero() bool { return len(v.value) == 0 }

func boundedValue(value string, max int) bool {
	return value != "" && len(value) <= max && value == strings.TrimSpace(value) && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

type FeatureVersion uint16
const FeatureVersion1 FeatureVersion = 1

type Feature uint8
const (
	FeatureStreaming Feature = iota + 1
	FeatureUsage
	FeaturePrefixCache
	FeatureToolCalling
)
const knownFeatureBits uint64 = (1 << FeatureStreaming) | (1 << FeatureUsage) | (1 << FeaturePrefixCache) | (1 << FeatureToolCalling)

type FeatureSet struct { version FeatureVersion; bits uint64 }
func NewFeatureSet(version FeatureVersion, bits uint64) (FeatureSet, error) {
	if version != FeatureVersion1 || bits&^knownFeatureBits != 0 { return FeatureSet{}, fmt.Errorf("invalid feature set") }
	return FeatureSet{version: version, bits: bits}, nil
}
func EmptyFeatureSet() FeatureSet { return FeatureSet{version: FeatureVersion1} }
func (s FeatureSet) Version() FeatureVersion { return s.version }
func (s FeatureSet) Bits() uint64 { return s.bits }
func (s FeatureSet) Has(feature Feature) bool {
	return s.version == FeatureVersion1 && feature > 0 && feature <= FeatureToolCalling && s.bits&(1<<feature) != 0
}
func (s FeatureSet) Valid() bool { return s.version == FeatureVersion1 && s.bits&^knownFeatureBits == 0 }
func (s FeatureSet) Contains(required FeatureSet) bool { return s.Valid() && required.Valid() && s.version == required.version && s.bits&required.bits == required.bits }

type Priority uint8
const (
	PriorityBackground Priority = iota + 1
	PriorityNormal
	PriorityHigh
)
func validPriority(v Priority) bool { return v >= PriorityBackground && v <= PriorityHigh }

type CachePolicy uint8
const (
	CachePolicyDisabled CachePolicy = iota + 1
	CachePolicyPrefer
	CachePolicyRequireCompatible
)
func validCachePolicy(v CachePolicy) bool { return v >= CachePolicyDisabled && v <= CachePolicyRequireCompatible }

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
	ErrorIdempotencyConflict
	ErrorRequestInProgress
	ErrorRequestNotReplayable
	ErrorUnavailable
	ErrorInternal
)
type PublicError struct{ kind ErrorKind }
func (e *PublicError) Error() string { return "request failed" }
func (e *PublicError) Kind() ErrorKind { return e.kind }
func NewPublicError(kind ErrorKind) error {
	if kind < ErrorInvalidRequest || kind > ErrorInternal { kind = ErrorInternal }
	return &PublicError{kind: kind}
}
func ErrorKindOf(err error) ErrorKind {
	var publicErr *PublicError
	if errors.As(err, &publicErr) { return publicErr.Kind() }
	return ErrorInternal
}

type MessageParams struct { Role, Content string }
type Message struct { role, content string }
func (m Message) Role() string { return m.role }
func (m Message) Content() string { return m.content }

type Input struct { messages []Message; extensions map[string]string }
type InputParams struct { Messages []MessageParams; Extensions map[string]string }
func NewInput(p InputParams) (Input, error) {
	if len(p.Messages) == 0 || len(p.Messages) > 128 || len(p.Extensions) > 32 { return Input{}, fmt.Errorf("invalid input") }
	messages := make([]Message, len(p.Messages))
	for i, m := range p.Messages {
		if m.Role != "system" && m.Role != "user" && m.Role != "assistant" { return Input{}, fmt.Errorf("invalid input") }
		if m.Content == "" || len(m.Content) > 1<<20 || !utf8.ValidString(m.Content) { return Input{}, fmt.Errorf("invalid input") }
		messages[i] = Message{role: m.Role, content: m.Content}
	}
	extensions := make(map[string]string, len(p.Extensions))
	for k, v := range p.Extensions {
		if !boundedValue(k, 64) || len(v) > 4096 || !utf8.ValidString(v) { return Input{}, fmt.Errorf("invalid input") }
		extensions[k] = v
	}
	return Input{messages: messages, extensions: extensions}, nil
}
func (i Input) Clone() Input {
	return Input{messages: append([]Message(nil), i.messages...), extensions: cloneStringMap(i.extensions)}
}
func (i Input) Messages() []Message { return append([]Message(nil), i.messages...) }
func (i Input) Extensions() map[string]string { return cloneStringMap(i.extensions) }
func cloneStringMap(source map[string]string) map[string]string {
	if source == nil { return nil }
	result := make(map[string]string, len(source)); for k, v := range source { result[k] = v }; return result
}

type InferenceRequestParams struct {
	RequestID RequestID
	Model string
	ModelKey ModelKey
	Messages []MessageParams
	Input Input
	MaxCompletionTokens uint32
	MaxOutputTokens uint32
	Priority Priority
	Features FeatureSet
	CachePolicy CachePolicy
	ExecutionBudget time.Duration
	IdempotencyKey SecretString
}
type InferenceRequest struct {
	requestID RequestID; model ModelKey; input Input; maxOutputTokens uint32
	priority Priority; features FeatureSet; cachePolicy CachePolicy; executionBudget time.Duration; idempotencyKey SecretString
}
func NewInferenceRequest(p InferenceRequestParams) (InferenceRequest, error) {
	model := p.ModelKey
	if model.value == "" { var err error; model, err = NewModelKey(strings.TrimSpace(p.Model)); if err != nil { return InferenceRequest{}, NewPublicError(ErrorInvalidRequest) } }
	input := p.Input
	if len(input.messages) == 0 { var err error; input, err = NewInput(InputParams{Messages: p.Messages}); if err != nil { return InferenceRequest{}, NewPublicError(ErrorInvalidRequest) } } else { input = input.Clone() }
	maxTokens := p.MaxOutputTokens; if maxTokens == 0 { maxTokens = p.MaxCompletionTokens }
	if maxTokens == 0 || maxTokens > 65536 { return InferenceRequest{}, NewPublicError(ErrorInvalidRequest) }
	priority := p.Priority
	features := p.Features
	cachePolicy := p.CachePolicy
	budget := p.ExecutionBudget
	if !validPriority(priority) || !features.Valid() || !validCachePolicy(cachePolicy) || budget <= 0 || budget > 30*time.Minute { return InferenceRequest{}, NewPublicError(ErrorInvalidRequest) }
	return InferenceRequest{requestID: p.RequestID, model: model, input: input, maxOutputTokens: maxTokens, priority: priority, features: features, cachePolicy: cachePolicy, executionBudget: budget, idempotencyKey: SecretString{value: p.IdempotencyKey.Bytes()}}, nil
}
func (r InferenceRequest) RequestID() (RequestID, bool) { return r.requestID, r.requestID.value != "" }
func (r InferenceRequest) Model() string { return r.model.value }
func (r InferenceRequest) ModelKey() ModelKey { return r.model }
func (r InferenceRequest) Input() Input { return r.input.Clone() }
func (r InferenceRequest) Messages() []Message { return r.input.Messages() }
func (r InferenceRequest) MaxCompletionTokens() uint32 { return r.maxOutputTokens }
func (r InferenceRequest) MaxOutputTokens() uint32 { return r.maxOutputTokens }
func (r InferenceRequest) Priority() Priority { return r.priority }
func (r InferenceRequest) Features() FeatureSet { return r.features }
func (r InferenceRequest) CachePolicy() CachePolicy { return r.cachePolicy }
func (r InferenceRequest) ExecutionBudget() time.Duration { return r.executionBudget }
func (r InferenceRequest) IdempotencyKey() (SecretString, bool) { return SecretString{value: r.idempotencyKey.Bytes()}, !r.idempotencyKey.IsZero() }

type PrincipalParams struct {
	Subject string; Tenant TenantScope; Models []ModelKey; Features FeatureSet
	MaxPriority Priority; CachePolicies []CachePolicy
}
type Principal struct {
	subject string; tenant TenantScope; models map[ModelKey]struct{}; features FeatureSet
	maxPriority Priority; cachePolicies uint8
}
func NewPrincipal(tenant string, models []string) (Principal, error) {
	t, err := NewTenantScope(strings.TrimSpace(tenant)); if err != nil { return Principal{}, fmt.Errorf("invalid principal") }
	keys := make([]ModelKey, len(models)); for i, model := range models { keys[i], err = NewModelKey(strings.TrimSpace(model)); if err != nil { return Principal{}, fmt.Errorf("invalid principal") } }
	return NewPrincipalFromParams(PrincipalParams{Subject: tenant, Tenant: t, Models: keys, Features: EmptyFeatureSet(), MaxPriority: PriorityNormal, CachePolicies: []CachePolicy{CachePolicyDisabled}})
}
func NewPrincipalFromParams(p PrincipalParams) (Principal, error) {
	if !boundedValue(p.Subject, 256) || p.Tenant.value == "" || len(p.Models) == 0 || len(p.Models) > 128 || !p.Features.Valid() || !validPriority(p.MaxPriority) || len(p.CachePolicies) == 0 { return Principal{}, fmt.Errorf("invalid principal") }
	models := make(map[ModelKey]struct{}, len(p.Models)); for _, model := range p.Models { if model.value == "" { return Principal{}, fmt.Errorf("invalid principal") }; models[model] = struct{}{} }
	var policies uint8; for _, policy := range p.CachePolicies { if !validCachePolicy(policy) { return Principal{}, fmt.Errorf("invalid principal") }; policies |= 1 << policy }
	return Principal{subject: p.Subject, tenant: p.Tenant, models: models, features: p.Features, maxPriority: p.MaxPriority, cachePolicies: policies}, nil
}
func (p Principal) Subject() string { return p.subject }
func (p Principal) Tenant() string { return p.tenant.value }
func (p Principal) TenantScope() TenantScope { return p.tenant }
func (p Principal) AllowsModel(model string) bool { key, err := NewModelKey(model); if err != nil { return false }; _, ok := p.models[key]; return ok }
func (p Principal) Allows(req InferenceRequest) bool {
	_, model := p.models[req.model]
	return model && p.features.Contains(req.features) && req.priority <= p.maxPriority && p.cachePolicies&(1<<req.cachePolicy) != 0
}

type AuthorizedRequest struct { principal Principal; request InferenceRequest }
func NewAuthorizedRequest(p Principal, r InferenceRequest) AuthorizedRequest { return AuthorizedRequest{principal: p, request: r} }
func NewCheckedAuthorizedRequest(p Principal, r InferenceRequest) (AuthorizedRequest, error) {
	if !p.Allows(r) { return AuthorizedRequest{}, NewPublicError(ErrorForbidden) }
	return AuthorizedRequest{principal: p, request: r}, nil
}
func (r AuthorizedRequest) Tenant() string { return r.principal.Tenant() }
func (r AuthorizedRequest) TenantScope() TenantScope { return r.principal.TenantScope() }
func (r AuthorizedRequest) Principal() Principal { return r.principal }
func (r AuthorizedRequest) Request() InferenceRequest { return r.request }

type Usage struct { PromptTokens uint32 `json:"prompt_tokens"`; CompletionTokens uint32 `json:"completion_tokens"`; TotalTokens uint32 `json:"total_tokens"` }
func (u Usage) Valid() bool { return uint64(u.PromptTokens)+uint64(u.CompletionTokens) == uint64(u.TotalTokens) }

type OutputDelta struct { data []byte }
func NewOutputDelta(data []byte) (OutputDelta, error) {
	if len(data) == 0 || len(data) > MaxOutputDeltaBytes || !utf8.Valid(data) { return OutputDelta{}, fmt.Errorf("invalid output delta") }
	return OutputDelta{data: append([]byte(nil), data...)}, nil
}
func (d OutputDelta) Bytes() []byte { return append([]byte(nil), d.data...) }
func (d OutputDelta) String() string { return string(d.data) }
func (d OutputDelta) Clone() OutputDelta { return OutputDelta{data: d.Bytes()} }

type StreamEventKind uint8
const (
	StreamEventDelta StreamEventKind = iota + 1
	StreamEventUsage
	StreamEventTerminal
)
type TerminalProof uint8
const (
	TerminalProofProviderFinish TerminalProof = iota + 1
	TerminalProofCompleteNonStreaming
	TerminalProofNotSent
	TerminalProofAuthenticatedProviderTermination
)
func (p TerminalProof) Valid() bool { return p >= TerminalProofProviderFinish && p <= TerminalProofAuthenticatedProviderTermination }

type StreamEventParams struct { Kind StreamEventKind; Delta OutputDelta; Usage Usage; HasUsage bool; TerminalProof TerminalProof }
type StreamEvent struct { kind StreamEventKind; delta OutputDelta; usage Usage; hasUsage bool; terminalProof TerminalProof }
func NewStreamEvent(p StreamEventParams) (StreamEvent, error) {
	switch p.Kind {
	case StreamEventDelta:
		if len(p.Delta.data) == 0 || p.HasUsage || p.TerminalProof != 0 { return StreamEvent{}, fmt.Errorf("invalid stream event") }
		return StreamEvent{kind: p.Kind, delta: p.Delta.Clone()}, nil
	case StreamEventUsage:
		if len(p.Delta.data) != 0 || !p.HasUsage || !p.Usage.Valid() || p.TerminalProof != 0 { return StreamEvent{}, fmt.Errorf("invalid stream event") }
		return StreamEvent{kind: p.Kind, usage: p.Usage, hasUsage: true}, nil
	case StreamEventTerminal:
		if len(p.Delta.data) != 0 || p.HasUsage || !p.TerminalProof.Valid() { return StreamEvent{}, fmt.Errorf("invalid stream event") }
		return StreamEvent{kind: p.Kind, terminalProof: p.TerminalProof}, nil
	default:
		return StreamEvent{}, fmt.Errorf("invalid stream event")
	}
}
func NewDeltaEvent(delta string) (StreamEvent, error) { d, err := NewOutputDelta([]byte(delta)); if err != nil { return StreamEvent{}, err }; return NewStreamEvent(StreamEventParams{Kind: StreamEventDelta, Delta: d}) }
func NewUsageEvent(usage Usage) (StreamEvent, error) { return NewStreamEvent(StreamEventParams{Kind: StreamEventUsage, Usage: usage, HasUsage: true}) }
func NewTerminalEvent() StreamEvent { event, _ := NewStreamEvent(StreamEventParams{Kind: StreamEventTerminal, TerminalProof: TerminalProofProviderFinish}); return event }
func NewTerminalEventWithProof(proof TerminalProof) (StreamEvent, error) { return NewStreamEvent(StreamEventParams{Kind: StreamEventTerminal, TerminalProof: proof}) }
func (e StreamEvent) Kind() StreamEventKind { return e.kind }
func (e StreamEvent) Delta() string { return e.delta.String() }
func (e StreamEvent) OutputDelta() OutputDelta { return e.delta.Clone() }
func (e StreamEvent) Usage() Usage { return e.usage }
func (e StreamEvent) HasUsage() bool { return e.hasUsage }
func (e StreamEvent) TerminalProof() (TerminalProof, bool) { return e.terminalProof, e.kind == StreamEventTerminal && e.terminalProof.Valid() }

type CompletionResultParams struct { Output []byte; Usage Usage; HasUsage bool; TerminalProof TerminalProof }
type CompletionResult struct { output []byte; usage Usage; hasUsage bool; terminalProof TerminalProof }
func NewCompletionResult(p CompletionResultParams) (CompletionResult, error) {
	if len(p.Output) > MaxOutputDeltaBytes || !utf8.Valid(p.Output) || !p.TerminalProof.Valid() || p.TerminalProof == TerminalProofNotSent || (p.HasUsage && !p.Usage.Valid()) || (!p.HasUsage && p.Usage != (Usage{})) { return CompletionResult{}, fmt.Errorf("invalid completion result") }
	return CompletionResult{output: append([]byte(nil), p.Output...), usage: p.Usage, hasUsage: p.HasUsage, terminalProof: p.TerminalProof}, nil
}
func (r CompletionResult) Output() []byte { return append([]byte(nil), r.output...) }
func (r CompletionResult) Content() string { return string(r.output) }
func (r CompletionResult) Usage() (Usage, bool) { return r.usage, r.hasUsage }
func (r CompletionResult) TerminalProof() TerminalProof { return r.terminalProof }
func (r CompletionResult) Complete() bool { return r.terminalProof.Valid() && r.terminalProof != TerminalProofNotSent }
