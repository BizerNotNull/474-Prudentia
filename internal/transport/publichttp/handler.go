package publichttp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type Authenticator interface {
	Authenticate(context.Context, *http.Request) (domain.Principal, error)
}

type Authorizer interface {
	Authorize(context.Context, domain.Principal, domain.InferenceRequest) (domain.AuthorizedRequest, error)
}

type Inferer interface {
	Infer(context.Context, string, []byte, domain.AuthorizedRequest, domain.ResponseMode, StreamSink) error
}

type Handler struct {
	authenticator Authenticator
	authorizer    Authorizer
	inferer       Inferer
	limits        Limits
}

func NewHandler(authenticator Authenticator, authorizer Authorizer, inferer Inferer, limits Limits) *Handler {
	return &Handler{authenticator: authenticator, authorizer: authorizer, inferer: inferer, limits: limits}
}

func (h *Handler) Routes(health http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", h.ChatCompletions)
	mux.Handle("/livez", health)
	mux.Handle("/readyz", health)
	mux.Handle("/startupz", health)
	return mux
}

func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	id, err := newRequestID()
	if err != nil {
		WritePublicError(w, "", domain.NewPublicError(domain.ErrorInternal))
		return
	}
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, h.limits.MaxBodyBytes)

	principal, err := h.authenticator.Authenticate(r.Context(), r)
	if err != nil {
		WritePublicError(w, id, err)
		return
	}
	request, mode, err := DecodeChat(r, h.limits)
	if err != nil {
		WritePublicError(w, id, err)
		return
	}
	authorized, err := h.authorizer.Authorize(r.Context(), principal, request)
	if err != nil {
		WritePublicError(w, id, err)
		return
	}
	idempotencyKey, err := takeIdempotencyKey(r)
	if err != nil {
		WritePublicError(w, id, err)
		return
	}
	defer clear(idempotencyKey)

	if mode == domain.ResponseModeStreaming {
		h.serveStreaming(w, r, id, idempotencyKey, authorized)
		return
	}
	h.serveNonStreaming(w, r, id, idempotencyKey, authorized)
}

func (h *Handler) serveStreaming(w http.ResponseWriter, r *http.Request, id string, idempotencyKey []byte, request domain.AuthorizedRequest) {
	sink, err := NewSSESink(w, id, request.Request().Model())
	if err != nil {
		WritePublicError(w, id, err)
		return
	}
	err = h.inferer.Infer(r.Context(), id, idempotencyKey, request, domain.ResponseModeStreaming, filterUsage(sink, request.Request().Features()))
	if err != nil && !sink.Started() {
		WritePublicError(w, id, err)
	}
}

func (h *Handler) serveNonStreaming(w http.ResponseWriter, r *http.Request, id string, idempotencyKey []byte, request domain.AuthorizedRequest) {
	collector := NewNonStreamingCollector(h.limits.MaxOutputBytes, h.limits.MaxStreamEvents)
	if err := h.inferer.Infer(r.Context(), id, idempotencyKey, request, domain.ResponseModeNonStreaming, filterUsage(collector, request.Request().Features())); err != nil {
		WritePublicError(w, id, err)
		return
	}
	result, err := collector.Result()
	if err != nil {
		WritePublicError(w, id, err)
		return
	}
	if err := EncodeNonStreaming(w, id, request.Request().Model(), result); err != nil {
		return
	}
}

type usageFilter struct {
	next  StreamSink
	allow bool
}

func filterUsage(next StreamSink, features domain.FeatureSet) StreamSink {
	return usageFilter{next: next, allow: features.Has(domain.FeatureUsage)}
}

func (s usageFilter) Write(ctx context.Context, event domain.StreamEvent) error {
	if event.Kind() == domain.StreamEventUsage && !s.allow {
		return nil
	}
	return s.next.Write(ctx, event)
}

func takeIdempotencyKey(r *http.Request) ([]byte, error) {
	values := r.Header.Values("Idempotency-Key")
	r.Header.Del("Idempotency-Key")
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) != 1 || len(values[0]) == 0 || len(values[0]) > 256 {
		return nil, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	key := []byte(values[0])
	for _, value := range key {
		if value < 0x21 || value > 0x7e {
			clear(key)
			return nil, domain.NewPublicError(domain.ErrorInvalidRequest)
		}
	}
	return key, nil
}

func newRequestID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "req_" + hex.EncodeToString(raw[:]), nil
}
