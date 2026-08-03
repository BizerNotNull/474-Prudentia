package publichttp

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type Limits struct {
	MaxBodyBytes        int64
	MaxMessages         int
	MaxMessageBytes     int
	MaxCompletionTokens uint32
	MaxOutputBytes      int
	MaxStreamEvents     int
	DefaultOutputTokens uint32
	DefaultBudget       time.Duration
}

func DefaultLimits() Limits {
	return Limits{
		MaxBodyBytes:        2 << 20,
		MaxMessages:         128,
		MaxMessageBytes:     1 << 20,
		MaxCompletionTokens: 65536,
		MaxOutputBytes:      8 << 20,
		MaxStreamEvents:     131072,
		DefaultOutputTokens: 1024,
		DefaultBudget:       2 * time.Minute,
	}
}

type chatRequest struct {
	Model               string        `json:"model"`
	Messages            []chatMessage `json:"messages"`
	Stream              bool          `json:"stream"`
	MaxCompletionTokens *uint32       `json:"max_completion_tokens"`
	Priority            string        `json:"priority,omitempty"`
	CachePolicy         string        `json:"cache_policy,omitempty"`
	ExecutionBudgetMS   *uint32       `json:"execution_budget_ms,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ResponseMode = domain.ResponseMode

func DecodeChat(r *http.Request, limits Limits) (domain.InferenceRequest, ResponseMode, error) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	if limits.MaxBodyBytes <= 0 || limits.MaxMessages <= 0 || limits.MaxMessageBytes <= 0 || limits.MaxCompletionTokens == 0 {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInternal)
	}
	defaultTokens := limits.DefaultOutputTokens
	if defaultTokens == 0 {
		defaultTokens = 1024
		if defaultTokens > limits.MaxCompletionTokens {
			defaultTokens = limits.MaxCompletionTokens
		}
	}
	defaultBudget := limits.DefaultBudget
	if defaultBudget == 0 {
		defaultBudget = 2 * time.Minute
	}
	if defaultTokens > limits.MaxCompletionTokens || defaultBudget <= 0 || defaultBudget > 30*time.Minute {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInternal)
	}

	limited := io.LimitReader(r.Body, limits.MaxBodyBytes+1)
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var input chatRequest
	if err := decoder.Decode(&input); err != nil {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	if err := ensureEOF(decoder); err != nil {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	if len(input.Messages) == 0 || len(input.Messages) > limits.MaxMessages {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}

	messages := make([]domain.MessageParams, len(input.Messages))
	for i, message := range input.Messages {
		if len(message.Content) > limits.MaxMessageBytes {
			return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
		}
		messages[i] = domain.MessageParams{Role: message.Role, Content: message.Content}
	}
	maxTokens := defaultTokens
	if input.MaxCompletionTokens != nil {
		maxTokens = *input.MaxCompletionTokens
	}
	if maxTokens == 0 || maxTokens > limits.MaxCompletionTokens {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	priority := domain.PriorityNormal
	switch input.Priority {
	case "", "normal":
	case "background":
		priority = domain.PriorityBackground
	case "high":
		priority = domain.PriorityHigh
	default:
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	cachePolicy := domain.CachePolicyDisabled
	switch input.CachePolicy {
	case "", "disabled":
	case "prefer":
		cachePolicy = domain.CachePolicyPrefer
	case "require_compatible":
		cachePolicy = domain.CachePolicyRequireCompatible
	default:
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	budget := defaultBudget
	if input.ExecutionBudgetMS != nil {
		budget = time.Duration(*input.ExecutionBudgetMS) * time.Millisecond
		if budget <= 0 || budget > 30*time.Minute {
			return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
		}
	}
	var featureBits uint64
	if input.Stream {
		featureBits |= 1 << domain.FeatureStreaming
	}
	features, err := domain.NewFeatureSet(domain.FeatureVersion1, featureBits)
	if err != nil {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInternal)
	}
	request, err := domain.NewInferenceRequest(domain.InferenceRequestParams{
		Model: input.Model, Messages: messages, MaxCompletionTokens: maxTokens,
		Priority: priority, Features: features, CachePolicy: cachePolicy, ExecutionBudget: budget,
	})
	if err != nil {
		return domain.InferenceRequest{}, 0, err
	}
	mode := domain.ResponseModeNonStreaming
	if input.Stream {
		mode = domain.ResponseModeStreaming
	}
	return request, mode, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return errors.New("request must contain one JSON value")
}
