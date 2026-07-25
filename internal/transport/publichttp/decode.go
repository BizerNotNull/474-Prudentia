package publichttp

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type Limits struct {
	MaxBodyBytes        int64
	MaxMessages         int
	MaxMessageBytes     int
	MaxCompletionTokens uint32
	MaxOutputBytes      int
	MaxStreamEvents     int
}

func DefaultLimits() Limits {
	return Limits{
		MaxBodyBytes:        2 << 20,
		MaxMessages:         128,
		MaxMessageBytes:     1 << 20,
		MaxCompletionTokens: 65536,
		MaxOutputBytes:      8 << 20,
		MaxStreamEvents:     131072,
	}
}

type chatRequest struct {
	Model               string        `json:"model"`
	Messages            []chatMessage `json:"messages"`
	Stream              bool          `json:"stream"`
	MaxCompletionTokens *uint32       `json:"max_completion_tokens"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func DecodeChat(r *http.Request, limits Limits) (domain.InferenceRequest, domain.ResponseMode, error) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	if limits.MaxBodyBytes <= 0 || limits.MaxMessages <= 0 || limits.MaxMessageBytes <= 0 || limits.MaxCompletionTokens == 0 {
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
	maxTokens := uint32(1024)
	if input.MaxCompletionTokens != nil {
		maxTokens = *input.MaxCompletionTokens
	}
	if maxTokens == 0 || maxTokens > limits.MaxCompletionTokens {
		return domain.InferenceRequest{}, 0, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	request, err := domain.NewInferenceRequest(domain.InferenceRequestParams{
		Model: input.Model, Messages: messages, MaxCompletionTokens: maxTokens,
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
