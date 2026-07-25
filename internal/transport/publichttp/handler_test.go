package publichttp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/auth"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/health"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

const testToken = "0123456789abcdef0123456789abcdef"

type fakeInferer struct {
	calls int
	err   error
}

func (f *fakeInferer) Infer(ctx context.Context, _ domain.AuthorizedRequest, _ domain.ResponseMode, sink publichttp.StreamSink) error {
	f.calls++
	if f.err != nil {
		return f.err
	}
	delta, _ := domain.NewDeltaEvent("hello")
	usage, _ := domain.NewUsageEvent(domain.Usage{PromptTokens: 2, CompletionTokens: 1, TotalTokens: 3})
	for _, event := range []domain.StreamEvent{delta, usage, domain.NewTerminalEvent()} {
		if err := sink.Write(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func newTestHandler(t *testing.T, inferer publichttp.Inferer) http.Handler {
	t.Helper()
	authenticator, err := auth.NewAuthenticator([]auth.APIKey{{
		Token: testToken, Tenant: "tenant-a", Models: []string{"model-a"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	state := &health.State{}
	state.SetStarted(true)
	state.SetReady(true)
	return publichttp.NewHandler(authenticator, auth.Authorizer{}, inferer, publichttp.DefaultLimits()).Routes(health.NewHandler(state))
}

func chatRequest(t *testing.T, handler http.Handler, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestChatRejectsMissingAuthenticationBeforeInference(t *testing.T) {
	inferer := &fakeInferer{}
	response := chatRequest(t, newTestHandler(t, inferer), "", `{"model":"model-a","messages":[{"role":"user","content":"secret prompt"}]}`)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if inferer.calls != 0 {
		t.Fatalf("inference calls = %d, want 0", inferer.calls)
	}
	if strings.Contains(response.Body.String(), "secret prompt") {
		t.Fatal("public error leaked request content")
	}
}

func TestChatRejectsUnknownFieldBeforeInference(t *testing.T) {
	inferer := &fakeInferer{}
	response := chatRequest(t, newTestHandler(t, inferer), testToken, `{"model":"model-a","messages":[{"role":"user","content":"hi"}],"temperature":0.2}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if inferer.calls != 0 {
		t.Fatalf("inference calls = %d, want 0", inferer.calls)
	}
}

func TestChatRejectsUnauthorizedModelBeforeInference(t *testing.T) {
	inferer := &fakeInferer{}
	response := chatRequest(t, newTestHandler(t, inferer), testToken, `{"model":"other-model","messages":[{"role":"user","content":"hi"}]}`)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if inferer.calls != 0 {
		t.Fatalf("inference calls = %d, want 0", inferer.calls)
	}
}

func TestChatCollectsNonStreamingResponse(t *testing.T) {
	inferer := &fakeInferer{}
	response := chatRequest(t, newTestHandler(t, inferer), testToken, `{"model":"model-a","messages":[{"role":"user","content":"hi"}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Choices) != 1 || payload.Choices[0].Message.Content != "hello" {
		t.Fatalf("unexpected response: %s", response.Body.String())
	}
	if response.Header().Get("X-Request-Id") == "" {
		t.Fatal("missing request ID")
	}
}

func TestChatStreamsSSEThroughSynchronousSink(t *testing.T) {
	response := chatRequest(t, newTestHandler(t, &fakeInferer{}), testToken, `{"model":"model-a","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content type = %q", got)
	}
	body := response.Body.String()
	if !strings.Contains(body, `"content":"hello"`) || !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("unexpected stream: %s", body)
	}
}

func TestChatSanitizesInferenceFailure(t *testing.T) {
	inferer := &fakeInferer{err: errors.New("provider 10.0.0.8 leaked credential")}
	response := chatRequest(t, newTestHandler(t, inferer), testToken, `{"model":"model-a","messages":[{"role":"user","content":"hi"}]}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	if strings.Contains(response.Body.String(), "10.0.0.8") || strings.Contains(response.Body.String(), "credential") {
		t.Fatalf("public error leaked internal details: %s", response.Body.String())
	}
}

func TestHealthRoutesReflectState(t *testing.T) {
	state := &health.State{}
	state.SetStarted(true)
	handler := health.NewHandler(state)
	for path, want := range map[string]int{"/livez": http.StatusOK, "/startupz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Errorf("%s status = %d, want %d", path, response.Code, want)
		}
	}
}
