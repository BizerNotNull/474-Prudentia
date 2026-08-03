package functional_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/auth"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

type countingInferer struct{ calls int }

func (i *countingInferer) Infer(context.Context, string, []byte, domain.AuthorizedRequest, domain.ResponseMode, publichttp.StreamSink) error {
	i.calls++
	return errors.New("unexpected inference")
}

func TestPublicDenialStopsBeforeScheduling(t *testing.T) {
	authenticator, err := auth.NewAuthenticator([]auth.APIKey{{Token: "0123456789abcdef0123456789abcdef", Tenant: "tenant-a", Models: []string{"model-a"}}})
	if err != nil {
		t.Fatal(err)
	}
	inferer := &countingInferer{}
	handler := publichttp.NewHandler(authenticator, auth.Authorizer{}, inferer, publichttp.DefaultLimits()).Routes(http.NotFoundHandler())
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[{"role":"user","content":"secret prompt"}]}`))
	request.Header.Set("Authorization", "Bearer wrong-credential-with-valid-length")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if inferer.calls != 0 {
		t.Fatalf("denied request reached inference boundary %d time(s)", inferer.calls)
	}
	if strings.Contains(response.Body.String(), "secret prompt") || strings.Contains(response.Body.String(), "wrong-credential") {
		t.Fatal("denial response leaked request material")
	}
}

func TestNonStreamingCollectorEnforcesEventAndByteBounds(t *testing.T) {
	ctx := context.Background()
	t.Run("events", func(t *testing.T) {
		collector := publichttp.NewNonStreamingCollector(1024, 1)
		delta, _ := domain.NewDeltaEvent("a")
		if err := collector.Write(ctx, delta); err != nil {
			t.Fatal(err)
		}
		if err := collector.Write(ctx, delta); err == nil {
			t.Fatal("collector accepted more than max events")
		}
	})
	t.Run("bytes", func(t *testing.T) {
		collector := publichttp.NewNonStreamingCollector(3, 8)
		delta, _ := domain.NewDeltaEvent("four")
		if err := collector.Write(ctx, delta); err == nil {
			t.Fatal("collector accepted output beyond max bytes")
		}
	})
	t.Run("requires terminal", func(t *testing.T) {
		collector := publichttp.NewNonStreamingCollector(1024, 8)
		delta, _ := domain.NewDeltaEvent("partial")
		if err := collector.Write(ctx, delta); err != nil {
			t.Fatal(err)
		}
		if _, err := collector.Result(); err == nil {
			t.Fatal("partial response was reported complete")
		}
	})
}
