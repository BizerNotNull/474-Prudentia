package observability

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrometheusSinkExportsAllowlistedMetrics(t *testing.T) {
	sink, err := NewPrometheusSink()
	if err != nil {
		t.Fatal(err)
	}
	observer, err := NewObserver(sink)
	if err != nil {
		t.Fatal(err)
	}
	_, finish := observer.Inference(context.Background(), RequestAttrs{Route: "chat_completions", Method: http.MethodPost})
	finish(OutcomeSuccess)
	observer.RecordCapacity(context.Background(), CapacityEvent{
		Kind: CapacityUnsafeOverride, State: "resolved", Reason: "operator_override", Slots: 2, DebtAge: time.Minute,
	})
	observer.RecordCapacity(context.Background(), CapacityEvent{
		Kind: CapacityRecoveryFence, State: "closed", RecoveryEpoch: "customer-secret-epoch",
	})
	sink.Record(context.Background(), Point{Name: MetricCapacitySlots, Value: math.NaN(), Labels: map[string]string{"state": "secret"}})

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	sink.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, expected := range []string{
		`prudentia_request_total{method="POST",outcome="success",route="chat_completions"} 1`,
		`prudentia_unsafe_debt_override_total{reason="operator_override"} 1`,
		`prudentia_recovery_fence{epoch="current"} 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics missing %q:\n%s", expected, body)
		}
	}
	for _, prohibited := range []string{"customer-secret-epoch", `state="secret"`, "go_gc_duration_seconds", "process_cpu_seconds"} {
		if strings.Contains(body, prohibited) {
			t.Fatalf("metrics exposed prohibited value %q", prohibited)
		}
	}
}
