package observability

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInferenceAllowsOnlyBoundedLabelsAndRecordsOnce(t *testing.T) {
	sink, _ := NewMemorySink(10)
	observer, _ := NewObserver(sink)
	clock := time.Unix(100, 0)
	observer.now = func() time.Time { clock = clock.Add(time.Second); return clock }
	_, finish := observer.Inference(context.Background(), RequestAttrs{Route: "/v1/users/tenant-secret", Method: "PATCH"})
	finish(OutcomeSuccess)
	finish(OutcomeAmbiguous)
	points := sink.Points()
	if len(points) != 2 {
		t.Fatalf("terminal recorder not idempotent: %d points", len(points))
	}
	for _, point := range points {
		if !reflect.DeepEqual(LabelKeys(point), []string{"method", "outcome", "route"}) {
			t.Fatalf("unexpected label set: %#v", point.Labels)
		}
		if point.Labels["route"] != "other" || point.Labels["method"] != "OTHER" {
			t.Fatalf("unbounded label escaped: %#v", point.Labels)
		}
	}
}

func TestCapacitySignalsNeverLabelUniqueEpoch(t *testing.T) {
	sink, _ := NewMemorySink(20)
	observer, _ := NewObserver(sink)
	observer.RecordCapacity(context.Background(), CapacityEvent{
		Kind: CapacityRecoveryFence, State: "closed", RecoveryEpoch: "epoch-customer-secret-123",
	})
	observer.RecordCapacity(context.Background(), CapacityEvent{
		Kind: CapacityUnsafeOverride, State: "resolved", Reason: "operator_override", Slots: 2, DebtAge: time.Minute,
	})
	points := sink.Points()
	var recovery, override bool
	for _, point := range points {
		for _, value := range point.Labels {
			if strings.Contains(value, "secret") {
				t.Fatalf("unique value used as metric label: %#v", point)
			}
		}
		recovery = recovery || point.Name == MetricRecoveryFence
		override = override || point.Name == MetricUnsafeOverride
	}
	if !recovery || !override {
		t.Fatalf("missing safety metrics: %#v", points)
	}
}

func TestProviderDiagnosticRedactsAndBounds(t *testing.T) {
	redactor, err := NewRedactor(160)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"prompt":"raw prompt","nested":{"token":"sekrit"},"error":"call http://10.1.2.3:8000 Authorization: Bearer-secret eyJabcdefgh.ijklmnop.qrstuvwx"}`)
	diagnostic := redactor.ProviderDiagnostic(body, http.Header{"Authorization": {"Bearer header-secret"}, "Content-Type": {"application/json"}})
	for _, forbidden := range []string{"raw prompt", "sekrit", "10.1.2.3", "Bearer-secret", "header-secret", "eyJabcdefgh"} {
		if strings.Contains(diagnostic, forbidden) {
			t.Fatalf("diagnostic leaked %q: %s", forbidden, diagnostic)
		}
	}
	if !strings.Contains(diagnostic, "[REDACTED]") || !strings.Contains(diagnostic, "content-type") {
		t.Fatalf("diagnostic lacks safe context: %s", diagnostic)
	}
}
