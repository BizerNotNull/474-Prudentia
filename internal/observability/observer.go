package observability

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type Metric string

const (
	MetricRequestTotal    Metric = "prudentia_request_total"
	MetricRequestDuration Metric = "prudentia_request_duration_seconds"
	MetricCapacityEvent   Metric = "prudentia_capacity_event_total"
	MetricCapacitySlots   Metric = "prudentia_capacity_slots"
	MetricDebt            Metric = "prudentia_orphaned_debt"
	MetricDebtAge         Metric = "prudentia_orphaned_debt_age_seconds"
	MetricBarrier         Metric = "prudentia_operation_barrier_total"
	MetricBarrierLatency  Metric = "prudentia_operation_barrier_seconds"
	MetricRecoveryFence   Metric = "prudentia_recovery_fence"
	MetricUnsafeOverride  Metric = "prudentia_unsafe_debt_override_total"
)

type Point struct {
	Name   Metric
	Value  float64
	Labels map[string]string
}

type Sink interface {
	Record(context.Context, Point)
}

type RequestAttrs struct {
	Route  string
	Method string
}

type Outcome string

const (
	OutcomeSuccess   Outcome = "success"
	OutcomeRejected  Outcome = "rejected"
	OutcomeCancelled Outcome = "cancelled"
	OutcomeAmbiguous Outcome = "ambiguous"
	OutcomeError     Outcome = "error"
)

type CapacityKind string

const (
	CapacityReserved       CapacityKind = "reserved"
	CapacityReleased       CapacityKind = "released"
	CapacityRerankAbandon  CapacityKind = "rerank_abandon"
	CapacityGiveUp         CapacityKind = "pre_dispatch_give_up"
	CapacityRetainedGrant  CapacityKind = "retained_grant"
	CapacityDebtCreated    CapacityKind = "debt_created"
	CapacityDebtResolved   CapacityKind = "debt_resolved"
	CapacityUnsafeOverride CapacityKind = "unsafe_override"
	CapacityBarrier        CapacityKind = "barrier"
	CapacityRecoveryFence  CapacityKind = "recovery_fence"
)

type CapacityEvent struct {
	Kind          CapacityKind
	Reason        string
	State         string
	Slots         int64
	DebtAge       time.Duration
	Latency       time.Duration
	RecoveryEpoch string
}

type Observer struct {
	sink Sink
	now  func() time.Time
}

func NewObserver(sink Sink) (*Observer, error) {
	if sink == nil {
		return nil, errors.New("observability: sink is required")
	}
	return &Observer{sink: sink, now: time.Now}, nil
}

func (o *Observer) Inference(ctx context.Context, attrs RequestAttrs) (context.Context, func(Outcome)) {
	started := o.now()
	labels := map[string]string{"route": allowedRoute(attrs.Route), "method": allowedMethod(attrs.Method)}
	var once sync.Once
	return ctx, func(outcome Outcome) {
		once.Do(func() {
			labels["outcome"] = allowedOutcome(outcome)
			o.sink.Record(ctx, Point{Name: MetricRequestTotal, Value: 1, Labels: cloneLabels(labels)})
			o.sink.Record(ctx, Point{Name: MetricRequestDuration, Value: o.now().Sub(started).Seconds(), Labels: cloneLabels(labels)})
		})
	}
}

func (o *Observer) RecordCapacity(ctx context.Context, event CapacityEvent) {
	kind := allowedKind(event.Kind)
	labels := map[string]string{"event": string(kind), "reason": allowedReason(event.Reason), "state": allowedState(event.State)}
	o.sink.Record(ctx, Point{Name: MetricCapacityEvent, Value: 1, Labels: labels})
	if event.Slots != 0 {
		o.sink.Record(ctx, Point{Name: MetricCapacitySlots, Value: float64(event.Slots), Labels: map[string]string{"state": allowedState(event.State)}})
	}
	if kind == CapacityDebtCreated || kind == CapacityDebtResolved || kind == CapacityUnsafeOverride {
		o.sink.Record(ctx, Point{Name: MetricDebt, Value: float64(event.Slots), Labels: map[string]string{"state": allowedState(event.State), "reason": allowedReason(event.Reason)}})
		if event.DebtAge > 0 {
			o.sink.Record(ctx, Point{Name: MetricDebtAge, Value: event.DebtAge.Seconds(), Labels: map[string]string{"state": allowedState(event.State)}})
		}
	}
	if kind == CapacityUnsafeOverride {
		o.sink.Record(ctx, Point{Name: MetricUnsafeOverride, Value: 1, Labels: map[string]string{"reason": allowedReason(event.Reason)}})
	}
	if kind == CapacityBarrier {
		o.sink.Record(ctx, Point{Name: MetricBarrier, Value: 1, Labels: map[string]string{"state": allowedState(event.State)}})
		if event.Latency > 0 {
			o.sink.Record(ctx, Point{Name: MetricBarrierLatency, Value: event.Latency.Seconds(), Labels: map[string]string{"state": allowedState(event.State)}})
		}
	}
	if kind == CapacityRecoveryFence {
		o.sink.Record(ctx, Point{Name: MetricRecoveryFence, Value: recoveryValue(event.State), Labels: map[string]string{"epoch": boundedEpoch(event.RecoveryEpoch)}})
	}
}

func allowedRoute(value string) string {
	switch value {
	case "chat_completions", "completions", "health":
		return value
	default:
		return "other"
	}
}

func allowedMethod(value string) string {
	switch value {
	case "GET", "POST":
		return value
	default:
		return "OTHER"
	}
}

func allowedOutcome(value Outcome) string {
	switch value {
	case OutcomeSuccess, OutcomeRejected, OutcomeCancelled, OutcomeAmbiguous, OutcomeError:
		return string(value)
	default:
		return "error"
	}
}

func allowedKind(value CapacityKind) CapacityKind {
	switch value {
	case CapacityReserved, CapacityReleased, CapacityRerankAbandon, CapacityGiveUp, CapacityRetainedGrant, CapacityDebtCreated, CapacityDebtResolved, CapacityUnsafeOverride, CapacityBarrier, CapacityRecoveryFence:
		return value
	default:
		return CapacityKind("unknown")
	}
}

func allowedReason(value string) string {
	switch value {
	case "", "capacity", "deadline", "cancelled", "identity_gone", "provider_ack", "operator_override", "recovery", "conflict":
		if value == "" {
			return "none"
		}
		return value
	default:
		return "other"
	}
}

func allowedState(value string) string {
	switch value {
	case "active", "retained", "orphaned", "resolved", "installed", "observed", "open", "closed":
		return value
	default:
		return "unknown"
	}
}

func boundedEpoch(value string) string {
	// Epochs are intentionally bucketed: unique recovery identifiers are not labels.
	if value == "" {
		return "unknown"
	}
	return "current"
}

func recoveryValue(state string) float64 {
	if state == "closed" {
		return 1
	}
	return 0
}

func cloneLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

// MemorySink is a bounded test/embedding sink; production exporters can implement Sink.
type MemorySink struct {
	mu     sync.Mutex
	limit  int
	points []Point
}

func NewMemorySink(limit int) (*MemorySink, error) {
	if limit <= 0 {
		return nil, errors.New("observability: positive sink limit is required")
	}
	return &MemorySink{limit: limit}, nil
}

func (s *MemorySink) Record(_ context.Context, point Point) {
	s.mu.Lock()
	defer s.mu.Unlock()
	point.Labels = cloneLabels(point.Labels)
	if len(s.points) == s.limit {
		copy(s.points, s.points[1:])
		s.points[len(s.points)-1] = point
		return
	}
	s.points = append(s.points, point)
}

func (s *MemorySink) Points() []Point {
	s.mu.Lock()
	defer s.mu.Unlock()
	points := make([]Point, len(s.points))
	copy(points, s.points)
	for index := range points {
		points[index].Labels = cloneLabels(points[index].Labels)
	}
	return points
}

func LabelKeys(point Point) []string {
	keys := make([]string, 0, len(point.Labels))
	for key := range point.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
