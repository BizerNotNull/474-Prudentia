package observability

import (
	"context"
	"fmt"
	"math"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusSink exports the observer's fixed, low-cardinality metric set.
// It owns an isolated registry so process and Go runtime metrics are not
// exposed implicitly.
type PrometheusSink struct {
	registry        *prometheus.Registry
	requestTotal    *prometheus.CounterVec
	requestLatency  *prometheus.HistogramVec
	capacityEvents  *prometheus.CounterVec
	capacitySlots   *prometheus.GaugeVec
	debt            *prometheus.GaugeVec
	debtAge         *prometheus.HistogramVec
	barriers        *prometheus.CounterVec
	barrierLatency  *prometheus.HistogramVec
	recoveryFence   *prometheus.GaugeVec
	unsafeOverrides *prometheus.CounterVec
}

func NewPrometheusSink() (*PrometheusSink, error) {
	sink := &PrometheusSink{
		registry: prometheus.NewRegistry(),
		requestTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: string(MetricRequestTotal), Help: "Completed gateway inference requests.",
		}, []string{"route", "method", "outcome"}),
		requestLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: string(MetricRequestDuration), Help: "Gateway inference request duration in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
		}, []string{"route", "method", "outcome"}),
		capacityEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: string(MetricCapacityEvent), Help: "Authoritative capacity lifecycle events.",
		}, []string{"event", "reason", "state"}),
		capacitySlots: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: string(MetricCapacitySlots), Help: "Authoritative capacity slots by state.",
		}, []string{"state"}),
		debt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: string(MetricDebt), Help: "Authoritative orphaned capacity debt slots.",
		}, []string{"state", "reason"}),
		debtAge: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: string(MetricDebtAge), Help: "Observed orphaned debt age in seconds.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 10),
		}, []string{"state"}),
		barriers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: string(MetricBarrier), Help: "Durable workload operation barrier events.",
		}, []string{"state"}),
		barrierLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: string(MetricBarrierLatency), Help: "Workload operation barrier latency in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"state"}),
		recoveryFence: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: string(MetricRecoveryFence), Help: "Recovery admission fence state; one means closed.",
		}, []string{"epoch"}),
		unsafeOverrides: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: string(MetricUnsafeOverride), Help: "Privileged unsafe capacity debt overrides.",
		}, []string{"reason"}),
	}
	collectors := []prometheus.Collector{
		sink.requestTotal, sink.requestLatency, sink.capacityEvents, sink.capacitySlots,
		sink.debt, sink.debtAge, sink.barriers, sink.barrierLatency, sink.recoveryFence,
		sink.unsafeOverrides,
	}
	for _, collector := range collectors {
		if err := sink.registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register observability collector: %w", err)
		}
	}
	return sink, nil
}

func (s *PrometheusSink) Record(_ context.Context, point Point) {
	if !validMetricValue(point.Value) {
		return
	}
	switch point.Name {
	case MetricRequestTotal:
		if point.Value >= 0 {
			s.requestTotal.WithLabelValues(point.Labels["route"], point.Labels["method"], point.Labels["outcome"]).Add(point.Value)
		}
	case MetricRequestDuration:
		if point.Value >= 0 {
			s.requestLatency.WithLabelValues(point.Labels["route"], point.Labels["method"], point.Labels["outcome"]).Observe(point.Value)
		}
	case MetricCapacityEvent:
		if point.Value >= 0 {
			s.capacityEvents.WithLabelValues(point.Labels["event"], point.Labels["reason"], point.Labels["state"]).Add(point.Value)
		}
	case MetricCapacitySlots:
		s.capacitySlots.WithLabelValues(point.Labels["state"]).Set(point.Value)
	case MetricDebt:
		s.debt.WithLabelValues(point.Labels["state"], point.Labels["reason"]).Set(point.Value)
	case MetricDebtAge:
		if point.Value >= 0 {
			s.debtAge.WithLabelValues(point.Labels["state"]).Observe(point.Value)
		}
	case MetricBarrier:
		if point.Value >= 0 {
			s.barriers.WithLabelValues(point.Labels["state"]).Add(point.Value)
		}
	case MetricBarrierLatency:
		if point.Value >= 0 {
			s.barrierLatency.WithLabelValues(point.Labels["state"]).Observe(point.Value)
		}
	case MetricRecoveryFence:
		s.recoveryFence.WithLabelValues(point.Labels["epoch"]).Set(point.Value)
	case MetricUnsafeOverride:
		if point.Value >= 0 {
			s.unsafeOverrides.WithLabelValues(point.Labels["reason"]).Add(point.Value)
		}
	}
}

func (s *PrometheusSink) Handler() http.Handler {
	return promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

func validMetricValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
