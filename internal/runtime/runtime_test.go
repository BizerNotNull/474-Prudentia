package runtime

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type validatorFunc func(context.Context) error

func (f validatorFunc) Validate(ctx context.Context) error { return f(ctx) }

type componentFunc func(context.Context) error

func (f componentFunc) Run(ctx context.Context) error { return f(ctx) }

type gateFunc func(context.Context) error

func (f gateFunc) Close(ctx context.Context) error { return f(ctx) }

type drainerFunc func(context.Context) error

func (f drainerFunc) Drain(ctx context.Context) error { return f(ctx) }

type orderedBootstrap struct {
	mu    sync.Mutex
	steps []string
}

func (b *orderedBootstrap) append(step string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.steps = append(b.steps, step)
}
func (b *orderedBootstrap) Warm(context.Context) error { b.append("warm"); return nil }
func (b *orderedBootstrap) AcquireGeneration(context.Context) error {
	b.append("generation")
	return nil
}
func (b *orderedBootstrap) FenceTakeovers(context.Context) error { b.append("fence"); return nil }

func TestRunGatewayClosesAdmissionBeforeDraining(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var order []string
	record := func(value string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, value)
	}
	started := make(chan struct{})
	service := componentFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		record("service_stopped")
		return ctx.Err()
	})
	done := make(chan error, 1)
	go func() {
		done <- RunGateway(ctx, Config{ShutdownGrace: time.Second}, GatewayDeps{
			Validator: validatorFunc(func(context.Context) error { return nil }),
			Admission: gateFunc(func(context.Context) error { record("admission_closed"); return nil }),
			Streams:   drainerFunc(func(context.Context) error { record("streams_drained"); return nil }),
			Services:  []Component{service},
		})
	}()
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "admission_closed" || order[1] != "streams_drained" {
		t.Fatalf("unsafe shutdown order: %v", order)
	}
}

func TestRunControllerBootstrapsBeforeWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bootstrap := &orderedBootstrap{}
	worker := componentFunc(func(ctx context.Context) error {
		bootstrap.append("worker")
		cancel()
		<-ctx.Done()
		return ctx.Err()
	})
	if err := RunController(ctx, Config{ShutdownGrace: time.Second}, ControllerDeps{
		Validator: validatorFunc(func(context.Context) error { return nil }),
		Bootstrap: bootstrap,
		Workers:   []Component{worker},
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bootstrap.steps, []string{"warm", "generation", "fence", "worker"}) {
		t.Fatalf("wrong controller lifecycle: %v", bootstrap.steps)
	}
}

func TestRunSchedulerRollsBackOnComponentFailure(t *testing.T) {
	failure := errors.New("admin bind failed")
	err := RunScheduler(context.Background(), Config{ShutdownGrace: time.Second}, SchedulerDeps{
		Validator: validatorFunc(func(context.Context) error { return nil }),
		Request: componentFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}),
		Admin: componentFunc(func(context.Context) error { return failure }),
	})
	if !errors.Is(err, failure) {
		t.Fatalf("lost startup failure: %v", err)
	}
}
