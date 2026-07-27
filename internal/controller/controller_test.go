package controller

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type fakeCatalog struct {
	mu     sync.Mutex
	events []string
	states []domain.ResourceState
}

func (f *fakeCatalog) AcquireControllerWriterGeneration(context.Context, string, string) (domain.WriterGeneration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "acquire")
	return 7, nil
}

func (f *fakeCatalog) ReplaceResourceProjection(_ context.Context, generation domain.WriterGeneration, state domain.ResourceState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "replace")
	f.states = append(f.states, state)
	if generation != 7 {
		return domain.ErrStaleWriterGeneration
	}
	return nil
}

type fakeDiscovery struct {
	key   domain.ResourceKey
	state domain.ResourceState
}

func (f *fakeDiscovery) Run(ctx context.Context, _ func(domain.ResourceKey)) error {
	<-ctx.Done()
	return nil
}
func (f *fakeDiscovery) WaitForSync(context.Context) error { return nil }
func (f *fakeDiscovery) ListKeys(context.Context) ([]domain.ResourceKey, error) {
	return []domain.ResourceKey{f.key}, nil
}
func (f *fakeDiscovery) Reconcile(context.Context, domain.ResourceKey) (domain.ResourceState, error) {
	return f.state, nil
}

type fakeElector struct{}

func (fakeElector) Elect(ctx context.Context, callback func(context.Context) error) error {
	return callback(ctx)
}

type fakeReadiness struct {
	mu          sync.Mutex
	values      []bool
	becameReady chan struct{}
	once        sync.Once
}

func (r *fakeReadiness) SetReady(value bool) {
	r.mu.Lock()
	r.values = append(r.values, value)
	r.mu.Unlock()
	if value {
		r.once.Do(func() { close(r.becameReady) })
	}
}

func TestRunLeaderAcquiresGenerationBeforeInitialLevelReconcile(t *testing.T) {
	key, err := domain.NewResourceKey("models", "pod-0")
	if err != nil {
		t.Fatal(err)
	}
	state, err := domain.NewResourceState("cluster-a", key, nil)
	if err != nil {
		t.Fatal(err)
	}
	catalog := &fakeCatalog{}
	readiness := &fakeReadiness{becameReady: make(chan struct{})}
	controller, err := New("cluster-a", "controller-1", 1, 8, catalog, &fakeDiscovery{key: key, state: state}, fakeElector{}, readiness)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- controller.RunLeader(ctx) }()
	<-readiness.becameReady
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if !reflect.DeepEqual(catalog.events, []string{"acquire", "replace"}) {
		t.Fatalf("unexpected operation order: %v", catalog.events)
	}
	if len(catalog.states) != 1 || catalog.states[0].Key() != key {
		t.Fatalf("unexpected stored states: %v", catalog.states)
	}
}
