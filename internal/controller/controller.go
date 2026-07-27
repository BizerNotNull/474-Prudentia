package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type Catalog interface {
	AcquireControllerWriterGeneration(context.Context, string, string) (domain.WriterGeneration, error)
	ReplaceResourceProjection(context.Context, domain.WriterGeneration, domain.ResourceState) error
}

type Discovery interface {
	Run(context.Context, func(domain.ResourceKey)) error
	WaitForSync(context.Context) error
	ListKeys(context.Context) ([]domain.ResourceKey, error)
	Reconcile(context.Context, domain.ResourceKey) (domain.ResourceState, error)
}

type LeaderElector interface {
	Elect(context.Context, func(context.Context) error) error
}

type Readiness interface {
	SetReady(bool)
}

type Controller struct {
	cluster string
	holder  string
	workers int
	catalog Catalog
	source  Discovery
	elector LeaderElector
	ready   Readiness
	queue   chan domain.ResourceKey
}

func New(cluster, holder string, workers, queueSize int, catalog Catalog, source Discovery, elector LeaderElector, ready Readiness) (*Controller, error) {
	if cluster == "" || holder == "" || workers < 1 || workers > 32 || queueSize < workers || queueSize > 65536 || catalog == nil || source == nil || elector == nil || ready == nil {
		return nil, errors.New("invalid controller configuration")
	}
	return &Controller{
		cluster: cluster, holder: holder, workers: workers, catalog: catalog,
		source: source, elector: elector, ready: ready, queue: make(chan domain.ResourceKey, queueSize),
	}, nil
}

func (c *Controller) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.ready.SetReady(false)

	discoveryErr := make(chan error, 1)
	go func() { discoveryErr <- c.source.Run(ctx, c.enqueue) }()
	syncErr := make(chan error, 1)
	go func() { syncErr <- c.source.WaitForSync(ctx) }()
	select {
	case <-ctx.Done():
		return nil
	case err := <-discoveryErr:
		return fmt.Errorf("run discovery before cache sync: %w", err)
	case err := <-syncErr:
		if err != nil {
			return fmt.Errorf("sync discovery cache: %w", err)
		}
	}

	electionErr := make(chan error, 1)
	go func() { electionErr <- c.elector.Elect(ctx, c.RunLeader) }()
	select {
	case <-ctx.Done():
		return nil
	case err := <-discoveryErr:
		if err == nil && ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("run discovery: %w", err)
	case err := <-electionErr:
		if err == nil && ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("run leader election: %w", err)
	}
}

func (c *Controller) RunLeader(ctx context.Context) error {
	c.ready.SetReady(false)
	defer c.ready.SetReady(false)
	generation, err := c.catalog.AcquireControllerWriterGeneration(ctx, c.cluster, c.holder)
	if err != nil {
		return fmt.Errorf("acquire controller writer generation: %w", err)
	}
	keys, err := c.source.ListKeys(ctx)
	if err != nil {
		return fmt.Errorf("relist discovery keys: %w", err)
	}
	for _, key := range keys {
		if err := c.Reconcile(ctx, generation, key); err != nil {
			return err
		}
	}
	c.ready.SetReady(true)

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var workers sync.WaitGroup
	errCh := make(chan error, 1)
	for range c.workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case key := <-c.queue:
					if err := c.Reconcile(workerCtx, generation, key); err != nil && workerCtx.Err() == nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
				}
			}
		}()
	}
	select {
	case <-ctx.Done():
		workers.Wait()
		return nil
	case err := <-errCh:
		cancelWorkers()
		workers.Wait()
		return err
	}
}

func (c *Controller) Reconcile(ctx context.Context, generation domain.WriterGeneration, key domain.ResourceKey) error {
	state, err := c.source.Reconcile(ctx, key)
	if err != nil {
		return fmt.Errorf("reconcile discovery %s: %w", key.String(), err)
	}
	if err := c.catalog.ReplaceResourceProjection(ctx, generation, state); err != nil {
		return fmt.Errorf("store projection %s: %w", key.String(), err)
	}
	return nil
}

func (c *Controller) enqueue(key domain.ResourceKey) {
	select {
	case c.queue <- key:
	default:
		// Hints may be coalesced or dropped: periodic informer resync and level reads converge state.
	}
}
