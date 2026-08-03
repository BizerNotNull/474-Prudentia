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
}

type Discovery interface {
	Run(context.Context, func(domain.ResourceKey)) error
	WaitForSync(context.Context) error
	ListKeys(context.Context) ([]domain.ResourceKey, error)
	Reconcile(context.Context, domain.ResourceKey) (domain.ResourceState, error)
}

type NormalizedDiscovery interface {
	ReadDesired(context.Context, domain.ResourceKey) (domain.DesiredModel, error)
	ReconcileDiscovery(context.Context, domain.ResourceKey, domain.WriterGeneration) ([]domain.Observation, error)
	ApplyDesired(context.Context, domain.DesiredModel) (domain.ApplyResult, error)
}

type ObservationCapacityCatalog interface {
	RecordObservation(context.Context, domain.WriterGeneration, domain.Observation) (domain.StoredSourceStamp, bool, error)
	SyncCapacityProjection(context.Context, domain.WriterGeneration, domain.ProjectionUpdate) (domain.ProjectionVersion, error)
}

// ProviderEvidence resolves an exact signed manifest for each projection before
// probing through the attested proxy. Missing health or load is ineligible, not zero.
type ProviderEvidence interface {
	ProbeTarget(context.Context, domain.BackendProjection) (domain.ProbeTarget, error)
	Probe(context.Context, domain.ProbeTarget) (domain.RuntimeHealthObservation, error)
	ScrapeLoad(context.Context, domain.ProbeTarget) (domain.LoadObservation, error)
}

type LeaderElector interface {
	Elect(context.Context, func(context.Context) error) error
}

type Readiness interface {
	SetReady(bool)
}

type Controller struct {
	cluster      string
	holder       string
	workers      int
	catalog      Catalog
	observations ObservationCapacityCatalog
	operations   OperationCatalog
	source       Discovery
	normalized   NormalizedDiscovery
	workloads    WorkloadControl
	provider     ProviderEvidence
	elector      LeaderElector
	ready        Readiness
	queue        chan domain.ResourceKey
	sequenceMu   sync.Mutex
	sequence     uint64
}

func New(cluster, holder string, workers, queueSize int, catalog Catalog, source Discovery, elector LeaderElector, ready Readiness, provider ...ProviderEvidence) (*Controller, error) {
	if cluster == "" || holder == "" || workers < 1 || workers > 32 || queueSize < workers || queueSize > 65536 || catalog == nil || source == nil || elector == nil || ready == nil || len(provider) != 1 || provider[0] == nil {
		return nil, errors.New("invalid controller configuration")
	}
	observations, observationsOK := catalog.(ObservationCapacityCatalog)
	operations, operationsOK := catalog.(OperationCatalog)
	normalized, normalizedOK := source.(NormalizedDiscovery)
	workloads, workloadsOK := source.(WorkloadControl)
	if !observationsOK || !operationsOK || !normalizedOK || !workloadsOK {
		return nil, errors.New("mandatory controller observation, capacity, desired-state, or operation port unavailable")
	}
	return &Controller{
		cluster: cluster, holder: holder, workers: workers, catalog: catalog,
		observations: observations, operations: operations, source: source, normalized: normalized,
		workloads: workloads, provider: provider[0], elector: elector, ready: ready,
		queue: make(chan domain.ResourceKey, queueSize),
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
	operations, err := c.operations.ListIncompleteWorkloadOperations(ctx, generation)
	if err != nil {
		return fmt.Errorf("list incomplete workload operations: %w", err)
	}
	for _, operation := range operations {
		if _, err := c.FenceWorkloadHandoff(ctx, generation, operation.Scope()); err != nil {
			return err
		}
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
	structural, err := c.normalized.ReconcileDiscovery(ctx, key, generation)
	if err != nil {
		return fmt.Errorf("normalize discovery %s: %w", key.String(), err)
	}
	structuralByIdentity := make(map[domain.WorkloadIdentity]domain.Observation, len(structural))
	for _, observation := range structural {
		structuralByIdentity[observation.Identity()] = observation
	}
	for _, projection := range state.Projections() {
		structuralObservation, ok := structuralByIdentity[projection.Identity()]
		if !ok {
			return fmt.Errorf("structural observation missing for %s", key.String())
		}
		structuralStamp, accepted, err := c.observations.RecordObservation(ctx, generation, structuralObservation)
		if err != nil {
			return fmt.Errorf("record structural observation %s: %w", key.String(), err)
		}
		if !accepted {
			return fmt.Errorf("structural observation was not current for %s", key.String())
		}
		target, err := c.provider.ProbeTarget(ctx, projection)
		if err != nil {
			return fmt.Errorf("resolve exact manifest probe target %s: %w", key.String(), err)
		}
		health, err := c.provider.Probe(ctx, target)
		if err != nil {
			return fmt.Errorf("probe exact provider identity %s: %w", key.String(), err)
		}
		load, err := c.provider.ScrapeLoad(ctx, target)
		if err != nil {
			return fmt.Errorf("scrape exact provider load %s: %w", key.String(), err)
		}
		healthObservation, err := c.healthObservation(generation, health)
		if err != nil {
			return err
		}
		loadObservation, err := c.loadObservation(generation, load)
		if err != nil {
			return err
		}
		healthStamp, accepted, err := c.observations.RecordObservation(ctx, generation, healthObservation)
		if err != nil || !accepted {
			return fmt.Errorf("record runtime health observation %s: accepted=%t: %w", key.String(), accepted, err)
		}
		loadStamp, accepted, err := c.observations.RecordObservation(ctx, generation, loadObservation)
		if err != nil || !accepted {
			return fmt.Errorf("record load observation %s: accepted=%t: %w", key.String(), accepted, err)
		}
		update, err := domain.NewProjectionUpdate(domain.ProjectionUpdateParams{
			Identity: projection.Identity(), Structural: structuralStamp, Health: healthStamp,
			Load: loadStamp, HasLoad: true, ConfiguredSlots: projection.ConfiguredSlots(),
			AdmissionLimit: projection.ConfiguredSlots(),
		})
		if err != nil {
			return fmt.Errorf("construct capacity projection %s: %w", key.String(), err)
		}
		if _, err := c.observations.SyncCapacityProjection(ctx, generation, update); err != nil {
			return fmt.Errorf("synchronize capacity projection %s: %w", key.String(), err)
		}
	}
	desired, err := c.normalized.ReadDesired(ctx, key)
	if err != nil {
		return fmt.Errorf("read desired state %s: %w", key.String(), err)
	}
	if _, err := c.normalized.ApplyDesired(ctx, desired); err != nil {
		return fmt.Errorf("apply desired state %s: %w", key.String(), err)
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

func (c *Controller) nextSequence() domain.SourceSequence {
	c.sequenceMu.Lock()
	defer c.sequenceMu.Unlock()
	c.sequence++
	return domain.SourceSequence(c.sequence)
}

func (c *Controller) healthObservation(generation domain.WriterGeneration, value domain.RuntimeHealthObservation) (domain.Observation, error) {
	state, warm := domain.HealthUnhealthy, false
	switch value.State() {
	case domain.RuntimeHealthResponsive:
		state, warm = domain.HealthReady, true
	case domain.RuntimeHealthWarming:
		state = domain.HealthStarting
	case domain.RuntimeHealthUnresponsive:
	default:
		return domain.Observation{}, errors.New("provider returned unknown runtime health state")
	}
	fact, err := domain.NewRuntimeHealthFact(state, warm)
	if err != nil {
		return domain.Observation{}, err
	}
	stamp, err := domain.NewSourceStamp(domain.SourceRuntimeHealth, generation, c.nextSequence())
	if err != nil {
		return domain.Observation{}, err
	}
	return domain.NewObservation(domain.ObservationParams{
		Stamp: stamp, Identity: value.Identity(), TTLClass: domain.TTLRuntimeHealth,
		RuntimeHealth: fact, SourceReportedAt: value.ObservedAt(), HasSourceReportedAt: true,
	})
}

func (c *Controller) loadObservation(generation domain.WriterGeneration, value domain.LoadObservation) (domain.Observation, error) {
	running, hasRunning := value.UsedSlots()
	queued, hasQueued := value.QueueDepth()
	if !hasRunning || !hasQueued {
		return domain.Observation{}, errors.New("provider load observation is incomplete")
	}
	fact, err := domain.NewLoadFact(domain.LoadFactParams{RunningRequests: running, QueuedRequests: queued})
	if err != nil {
		return domain.Observation{}, err
	}
	stamp, err := domain.NewSourceStamp(domain.SourceLoad, generation, c.nextSequence())
	if err != nil {
		return domain.Observation{}, err
	}
	return domain.NewObservation(domain.ObservationParams{
		Stamp: stamp, Identity: value.Identity(), TTLClass: domain.TTLLoad,
		Load: fact, SourceReportedAt: value.ObservedAt(), HasSourceReportedAt: true,
	})
}
