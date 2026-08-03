package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

var ErrCompatibleCacheRequired = errors.New("compatible cache required")

type record struct {
	identity domain.CacheIdentity
	source   domain.WorkloadIdentity
	expires  time.Time
}

// Metadata starts empty and therefore returns truthful misses until a verified,
// request-specific record is supplied by the pinned provider event adapter.
type Metadata struct {
	mu      sync.RWMutex
	records map[[32]byte]record
	now     func() time.Time
}

func NewMetadata(now func() time.Time) (*Metadata, error) {
	if now == nil {
		return nil, errors.New("cache clock required")
	}
	return &Metadata{records: make(map[[32]byte]record), now: now}, nil
}

func (m *Metadata) Lookup(_ context.Context, id domain.CacheIdentity) (domain.CacheHint, error) {
	key := id.TenantHMAC()
	m.mu.RLock()
	found, ok := m.records[key]
	m.mu.RUnlock()
	if !ok || !found.identity.Compatible(id) || m.now().After(found.expires) {
		return domain.NewCacheMiss(), nil
	}
	hint, err := domain.NewCacheHit(id, found.identity, found.source, found.expires)
	if err != nil {
		return domain.NewCacheMiss(), nil
	}
	return hint, nil
}

// RecordVerified accepts only already authenticated provider evidence and never
// accepts aggregate utilization as a cache-location claim.
func (m *Metadata) RecordVerified(id domain.CacheIdentity, source domain.WorkloadIdentity, expires time.Time) error {
	if source.PodUID() == "" || expires.IsZero() || !m.now().Before(expires) {
		return errors.New("invalid verified cache metadata")
	}
	key := id.TenantHMAC()
	m.mu.Lock()
	m.records[key] = record{id, source, expires}
	m.mu.Unlock()
	return nil
}

type Mover interface {
	Start(context.Context, domain.TransferSpec) (domain.MoverHandle, error)
	Status(context.Context, domain.MoverHandle) (domain.TransferStatus, error)
	Commit(context.Context, domain.MoverHandle) error
	Abort(context.Context, domain.MoverHandle) error
}

type Coordinator struct {
	mover    Mover
	now      func() time.Time
	poll     time.Duration
	maxBytes uint64
}

func NewCoordinator(mover Mover, now func() time.Time, poll time.Duration, maxBytes uint64) (*Coordinator, error) {
	if now == nil || poll <= 0 || maxBytes == 0 {
		return nil, errors.New("invalid cache coordinator configuration")
	}
	return &Coordinator{mover: mover, now: now, poll: poll, maxBytes: maxBytes}, nil
}

func (c *Coordinator) Prepare(ctx context.Context, req domain.CacheRequest, target domain.ReservedTarget) (domain.CachePreparation, error) {
	cold := func() (domain.CachePreparation, error) {
		if req.Requirement() == domain.RequireCompatible {
			return domain.CachePreparation{}, ErrCompatibleCacheRequired
		}
		return domain.NewColdPreparation(req, target)
	}
	if req.Tenant() != target.Tenant() || !req.Identity().Compatible(target.CacheIdentity()) {
		return cold()
	}
	if req.Hint().ValidFor(req.Identity(), c.now()) {
		if source, ok := req.Hint().Source(); ok && source.Equal(target.Identity()) {
			return domain.NewLocalHitPreparation(req, target, c.now())
		}
	}
	source, ok := req.Hint().Source()
	if !ok || c.mover == nil || !req.Hint().ValidFor(req.Identity(), c.now()) || !target.Manifest().Supports(domain.CapabilityMover) {
		return cold()
	}
	deadline := c.now().Add(req.Budget())
	if expiry, ok := req.Hint().ExpiresAt(); ok && expiry.Before(deadline) {
		deadline = expiry
	}
	spec, err := domain.NewTransferSpec(domain.TransferSpecParams{Tenant: req.Tenant(), RequestID: req.RequestID(), CacheIdentity: req.Identity(), Source: source, Destination: target.Identity(), SourceManifest: target.Manifest(), DestinationManifest: target.Manifest(), ExpiresAt: deadline, MaxBytes: c.maxBytes})
	if err != nil {
		return cold()
	}
	handle, err := c.mover.Start(ctx, spec)
	if err != nil {
		return cold()
	}
	committed := false
	defer func() {
		if !committed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.poll)
			defer cancel()
			_ = c.mover.Abort(cleanup, handle)
		}
	}()
	timer := time.NewTimer(c.poll)
	defer timer.Stop()
	var previous domain.TransferStatus
	hasPrevious := false
	for {
		status, statusErr := c.mover.Status(ctx, handle)
		if statusErr != nil || (hasPrevious && !previous.CanAdvanceTo(status)) {
			return cold()
		}
		switch status.State() {
		case domain.TransferComplete:
			if err = c.mover.Commit(ctx, handle); err != nil {
				return cold()
			}
			committed = true
			return domain.NewTransferredPreparation(req, target, status)
		case domain.TransferFailed, domain.TransferAborted:
			return cold()
		}
		previous, hasPrevious = status, true
		select {
		case <-ctx.Done():
			return cold()
		case <-timer.C:
			timer.Reset(c.poll)
		}
	}
}
