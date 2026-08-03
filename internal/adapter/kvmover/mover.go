package kvmover

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

var ErrUnsupported = errors.New("KV mover capability unsupported")

const defaultMaxHandles = 1024

// AuthenticatedControlPlane is a separately configured, mutually authenticated
// connector channel. Its DTOs cannot carry KV bytes, descriptors, addresses, or
// file handles.
type AuthenticatedControlPlane interface {
	ConnectorIdentity() string
	Start(context.Context, StartRequest) (StartResponse, error)
	Status(context.Context, StatusRequest) (StatusResponse, error)
	Commit(context.Context, HandleRequest) error
	Abort(context.Context, HandleRequest) error
}

type DestinationRevalidator interface {
	RevalidateDestination(context.Context, domain.WorkloadIdentity, domain.CapabilityManifest) error
}

type StartRequest struct {
	Tenant, RequestID       string
	Source, Destination     domain.WorkloadIdentity
	ProviderManifestDigest  string
	ConnectorManifestDigest string
	ExpiresAt               time.Time
	MaxBytes                uint64
}
type StartResponse struct{ Opaque []byte }
type StatusRequest struct {
	Opaque                  []byte
	Tenant, RequestID       string
	Source, Destination     domain.WorkloadIdentity
	ProviderManifestDigest  string
	ConnectorManifestDigest string
}
type HandleRequest = StatusRequest
type StatusResponse struct {
	State                                           domain.TransferState
	Sequence, TransferredBytes                      uint64
	Tenant, RequestID                               string
	Source, Destination                             domain.WorkloadIdentity
	ProviderManifestDigest, ConnectorManifestDigest string
}

type tracked struct {
	mu                            sync.Mutex
	handle                        domain.MoverHandle
	destinationManifest           domain.CapabilityManifest
	connector                     domain.ConnectorManifest
	status                        domain.TransferStatus
	hasStatus, committed, aborted bool
}

type Mover struct {
	control        AuthenticatedControlPlane
	revalidator    DestinationRevalidator
	now            func() time.Time
	cleanupTimeout time.Duration
	maxHandles     int
	mu             sync.Mutex
	handles        map[[32]byte]*tracked
}

func New(control AuthenticatedControlPlane, revalidator DestinationRevalidator, now func() time.Time, cleanupTimeout time.Duration) (*Mover, error) {
	return NewBounded(control, revalidator, now, cleanupTimeout, defaultMaxHandles)
}

func NewBounded(control AuthenticatedControlPlane, revalidator DestinationRevalidator, now func() time.Time, cleanupTimeout time.Duration, maxHandles int) (*Mover, error) {
	if control == nil || revalidator == nil || now == nil || cleanupTimeout <= 0 || maxHandles <= 0 {
		return nil, errors.New("invalid mover configuration")
	}
	return &Mover{control: control, revalidator: revalidator, now: now, cleanupTimeout: cleanupTimeout, maxHandles: maxHandles, handles: make(map[[32]byte]*tracked)}, nil
}

func (m *Mover) Start(ctx context.Context, spec domain.TransferSpec) (domain.MoverHandle, error) {
	now := m.now()
	connector := spec.ConnectorManifest()
	if !spec.SourceManifest().ValidAt(now) || !spec.DestinationManifest().ValidAt(now) || !spec.SourceManifest().Supports(domain.CapabilityMover) || !spec.DestinationManifest().Supports(domain.CapabilityMover) || !spec.SourceManifest().Compatible(spec.DestinationManifest()) || !connector.ValidAt(now) || connector.Digest() != spec.CacheIdentity().ConnectorManifestDigest() || connector.ControlPlaneIdentity() != m.control.ConnectorIdentity() || !now.Before(spec.ExpiresAt()) {
		return domain.MoverHandle{}, ErrUnsupported
	}
	opCtx, cancel := operationContext(ctx, now, spec.ExpiresAt(), connector.MaxOperation())
	defer cancel()
	response, err := m.control.Start(opCtx, StartRequest{spec.Tenant(), spec.RequestID(), spec.Source(), spec.Destination(), spec.SourceManifest().PayloadDigestString(), connector.Digest(), spec.ExpiresAt(), spec.MaxBytes()})
	if err != nil {
		return domain.MoverHandle{}, err
	}
	handle, err := domain.NewMoverHandle(spec, response.Opaque)
	if err != nil {
		return domain.MoverHandle{}, err
	}
	key := opaqueKey(response.Opaque)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictExpiredLocked(now)
	if _, collision := m.handles[key]; collision {
		return domain.MoverHandle{}, errors.New("mover handle collision")
	}
	if len(m.handles) >= m.maxHandles {
		return domain.MoverHandle{}, errors.New("mover handle capacity exceeded")
	}
	m.handles[key] = &tracked{handle: handle, destinationManifest: spec.DestinationManifest(), connector: connector}
	return handle, nil
}

func (m *Mover) Status(ctx context.Context, handle domain.MoverHandle) (domain.TransferStatus, error) {
	item, err := m.lookup(handle)
	if err != nil {
		return domain.TransferStatus{}, err
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.committed || item.aborted || !m.now().Before(handle.ExpiresAt()) {
		return domain.TransferStatus{}, errors.New("inactive mover handle")
	}
	opCtx, cancel := operationContext(ctx, m.now(), handle.ExpiresAt(), item.connector.MaxOperation())
	defer cancel()
	response, err := m.control.Status(opCtx, request(handle))
	if err != nil {
		return domain.TransferStatus{}, err
	}
	if response.Tenant != handle.Tenant() || response.RequestID != handle.RequestID() || !response.Source.Equal(handle.Source()) || !response.Destination.Equal(handle.Destination()) || response.ProviderManifestDigest != handle.CacheIdentity().ProviderManifestDigest() || response.ConnectorManifestDigest != handle.ConnectorManifestDigest() || response.TransferredBytes > handle.MaxBytes() {
		return domain.TransferStatus{}, errors.New("mover status binding mismatch")
	}
	status, err := domain.NewTransferStatus(domain.TransferStatusParams{Handle: handle, State: response.State, Sequence: response.Sequence, TransferredBytes: response.TransferredBytes, Previous: item.status, HasPrevious: item.hasStatus})
	if err != nil {
		return domain.TransferStatus{}, err
	}
	item.status, item.hasStatus = status, true
	return status, nil
}

func (m *Mover) Commit(ctx context.Context, handle domain.MoverHandle) error {
	item, err := m.lookup(handle)
	if err != nil {
		return err
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.committed {
		return nil
	}
	if item.aborted || !item.hasStatus || item.status.State() != domain.TransferComplete || item.status.TransferredBytes() > handle.MaxBytes() || !m.now().Before(handle.ExpiresAt()) {
		return errors.New("incomplete or inactive mover handle")
	}
	opCtx, cancel := operationContext(ctx, m.now(), handle.ExpiresAt(), item.connector.MaxOperation())
	defer cancel()
	// This is deliberately the last action before the publish RPC.
	if err = m.revalidator.RevalidateDestination(opCtx, handle.Destination(), item.destinationManifest); err != nil {
		return errors.New("destination identity or manifest changed")
	}
	if err = m.control.Commit(opCtx, request(handle)); err != nil {
		return err
	}
	item.committed = true
	return nil
}

func (m *Mover) Abort(ctx context.Context, handle domain.MoverHandle) error {
	item, err := m.lookup(handle)
	if err != nil {
		return nil
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.committed || item.aborted {
		return nil
	}
	limit := m.cleanupTimeout
	if manifestLimit := item.connector.MaxOperation(); manifestLimit < limit {
		limit = manifestLimit
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), limit)
	defer cancel()
	if err = m.control.Abort(cleanup, request(handle)); err != nil {
		return err
	}
	item.aborted = true
	return nil
}

func (m *Mover) lookup(handle domain.MoverHandle) (*tracked, error) {
	key := opaqueKey(handle.Opaque())
	m.mu.Lock()
	item, ok := m.handles[key]
	m.mu.Unlock()
	if !ok || item.handle.Tenant() != handle.Tenant() || item.handle.RequestID() != handle.RequestID() || !item.handle.Source().Equal(handle.Source()) || !item.handle.Destination().Equal(handle.Destination()) || item.handle.ConnectorManifestDigest() != handle.ConnectorManifestDigest() {
		return nil, errors.New("unknown mover handle")
	}
	return item, nil
}

func (m *Mover) evictExpiredLocked(now time.Time) {
	for key, item := range m.handles {
		if !now.Before(item.handle.ExpiresAt()) {
			delete(m.handles, key)
		}
	}
}

func request(handle domain.MoverHandle) HandleRequest {
	return HandleRequest{handle.Opaque(), handle.Tenant(), handle.RequestID(), handle.Source(), handle.Destination(), handle.CacheIdentity().ProviderManifestDigest(), handle.ConnectorManifestDigest()}
}

func opaqueKey(opaque []byte) [32]byte { return sha256.Sum256(opaque) }

func operationContext(parent context.Context, now, expiry time.Time, max time.Duration) (context.Context, context.CancelFunc) {
	deadline := expiry
	if bounded := now.Add(max); bounded.Before(deadline) {
		deadline = bounded
	}
	return context.WithDeadline(parent, deadline)
}
