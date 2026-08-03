package kvmover

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

var ErrUnsupported = errors.New("KV mover capability unsupported")

// AuthenticatedControlPlane is implemented by a pinned mTLS connector adapter.
// Its contract deliberately contains no KV bytes, file descriptors, addresses,
// or transport descriptors.
type AuthenticatedControlPlane interface {
	Start(context.Context, StartRequest) (StartResponse, error)
	Status(context.Context, StatusRequest) (StatusResponse, error)
	Commit(context.Context, HandleRequest) error
	Abort(context.Context, HandleRequest) error
}

type StartRequest struct {
	Tenant, RequestID   string
	Source, Destination domain.WorkloadIdentity
	ManifestID          string
	ExpiresAt           time.Time
	MaxBytes            uint64
}
type StartResponse struct{ Opaque []byte }
type StatusRequest struct {
	Opaque      []byte
	Tenant      string
	Destination domain.WorkloadIdentity
	ManifestID  string
}
type HandleRequest = StatusRequest
type StatusResponse struct {
	State                      domain.TransferState
	Sequence, TransferredBytes uint64
	Tenant                     string
	Destination                domain.WorkloadIdentity
	ManifestID                 string
}

type tracked struct {
	status                        domain.TransferStatus
	hasStatus, committed, aborted bool
}
type Mover struct {
	control        AuthenticatedControlPlane
	now            func() time.Time
	cleanupTimeout time.Duration
	mu             sync.Mutex
	handles        map[string]tracked
}

func New(control AuthenticatedControlPlane, now func() time.Time, cleanupTimeout time.Duration) (*Mover, error) {
	if control == nil || now == nil || cleanupTimeout <= 0 {
		return nil, errors.New("invalid mover configuration")
	}
	return &Mover{control: control, now: now, cleanupTimeout: cleanupTimeout, handles: make(map[string]tracked)}, nil
}

func (m *Mover) Start(ctx context.Context, spec domain.TransferSpec) (domain.MoverHandle, error) {
	if !spec.SourceManifest().Supports(domain.CapabilityMover) || !spec.DestinationManifest().Supports(domain.CapabilityMover) || !spec.SourceManifest().Compatible(spec.DestinationManifest()) || !m.now().Before(spec.ExpiresAt()) {
		return domain.MoverHandle{}, ErrUnsupported
	}
	response, err := m.control.Start(ctx, StartRequest{spec.Tenant(), spec.RequestID(), spec.Source(), spec.Destination(), spec.SourceManifest().ID(), spec.ExpiresAt(), spec.MaxBytes()})
	if err != nil {
		return domain.MoverHandle{}, err
	}
	handle, err := domain.NewMoverHandle(spec, response.Opaque)
	if err != nil {
		return domain.MoverHandle{}, err
	}
	m.mu.Lock()
	m.handles[string(response.Opaque)] = tracked{}
	m.mu.Unlock()
	return handle, nil
}

func (m *Mover) Status(ctx context.Context, handle domain.MoverHandle) (domain.TransferStatus, error) {
	if !m.now().Before(handle.ExpiresAt()) {
		return domain.TransferStatus{}, errors.New("mover handle expired")
	}
	key := string(handle.Opaque())
	m.mu.Lock()
	prior, ok := m.handles[key]
	m.mu.Unlock()
	if !ok || prior.committed || prior.aborted {
		return domain.TransferStatus{}, errors.New("unknown mover handle")
	}
	response, err := m.control.Status(ctx, request(handle))
	if err != nil {
		return domain.TransferStatus{}, err
	}
	if response.Tenant != handle.Tenant() || !response.Destination.Equal(handle.Destination()) || response.ManifestID != handle.ManifestID() {
		return domain.TransferStatus{}, errors.New("mover status binding mismatch")
	}
	status, err := domain.NewTransferStatus(domain.TransferStatusParams{Handle: handle, State: response.State, Sequence: response.Sequence, TransferredBytes: response.TransferredBytes, Previous: prior.status, HasPrevious: prior.hasStatus})
	if err != nil {
		return domain.TransferStatus{}, err
	}
	m.mu.Lock()
	current, exists := m.handles[key]
	if exists && current.hasStatus && !current.status.CanAdvanceTo(status) {
		m.mu.Unlock()
		return domain.TransferStatus{}, errors.New("non-monotonic concurrent mover status")
	}
	current.status, current.hasStatus = status, true
	m.handles[key] = current
	m.mu.Unlock()
	return status, nil
}

func (m *Mover) Commit(ctx context.Context, handle domain.MoverHandle) error {
	key := string(handle.Opaque())
	m.mu.Lock()
	item, ok := m.handles[key]
	if !ok || item.aborted || !item.hasStatus || item.status.State() != domain.TransferComplete {
		m.mu.Unlock()
		return errors.New("incomplete or unknown mover handle")
	}
	if item.committed {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	if err := m.control.Commit(ctx, request(handle)); err != nil {
		return err
	}
	m.mu.Lock()
	item = m.handles[key]
	item.committed = true
	m.handles[key] = item
	m.mu.Unlock()
	return nil
}

func (m *Mover) Abort(ctx context.Context, handle domain.MoverHandle) error {
	key := string(handle.Opaque())
	m.mu.Lock()
	item, ok := m.handles[key]
	if !ok || item.committed {
		m.mu.Unlock()
		return nil
	}
	if item.aborted {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.cleanupTimeout)
	defer cancel()
	if err := m.control.Abort(cleanup, request(handle)); err != nil {
		return err
	}
	m.mu.Lock()
	item = m.handles[key]
	item.aborted = true
	m.handles[key] = item
	m.mu.Unlock()
	return nil
}

func request(handle domain.MoverHandle) HandleRequest {
	return HandleRequest{Opaque: handle.Opaque(), Tenant: handle.Tenant(), Destination: handle.Destination(), ManifestID: handle.ManifestID()}
}
