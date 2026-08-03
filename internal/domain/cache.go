package domain

import (
	"crypto/subtle"
	"fmt"
	"time"
)

const MaxMoverHandleBytes = 512

type CacheIdentityParams struct {
	Tenant                                                               string
	HMACVersion                                                          uint32
	TenantHMAC                                                           []byte
	ProviderImageDigest, ProxyDigest, ManifestID, ProviderManifestDigest string
	ConnectorManifestDigest                                              string
	ManifestSchemaVersion, CapabilityVersion                             uint16
	ModelFingerprint, TokenizerFingerprint, ModelConfigDigest            string
	CacheFormatVersion, CacheContentVersion                              uint16
	AttentionBackend, DType, Quantization                                string
	TensorParallel, DataParallel                                         uint16
	GPUArchitecture, DriverVersion, CacheLayout                          string
	BlockSize                                                            uint32
}
type CacheIdentity struct {
	tenant                                                               string
	hmacVersion                                                          uint32
	tenantHMAC                                                           [32]byte
	providerImageDigest, proxyDigest, manifestID, providerManifestDigest string
	connectorManifestDigest                                              string
	manifestSchemaVersion, capabilityVersion                             uint16
	modelFingerprint, tokenizerFingerprint, modelConfigDigest            string
	cacheFormatVersion, cacheContentVersion                              uint16
	attentionBackend, dtype, quantization                                string
	tensorParallel, dataParallel                                         uint16
	gpuArchitecture, driverVersion, cacheLayout                          string
	blockSize                                                            uint32
}

func NewCacheIdentity(p CacheIdentityParams) (CacheIdentity, error) {
	if !boundedProviderString(p.Tenant, 128) || p.HMACVersion == 0 || len(p.TenantHMAC) != 32 || !validSHA256Digest(p.ProviderImageDigest) || !validSHA256Digest(p.ProxyDigest) || !boundedProviderString(p.ManifestID, 256) || !validSHA256Digest(p.ProviderManifestDigest) || !validSHA256Digest(p.ConnectorManifestDigest) || p.ManifestSchemaVersion != CurrentManifestSchemaVersion || p.CapabilityVersion != CurrentCapabilityVersion || !validSHA256Digest(p.ModelFingerprint) || !validSHA256Digest(p.TokenizerFingerprint) || !validSHA256Digest(p.ModelConfigDigest) || p.CacheFormatVersion == 0 || p.CacheContentVersion == 0 || !boundedProviderString(p.AttentionBackend, 128) || !boundedProviderString(p.DType, 64) || !boundedProviderString(p.Quantization, 64) || p.TensorParallel == 0 || p.DataParallel == 0 || !boundedProviderString(p.GPUArchitecture, 128) || !boundedProviderString(p.DriverVersion, 128) || !boundedProviderString(p.CacheLayout, 128) || p.BlockSize == 0 {
		return CacheIdentity{}, fmt.Errorf("invalid cache identity")
	}
	var h [32]byte
	copy(h[:], p.TenantHMAC)
	return CacheIdentity{p.Tenant, p.HMACVersion, h, p.ProviderImageDigest, p.ProxyDigest, p.ManifestID, p.ProviderManifestDigest, p.ConnectorManifestDigest, p.ManifestSchemaVersion, p.CapabilityVersion, p.ModelFingerprint, p.TokenizerFingerprint, p.ModelConfigDigest, p.CacheFormatVersion, p.CacheContentVersion, p.AttentionBackend, p.DType, p.Quantization, p.TensorParallel, p.DataParallel, p.GPUArchitecture, p.DriverVersion, p.CacheLayout, p.BlockSize}, nil
}
func (i CacheIdentity) Tenant() string                  { return i.tenant }
func (i CacheIdentity) HMACVersion() uint32             { return i.hmacVersion }
func (i CacheIdentity) TenantHMAC() [32]byte            { return i.tenantHMAC }
func (i CacheIdentity) ProviderImageDigest() string     { return i.providerImageDigest }
func (i CacheIdentity) ProxyDigest() string             { return i.proxyDigest }
func (i CacheIdentity) ManifestID() string              { return i.manifestID }
func (i CacheIdentity) ProviderManifestDigest() string  { return i.providerManifestDigest }
func (i CacheIdentity) ConnectorManifestDigest() string { return i.connectorManifestDigest }
func (i CacheIdentity) ManifestSchemaVersion() uint16   { return i.manifestSchemaVersion }
func (i CacheIdentity) CapabilityVersion() uint16       { return i.capabilityVersion }
func (i CacheIdentity) ModelFingerprint() string        { return i.modelFingerprint }
func (i CacheIdentity) TokenizerFingerprint() string    { return i.tokenizerFingerprint }
func (i CacheIdentity) ModelConfigDigest() string       { return i.modelConfigDigest }
func (i CacheIdentity) CacheFormatVersion() uint16      { return i.cacheFormatVersion }
func (i CacheIdentity) CacheContentVersion() uint16     { return i.cacheContentVersion }
func (i CacheIdentity) AttentionBackend() string        { return i.attentionBackend }
func (i CacheIdentity) DType() string                   { return i.dtype }
func (i CacheIdentity) Quantization() string            { return i.quantization }
func (i CacheIdentity) TensorParallel() uint16          { return i.tensorParallel }
func (i CacheIdentity) DataParallel() uint16            { return i.dataParallel }
func (i CacheIdentity) GPUArchitecture() string         { return i.gpuArchitecture }
func (i CacheIdentity) DriverVersion() string           { return i.driverVersion }
func (i CacheIdentity) CacheLayout() string             { return i.cacheLayout }
func (i CacheIdentity) BlockSize() uint32               { return i.blockSize }
func (i CacheIdentity) Compatible(o CacheIdentity) bool {
	hmacEqual := subtle.ConstantTimeCompare(i.tenantHMAC[:], o.tenantHMAC[:]) == 1
	return hmacEqual && i.tenant == o.tenant && i.hmacVersion == o.hmacVersion && i.providerImageDigest == o.providerImageDigest && i.proxyDigest == o.proxyDigest && i.manifestID == o.manifestID && i.providerManifestDigest == o.providerManifestDigest && i.connectorManifestDigest == o.connectorManifestDigest && i.manifestSchemaVersion == o.manifestSchemaVersion && i.capabilityVersion == o.capabilityVersion && i.modelFingerprint == o.modelFingerprint && i.tokenizerFingerprint == o.tokenizerFingerprint && i.modelConfigDigest == o.modelConfigDigest && i.cacheFormatVersion == o.cacheFormatVersion && i.cacheContentVersion == o.cacheContentVersion && i.attentionBackend == o.attentionBackend && i.dtype == o.dtype && i.quantization == o.quantization && i.tensorParallel == o.tensorParallel && i.dataParallel == o.dataParallel && i.gpuArchitecture == o.gpuArchitecture && i.driverVersion == o.driverVersion && i.cacheLayout == o.cacheLayout && i.blockSize == o.blockSize
}
func (i CacheIdentity) String() string { return "cache-identity[tenant-private]" }

type CacheHintKind uint8

const (
	CacheMiss CacheHintKind = iota + 1
	CacheHit
	CacheCatalog
)

type CacheHintParams struct {
	Identity  WorkloadIdentity
	Digest    [32]byte
	ExpiresAt time.Time
}

// CacheHint represents either a verified cache-compatibility hit or a
// provider-neutral catalog hint. Miss is the explicit empty variant.
type CacheHint struct {
	kind      CacheHintKind
	identity  CacheIdentity
	source    WorkloadIdentity
	digest    [32]byte
	expiresAt time.Time
}

func NewCacheMiss() CacheHint { return CacheHint{kind: CacheMiss} }
func NewCacheHit(want, found CacheIdentity, source WorkloadIdentity, expiresAt time.Time) (CacheHint, error) {
	if !want.Compatible(found) || source.PodUID() == "" || expiresAt.IsZero() {
		return CacheHint{}, fmt.Errorf("unverified cache hit")
	}
	return CacheHint{kind: CacheHit, identity: found, source: source, expiresAt: expiresAt}, nil
}
func NewCacheHint(p CacheHintParams) (CacheHint, error) {
	if !p.Identity.valid() || p.Digest == ([32]byte{}) || p.ExpiresAt.IsZero() {
		return CacheHint{}, fmt.Errorf("invalid cache hint")
	}
	return CacheHint{kind: CacheCatalog, source: p.Identity, digest: p.Digest, expiresAt: p.ExpiresAt}, nil
}
func (h CacheHint) Kind() CacheHintKind {
	if h.kind < CacheMiss || h.kind > CacheCatalog {
		return CacheMiss
	}
	return h.kind
}
func (h CacheHint) IsHit() bool                     { return h.kind == CacheHit }
func (h CacheHint) Identity() (CacheIdentity, bool) { return h.identity, h.kind == CacheHit }
func (h CacheHint) Source() (WorkloadIdentity, bool) {
	return h.source, h.kind == CacheHit || h.kind == CacheCatalog
}
func (h CacheHint) Digest() [32]byte { return h.digest }
func (h CacheHint) ExpiresAt() (time.Time, bool) {
	return h.expiresAt, h.kind == CacheHit || h.kind == CacheCatalog
}
func (h CacheHint) ValidFor(id CacheIdentity, at time.Time) bool {
	return h.kind == CacheHit && h.identity.Compatible(id) && !at.IsZero() && at.Before(h.expiresAt)
}

type CacheRequirement uint8

const (
	ColdAllowed CacheRequirement = iota + 1
	RequireCompatible
)

type CacheRequestParams struct {
	Tenant, RequestID string
	Identity          CacheIdentity
	Hint              CacheHint
	Requirement       CacheRequirement
	Budget            time.Duration
}
type CacheRequest struct {
	tenant, requestID string
	identity          CacheIdentity
	hint              CacheHint
	requirement       CacheRequirement
	budget            time.Duration
}

func NewCacheRequest(p CacheRequestParams) (CacheRequest, error) {
	if !boundedProviderString(p.Tenant, 128) || !boundedProviderString(p.RequestID, 256) || p.Identity.Tenant() != p.Tenant || p.Budget <= 0 || (p.Requirement != ColdAllowed && p.Requirement != RequireCompatible) || (p.Hint.IsHit() && !p.Hint.identity.Compatible(p.Identity)) {
		return CacheRequest{}, fmt.Errorf("invalid cache request")
	}
	return CacheRequest{p.Tenant, p.RequestID, p.Identity, p.Hint, p.Requirement, p.Budget}, nil
}
func (r CacheRequest) Tenant() string                { return r.tenant }
func (r CacheRequest) RequestID() string             { return r.requestID }
func (r CacheRequest) Identity() CacheIdentity       { return r.identity }
func (r CacheRequest) Hint() CacheHint               { return r.hint }
func (r CacheRequest) Requirement() CacheRequirement { return r.requirement }
func (r CacheRequest) Budget() time.Duration         { return r.budget }

type ReservedTarget struct {
	tenant        string
	identity      WorkloadIdentity
	manifest      CapabilityManifest
	cacheIdentity CacheIdentity
}

func NewReservedTarget(tenant string, identity WorkloadIdentity, manifest CapabilityManifest, cacheIdentity CacheIdentity) (ReservedTarget, error) {
	if !boundedProviderString(tenant, 128) || identity.PodUID() == "" || cacheIdentity.Tenant() != tenant || !manifest.Supports(CapabilityInference) || manifest.ImageDigest() != cacheIdentity.ProviderImageDigest() || manifest.ProxyDigest() != cacheIdentity.ProxyDigest() || manifest.ID() != cacheIdentity.ManifestID() || manifest.PayloadDigestString() != cacheIdentity.ProviderManifestDigest() {
		return ReservedTarget{}, fmt.Errorf("incompatible reserved target")
	}
	return ReservedTarget{tenant, identity, manifest, cacheIdentity}, nil
}
func (t ReservedTarget) Tenant() string               { return t.tenant }
func (t ReservedTarget) Identity() WorkloadIdentity   { return t.identity }
func (t ReservedTarget) Manifest() CapabilityManifest { return t.manifest }
func (t ReservedTarget) CacheIdentity() CacheIdentity { return t.cacheIdentity }

type CachePreparationKind uint8

const (
	CachePreparationCold CachePreparationKind = iota + 1
	CachePreparationLocalHit
	CachePreparationTransferred
)

type CachePreparation struct {
	kind          CachePreparationKind
	tenant        string
	target        WorkloadIdentity
	cacheIdentity CacheIdentity
}

func NewColdPreparation(req CacheRequest, target ReservedTarget) (CachePreparation, error) {
	if req.Tenant() != target.Tenant() {
		return CachePreparation{}, fmt.Errorf("incompatible cold preparation")
	}
	return CachePreparation{CachePreparationCold, req.Tenant(), target.Identity(), req.Identity()}, nil
}
func NewLocalHitPreparation(req CacheRequest, target ReservedTarget, at time.Time) (CachePreparation, error) {
	source, ok := req.Hint().Source()
	if !ok || !source.Equal(target.Identity()) || !req.Hint().ValidFor(req.Identity(), at) || !req.Identity().Compatible(target.CacheIdentity()) {
		return CachePreparation{}, fmt.Errorf("unverified local hit")
	}
	return CachePreparation{CachePreparationLocalHit, req.Tenant(), target.Identity(), req.Identity()}, nil
}
func NewTransferredPreparation(req CacheRequest, target ReservedTarget, status TransferStatus) (CachePreparation, error) {
	if status.State() != TransferComplete || status.Tenant() != req.Tenant() || !status.CacheIdentity().Compatible(req.Identity()) || !req.Identity().Compatible(target.CacheIdentity()) || !status.Destination().Equal(target.Identity()) {
		return CachePreparation{}, fmt.Errorf("unverified transfer")
	}
	return CachePreparation{CachePreparationTransferred, req.Tenant(), target.Identity(), req.Identity()}, nil
}
func (p CachePreparation) Kind() CachePreparationKind   { return p.kind }
func (p CachePreparation) Tenant() string               { return p.tenant }
func (p CachePreparation) Target() WorkloadIdentity     { return p.target }
func (p CachePreparation) CacheIdentity() CacheIdentity { return p.cacheIdentity }

type ConnectorManifestParams struct {
	Digest, ControlPlaneIdentity string
	SignatureVerified            bool
	ValidFrom, ValidUntil        time.Time
	MaxBytes                     uint64
	MaxOperation                 time.Duration
}

// ConnectorManifest is signed independently from the provider/proxy manifest.
// It identifies only the authenticated mover control plane; no data-plane
// address or descriptor is represented.
type ConnectorManifest struct {
	digest, controlPlaneIdentity string
	validFrom, validUntil        time.Time
	maxBytes                     uint64
	maxOperation                 time.Duration
}

func NewConnectorManifest(p ConnectorManifestParams) (ConnectorManifest, error) {
	if !validSHA256Digest(p.Digest) || !boundedProviderString(p.ControlPlaneIdentity, 512) || !p.SignatureVerified || p.ValidFrom.IsZero() || p.ValidUntil.IsZero() || p.ValidUntil.Before(p.ValidFrom) || p.MaxBytes == 0 || p.MaxOperation <= 0 {
		return ConnectorManifest{}, fmt.Errorf("invalid connector manifest")
	}
	return ConnectorManifest{p.Digest, p.ControlPlaneIdentity, p.ValidFrom, p.ValidUntil, p.MaxBytes, p.MaxOperation}, nil
}
func (m ConnectorManifest) Digest() string               { return m.digest }
func (m ConnectorManifest) ControlPlaneIdentity() string { return m.controlPlaneIdentity }
func (m ConnectorManifest) ValidAt(at time.Time) bool {
	return !at.IsZero() && !at.Before(m.validFrom) && !at.After(m.validUntil)
}
func (m ConnectorManifest) MaxBytes() uint64            { return m.maxBytes }
func (m ConnectorManifest) MaxOperation() time.Duration { return m.maxOperation }

type TransferSpecParams struct {
	Tenant, RequestID                   string
	CacheIdentity                       CacheIdentity
	Source, Destination                 WorkloadIdentity
	SourceManifest, DestinationManifest CapabilityManifest
	ConnectorManifest                   ConnectorManifest
	ExpiresAt                           time.Time
	MaxBytes                            uint64
}
type TransferSpec struct {
	tenant, requestID                   string
	cacheIdentity                       CacheIdentity
	source, destination                 WorkloadIdentity
	sourceManifest, destinationManifest CapabilityManifest
	connectorManifest                   ConnectorManifest
	expiresAt                           time.Time
	maxBytes                            uint64
}

func NewTransferSpec(p TransferSpecParams) (TransferSpec, error) {
	if !boundedProviderString(p.Tenant, 128) || !boundedProviderString(p.RequestID, 256) || p.CacheIdentity.Tenant() != p.Tenant || p.Source.PodUID() == "" || p.Destination.PodUID() == "" || p.Source.Equal(p.Destination) || !p.SourceManifest.Supports(CapabilityMover) || !p.DestinationManifest.Supports(CapabilityMover) || !p.SourceManifest.Compatible(p.DestinationManifest) || p.SourceManifest.PayloadDigestString() != p.CacheIdentity.ProviderManifestDigest() || p.ConnectorManifest.Digest() != p.CacheIdentity.ConnectorManifestDigest() || p.ExpiresAt.IsZero() || p.MaxBytes == 0 || p.MaxBytes > p.ConnectorManifest.MaxBytes() {
		return TransferSpec{}, fmt.Errorf("unsupported transfer")
	}
	return TransferSpec{p.Tenant, p.RequestID, p.CacheIdentity, p.Source, p.Destination, p.SourceManifest, p.DestinationManifest, p.ConnectorManifest, p.ExpiresAt, p.MaxBytes}, nil
}
func (s TransferSpec) Tenant() string                          { return s.tenant }
func (s TransferSpec) RequestID() string                       { return s.requestID }
func (s TransferSpec) CacheIdentity() CacheIdentity            { return s.cacheIdentity }
func (s TransferSpec) Source() WorkloadIdentity                { return s.source }
func (s TransferSpec) Destination() WorkloadIdentity           { return s.destination }
func (s TransferSpec) SourceManifest() CapabilityManifest      { return s.sourceManifest }
func (s TransferSpec) DestinationManifest() CapabilityManifest { return s.destinationManifest }
func (s TransferSpec) ConnectorManifest() ConnectorManifest    { return s.connectorManifest }
func (s TransferSpec) ExpiresAt() time.Time                    { return s.expiresAt }
func (s TransferSpec) MaxBytes() uint64                        { return s.maxBytes }

type MoverHandle struct {
	opaque                              []byte
	tenant, requestID                   string
	cacheIdentity                       CacheIdentity
	source, destination                 WorkloadIdentity
	manifestID, connectorManifestDigest string
	expiresAt                           time.Time
	maxBytes                            uint64
}

func NewMoverHandle(spec TransferSpec, opaque []byte) (MoverHandle, error) {
	if len(opaque) == 0 || len(opaque) > MaxMoverHandleBytes {
		return MoverHandle{}, fmt.Errorf("invalid mover handle")
	}
	return MoverHandle{append([]byte(nil), opaque...), spec.tenant, spec.requestID, spec.cacheIdentity, spec.source, spec.destination, spec.sourceManifest.ID(), spec.cacheIdentity.ConnectorManifestDigest(), spec.expiresAt, spec.maxBytes}, nil
}
func (h MoverHandle) Opaque() []byte                  { return append([]byte(nil), h.opaque...) }
func (h MoverHandle) Tenant() string                  { return h.tenant }
func (h MoverHandle) RequestID() string               { return h.requestID }
func (h MoverHandle) CacheIdentity() CacheIdentity    { return h.cacheIdentity }
func (h MoverHandle) Source() WorkloadIdentity        { return h.source }
func (h MoverHandle) Destination() WorkloadIdentity   { return h.destination }
func (h MoverHandle) ManifestID() string              { return h.manifestID }
func (h MoverHandle) ConnectorManifestDigest() string { return h.connectorManifestDigest }
func (h MoverHandle) ExpiresAt() time.Time            { return h.expiresAt }
func (h MoverHandle) MaxBytes() uint64                { return h.maxBytes }
func (h MoverHandle) String() string                  { return "mover-handle[redacted]" }

type TransferState uint8

const (
	TransferPending TransferState = iota + 1
	TransferRunning
	TransferComplete
	TransferFailed
	TransferAborted
)

type TransferStatusParams struct {
	Handle                     MoverHandle
	State                      TransferState
	Sequence, TransferredBytes uint64
	Previous                   TransferStatus
	HasPrevious                bool
}
type TransferStatus struct {
	tenant                     string
	cacheIdentity              CacheIdentity
	destination                WorkloadIdentity
	state                      TransferState
	sequence, transferredBytes uint64
}

func NewTransferStatus(p TransferStatusParams) (TransferStatus, error) {
	if len(p.Handle.opaque) == 0 || p.Sequence == 0 || !validTransferState(p.State) || p.TransferredBytes > p.Handle.maxBytes {
		return TransferStatus{}, fmt.Errorf("invalid transfer status")
	}
	next := TransferStatus{p.Handle.tenant, p.Handle.cacheIdentity, p.Handle.destination, p.State, p.Sequence, p.TransferredBytes}
	if p.HasPrevious && !p.Previous.CanAdvanceTo(next) {
		return TransferStatus{}, fmt.Errorf("non-monotonic transfer status")
	}
	if !p.HasPrevious && (p.Previous.state != 0 || p.Previous.sequence != 0) {
		return TransferStatus{}, fmt.Errorf("invalid previous status")
	}
	return next, nil
}
func (s TransferStatus) Tenant() string                { return s.tenant }
func (s TransferStatus) CacheIdentity() CacheIdentity  { return s.cacheIdentity }
func (s TransferStatus) Destination() WorkloadIdentity { return s.destination }
func (s TransferStatus) State() TransferState          { return s.state }
func (s TransferStatus) Sequence() uint64              { return s.sequence }
func (s TransferStatus) TransferredBytes() uint64      { return s.transferredBytes }
func (s TransferStatus) CanAdvanceTo(n TransferStatus) bool {
	if s.tenant != n.tenant || !s.cacheIdentity.Compatible(n.cacheIdentity) || !s.destination.Equal(n.destination) || n.sequence <= s.sequence || n.transferredBytes < s.transferredBytes {
		return false
	}
	if s.state == TransferComplete || s.state == TransferFailed || s.state == TransferAborted {
		return false
	}
	switch s.state {
	case TransferPending:
		return n.state == TransferPending || n.state == TransferRunning || n.state == TransferComplete || n.state == TransferFailed || n.state == TransferAborted
	case TransferRunning:
		return n.state == TransferRunning || n.state == TransferComplete || n.state == TransferFailed || n.state == TransferAborted
	default:
		return false
	}
}
func validTransferState(s TransferState) bool { return s >= TransferPending && s <= TransferAborted }
