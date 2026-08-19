package cache

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

var ErrCompatibleCacheRequired = errors.New("compatible cache required")

const (
	MaxRetainedHMACVersions = 8
	defaultMaxRecords       = 4096
	defaultMaxTTL           = 5 * time.Minute
)

var cacheIdentityDomain = []byte("prudentia/cache-identity/v1\x00")

type HMACKeyVersion struct {
	Version uint32
	Key     []byte
}

type HMACCandidate struct {
	Version uint32
	Digest  [32]byte
}

type IdentityKeyring struct {
	keys         []HMACKeyVersion
	writeVersion uint32
}

func NewIdentityKeyring(keys []HMACKeyVersion, writeVersion uint32) (IdentityKeyring, error) {
	if len(keys) == 0 || len(keys) > MaxRetainedHMACVersions || writeVersion == 0 {
		return IdentityKeyring{}, errors.New("invalid cache HMAC keyring")
	}
	cloned := make([]HMACKeyVersion, len(keys))
	foundWrite := false
	for i, item := range keys {
		if item.Version == 0 || len(item.Key) < 32 || len(item.Key) > 64 || (i > 0 && keys[i-1].Version >= item.Version) {
			return IdentityKeyring{}, errors.New("invalid cache HMAC keyring")
		}
		cloned[i] = HMACKeyVersion{Version: item.Version, Key: append([]byte(nil), item.Key...)}
		foundWrite = foundWrite || item.Version == writeVersion
	}
	if !foundWrite {
		return IdentityKeyring{}, errors.New("cache HMAC write version not retained")
	}
	return IdentityKeyring{keys: cloned, writeVersion: writeVersion}, nil
}

// Candidates always derives every retained version. Tenant length framing and a
// fixed protocol label prevent ambiguity or reuse in another HMAC domain.
func (k IdentityKeyring) Candidates(tenant string, canonicalPrefixDigest [32]byte) ([]HMACCandidate, error) {
	if tenant == "" || len(tenant) > 128 || canonicalPrefixDigest == ([32]byte{}) {
		return nil, errors.New("invalid cache identity input")
	}
	out := make([]HMACCandidate, len(k.keys))
	for i, item := range k.keys {
		mac := hmac.New(sha256.New, item.Key)
		_, _ = mac.Write(cacheIdentityDomain)
		_, _ = mac.Write([]byte{byte(len(tenant) >> 8), byte(len(tenant))})
		_, _ = mac.Write([]byte(tenant))
		_, _ = mac.Write(canonicalPrefixDigest[:])
		copy(out[i].Digest[:], mac.Sum(nil))
		out[i].Version = item.Version
	}
	return out, nil
}

func (k IdentityKeyring) WriteVersion() uint32 { return k.writeVersion }

func (k IdentityKeyring) WriteCandidate(candidates []HMACCandidate) (HMACCandidate, error) {
	var selected HMACCandidate
	found := 0
	for _, candidate := range candidates {
		match := subtle.ConstantTimeEq(int32(candidate.Version), int32(k.writeVersion))
		if match == 1 {
			selected = candidate
			found++
		}
	}
	if found != 1 {
		return HMACCandidate{}, errors.New("current cache HMAC candidate missing")
	}
	return selected, nil
}

type metadataKey struct {
	tenant  string
	version uint32
	hmac    [32]byte
}

type record struct {
	identity domain.CacheIdentity
	source   domain.WorkloadIdentity
	manifest domain.CapabilityManifest
	expires  time.Time
}

// Metadata is a bounded authoritative adapter. Only authenticated,
// request-specific records can enter it; it has deliberately no aggregate
// utilization ingestion API because aggregate metrics are never locality.
type Metadata struct {
	mu         sync.RWMutex
	records    map[metadataKey]record
	now        func() time.Time
	maxTTL     time.Duration
	maxRecords int
}

func NewMetadata(now func() time.Time) (*Metadata, error) {
	return NewMetadataWithLimits(now, defaultMaxTTL, defaultMaxRecords)
}

func NewMetadataWithLimits(now func() time.Time, maxTTL time.Duration, maxRecords int) (*Metadata, error) {
	if now == nil || maxTTL <= 0 || maxRecords <= 0 {
		return nil, errors.New("invalid cache metadata configuration")
	}
	return &Metadata{records: make(map[metadataKey]record), now: now, maxTTL: maxTTL, maxRecords: maxRecords}, nil
}

func (m *Metadata) Lookup(ctx context.Context, id domain.CacheIdentity) (domain.CacheHint, error) {
	return m.LookupCandidates(ctx, []domain.CacheIdentity{id})
}

// LookupCandidates checks every retained version without early exit. Missing,
// expired, incomplete, corrupt, unavailable, or canceled metadata is a miss.
func (m *Metadata) LookupCandidates(ctx context.Context, candidates []domain.CacheIdentity) (domain.CacheHint, error) {
	if len(candidates) == 0 || len(candidates) > MaxRetainedHMACVersions || ctx.Err() != nil {
		return domain.NewCacheMiss(), nil
	}
	m.mu.RLock()
	var matches [MaxRetainedHMACVersions]record
	var present [MaxRetainedHMACVersions]bool
	for i, candidate := range candidates {
		matches[i], present[i] = m.records[keyFor(candidate)]
	}
	m.mu.RUnlock()
	now := m.now()
	var selected domain.CacheHint
	found := false
	for i, candidate := range candidates {
		record := matches[i]
		valid := present[i] && now.Before(record.expires) && record.identity.Compatible(candidate) && record.manifest.ValidAt(now) && record.manifest.PayloadDigestString() == candidate.ProviderManifestDigest()
		if valid {
			hint, err := domain.NewCacheHit(candidate, record.identity, record.source, record.expires)
			if err == nil && !found {
				selected, found = hint, true
			}
		}
	}
	if !found {
		return domain.NewCacheMiss(), nil
	}
	return selected, nil
}

func (m *Metadata) RecordVerified(id domain.CacheIdentity, source domain.WorkloadIdentity, manifest domain.CapabilityManifest, expires time.Time) error {
	now := m.now()
	if source.PodUID() == "" || !now.Before(expires) || expires.Sub(now) > m.maxTTL || !manifest.ValidAt(now) || expires.After(manifest.ValidUntil()) || manifest.PayloadDigestString() != id.ProviderManifestDigest() || manifest.ID() != id.ManifestID() || manifest.ImageDigest() != id.ProviderImageDigest() || manifest.ProxyDigest() != id.ProxyDigest() {
		return errors.New("invalid verified cache metadata")
	}
	key := keyFor(id)
	m.mu.Lock()
	defer m.mu.Unlock()
	for existing, item := range m.records {
		if !now.Before(item.expires) {
			delete(m.records, existing)
		}
	}
	if _, exists := m.records[key]; !exists && len(m.records) >= m.maxRecords {
		return errors.New("cache metadata capacity exceeded")
	}
	m.records[key] = record{id, source, manifest, expires}
	return nil
}
func keyFor(id domain.CacheIdentity) metadataKey {
	return metadataKey{tenant: id.Tenant(), version: id.HMACVersion(), hmac: id.TenantHMAC()}
}

type Mover interface {
	Start(context.Context, domain.TransferSpec) (domain.MoverHandle, error)
	Status(context.Context, domain.MoverHandle) (domain.TransferStatus, error)
	Commit(context.Context, domain.MoverHandle) error
	Abort(context.Context, domain.MoverHandle) error
}

type ManifestResolver interface {
	ResolveManifest(context.Context, domain.WorkloadIdentity) (domain.CapabilityManifest, error)
}

type Coordinator struct {
	mover     Mover
	resolver  ManifestResolver
	connector domain.ConnectorManifest
	now       func() time.Time
	poll      time.Duration
	maxBytes  uint64
}

func NewCoordinator(mover Mover, resolver ManifestResolver, connector domain.ConnectorManifest, now func() time.Time, poll time.Duration, maxBytes uint64) (*Coordinator, error) {
	if now == nil || poll <= 0 || maxBytes == 0 || maxBytes > connector.MaxBytes() || mover == nil || resolver == nil {
		return nil, errors.New("invalid cache coordinator configuration")
	}
	return &Coordinator{mover: mover, resolver: resolver, connector: connector, now: now, poll: poll, maxBytes: maxBytes}, nil
}

// NewColdCoordinator constructs the fail-closed baseline used when no signed
// mover capability is configured. Prepare still honors local verified hits,
// but never attempts a transfer.
func NewColdCoordinator(now func() time.Time) (*Coordinator, error) {
	if now == nil {
		return nil, errors.New("invalid cache coordinator configuration")
	}
	return &Coordinator{now: now}, nil
}

func (c *Coordinator) Prepare(ctx context.Context, req domain.CacheRequest, target domain.ReservedTarget) (domain.CachePreparation, error) {
	cold := func() (domain.CachePreparation, error) {
		if req.Requirement() == domain.RequireCompatible {
			return domain.CachePreparation{}, ErrCompatibleCacheRequired
		}
		return domain.NewColdPreparation(req, target)
	}
	now := c.now()
	if req.Tenant() != target.Tenant() || !req.Identity().Compatible(target.CacheIdentity()) {
		return cold()
	}
	if req.Hint().ValidFor(req.Identity(), now) {
		if source, ok := req.Hint().Source(); ok && source.Equal(target.Identity()) {
			return domain.NewLocalHitPreparation(req, target, now)
		}
	}
	source, ok := req.Hint().Source()
	if !ok || !req.Hint().ValidFor(req.Identity(), now) || c.mover == nil || c.resolver == nil || !target.Manifest().Supports(domain.CapabilityMover) || !c.connector.ValidAt(now) {
		return cold()
	}
	deadline := now.Add(req.Budget())
	if expiry, ok := req.Hint().ExpiresAt(); ok && expiry.Before(deadline) {
		deadline = expiry
	}
	if connectorDeadline := now.Add(c.connector.MaxOperation()); connectorDeadline.Before(deadline) {
		deadline = connectorDeadline
	}
	opCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	sourceManifest, err := c.resolver.ResolveManifest(opCtx, source)
	if err != nil || !sourceManifest.ValidAt(now) || !sourceManifest.Compatible(target.Manifest()) {
		return cold()
	}
	spec, err := domain.NewTransferSpec(domain.TransferSpecParams{Tenant: req.Tenant(), RequestID: req.RequestID(), CacheIdentity: req.Identity(), Source: source, Destination: target.Identity(), SourceManifest: sourceManifest, DestinationManifest: target.Manifest(), ConnectorManifest: c.connector, ExpiresAt: deadline, MaxBytes: c.maxBytes})
	if err != nil {
		return cold()
	}
	handle, err := c.mover.Start(opCtx, spec)
	if err != nil {
		return cold()
	}
	committed := false
	defer func() {
		if !committed {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), c.poll)
			defer stop()
			_ = c.mover.Abort(cleanup, handle)
		}
	}()
	timer := time.NewTimer(c.poll)
	defer timer.Stop()
	var previous domain.TransferStatus
	hasPrevious := false
	for {
		status, statusErr := c.mover.Status(opCtx, handle)
		if statusErr != nil || status.TransferredBytes() > c.maxBytes || (hasPrevious && !previous.CanAdvanceTo(status)) {
			return cold()
		}
		switch status.State() {
		case domain.TransferComplete:
			liveDestination, resolveErr := c.resolver.ResolveManifest(opCtx, target.Identity())
			if resolveErr != nil || !liveDestination.ValidAt(c.now()) || !liveDestination.Compatible(target.Manifest()) {
				return cold()
			}
			if err = c.mover.Commit(opCtx, handle); err != nil {
				return cold()
			}
			committed = true
			return domain.NewTransferredPreparation(req, target, status)
		case domain.TransferFailed, domain.TransferAborted:
			return cold()
		}
		previous, hasPrevious = status, true
		select {
		case <-opCtx.Done():
			return cold()
		case <-timer.C:
			timer.Reset(c.poll)
		}
	}
}

// SortCandidates provides a stable version order at transport/storage boundaries.
func SortCandidates(candidates []HMACCandidate) {
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Version < candidates[j].Version })
}
