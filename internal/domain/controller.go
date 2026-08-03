package domain

import (
	"errors"
	"fmt"
	"time"
)

var ErrStaleWriterGeneration = errors.New("stale controller writer generation")

type WriterGeneration uint64

func NewWriterGeneration(value uint64) (WriterGeneration, error) {
	if value == 0 {
		return 0, fmt.Errorf("invalid writer generation")
	}
	return WriterGeneration(value), nil
}

func (g WriterGeneration) Uint64() uint64 { return uint64(g) }

type ResourceKey struct {
	namespace string
	name      string
}

func NewResourceKey(namespace, name string) (ResourceKey, error) {
	if !boundedToken(namespace, 253) || !boundedToken(name, 253) {
		return ResourceKey{}, fmt.Errorf("invalid resource key")
	}
	return ResourceKey{namespace: namespace, name: name}, nil
}

func (k ResourceKey) Namespace() string { return k.namespace }
func (k ResourceKey) Name() string      { return k.name }
func (k ResourceKey) String() string    { return k.namespace + "/" + k.name }

type BackendProjectionParams struct {
	Identity        WorkloadIdentity
	Model           string
	Endpoint        string
	ConfiguredSlots uint32
	FreshFor        time.Duration
}

type BackendProjection struct {
	identity        WorkloadIdentity
	model           string
	endpoint        string
	configuredSlots uint32
	freshFor        time.Duration
}

func NewBackendProjection(p BackendProjectionParams) (BackendProjection, error) {
	if !boundedToken(p.Model, 256) || p.ConfiguredSlots == 0 || p.ConfiguredSlots > 1024 || p.FreshFor <= 0 || p.FreshFor > 10*time.Minute {
		return BackendProjection{}, fmt.Errorf("invalid backend projection")
	}
	target, err := NewDispatchTarget(p.Endpoint, p.Identity)
	if err != nil {
		return BackendProjection{}, err
	}
	return BackendProjection{
		identity: p.Identity, model: p.Model, endpoint: target.Endpoint(),
		configuredSlots: p.ConfiguredSlots, freshFor: p.FreshFor,
	}, nil
}

func (p BackendProjection) Identity() WorkloadIdentity { return p.identity }
func (p BackendProjection) Model() string              { return p.model }
func (p BackendProjection) Endpoint() string           { return p.endpoint }
func (p BackendProjection) ConfiguredSlots() uint32    { return p.configuredSlots }
func (p BackendProjection) FreshFor() time.Duration    { return p.freshFor }

type ResourceState struct {
	cluster     string
	key         ResourceKey
	projections []BackendProjection
}

func NewResourceState(cluster string, key ResourceKey, projections []BackendProjection) (ResourceState, error) {
	if !boundedToken(cluster, 128) {
		return ResourceState{}, fmt.Errorf("invalid resource cluster")
	}
	if len(projections) > 1024 {
		return ResourceState{}, fmt.Errorf("resource has too many backend projections")
	}
	cloned := make([]BackendProjection, len(projections))
	for i, projection := range projections {
		if projection.Identity().Cluster() != cluster {
			return ResourceState{}, fmt.Errorf("cross-cluster backend projection")
		}
		cloned[i] = projection
	}
	return ResourceState{cluster: cluster, key: key, projections: cloned}, nil
}

func (s ResourceState) Cluster() string  { return s.cluster }
func (s ResourceState) Key() ResourceKey { return s.key }
func (s ResourceState) Projections() []BackendProjection {
	return append([]BackendProjection(nil), s.projections...)
}
