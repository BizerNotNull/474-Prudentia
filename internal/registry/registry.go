package registry

import (
	"context"
	"errors"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

const MaxCandidates = 1024

type CandidateQuery struct {
	Model    string
	Required domain.FeatureSet
	Limit    int
}

func NewCandidateQuery(model string, required domain.FeatureSet, limit int) (CandidateQuery, error) {
	if model == "" || len(model) > 512 || !required.Valid() || limit < 1 || limit > MaxCandidates {
		return CandidateQuery{}, errors.New("invalid candidate query")
	}
	return CandidateQuery{Model: model, Required: required, Limit: limit}, nil
}

type Store interface {
	ProjectSnapshot(context.Context, domain.WorkloadIdentity) (domain.InstanceSnapshot, error)
	ListCandidateSnapshots(context.Context, CandidateQuery) (domain.CandidateCatalog, error)
}

// Registry is a read-only application facade. PostgreSQL remains the sole
// authority; Registry owns no cache, clock, or observation state.
type Registry struct{ store Store }

func New(store Store) (*Registry, error) {
	if store == nil {
		return nil, errors.New("invalid registry store")
	}
	return &Registry{store: store}, nil
}
func (r *Registry) Project(ctx context.Context, id domain.WorkloadIdentity) (domain.InstanceSnapshot, error) {
	return r.store.ProjectSnapshot(ctx, id)
}
func (r *Registry) ListCandidates(ctx context.Context, q CandidateQuery) (domain.CandidateCatalog, error) {
	if _, err := NewCandidateQuery(q.Model, q.Required, q.Limit); err != nil {
		return domain.CandidateCatalog{}, err
	}
	return r.store.ListCandidateSnapshots(ctx, q)
}
