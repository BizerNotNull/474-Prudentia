package registry

import (
	"context"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type fakeStore struct{ projectCalls, listCalls int }

func (f *fakeStore) ProjectSnapshot(context.Context, domain.WorkloadIdentity) (domain.InstanceSnapshot, error) {
	f.projectCalls++
	return domain.InstanceSnapshot{}, nil
}
func (f *fakeStore) ListCandidateSnapshots(context.Context, CandidateQuery) (domain.CandidateCatalog, error) {
	f.listCalls++
	return domain.CandidateCatalog{}, nil
}

func TestRegistryRejectsUnboundedQueryBeforeStore(t *testing.T) {
	store := new(fakeStore)
	value, err := New(store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.ListCandidates(context.Background(), CandidateQuery{Model: "model", Required: domain.EmptyFeatureSet(), Limit: MaxCandidates + 1})
	if err == nil {
		t.Fatal("unbounded query accepted")
	}
	if store.listCalls != 0 {
		t.Fatal("invalid query reached store")
	}
}
func TestRegistryDelegatesWithoutCaching(t *testing.T) {
	store := new(fakeStore)
	value, _ := New(store)
	query, _ := NewCandidateQuery("model", domain.EmptyFeatureSet(), 1)
	for range 2 {
		if _, err := value.ListCandidates(context.Background(), query); err != nil {
			t.Fatal(err)
		}
	}
	if store.listCalls != 2 {
		t.Fatalf("store calls = %d, want 2", store.listCalls)
	}
}
