package scheduling_test

import (
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/scheduling"
)

func identity(t *testing.T, pod string) domain.WorkloadIdentity {
	t.Helper()
	value, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster-a", Namespace: "inference", LogicalEngine: "engine-a", PodUID: pod, EndpointEpoch: 1, RecoveryEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestRankFiltersCapacityAndUsesStableIdentityTieBreak(t *testing.T) {
	podA := scheduling.Candidate{Identity: identity(t, "pod-a"), AvailableSlots: 3}
	podB := scheduling.Candidate{Identity: identity(t, "pod-b"), AvailableSlots: 3}
	insufficient := scheduling.Candidate{Identity: identity(t, "pod-c"), AvailableSlots: 1}

	first := scheduling.Rank([]scheduling.Candidate{podB, insufficient, podA}, 2)
	second := scheduling.Rank([]scheduling.Candidate{podA, podB, insufficient}, 2)
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("ranked lengths = %d and %d, want 2", len(first), len(second))
	}
	for i := range first {
		if first[i].Identity.PodUID() != second[i].Identity.PodUID() {
			t.Fatalf("ranking changed with input permutation")
		}
	}
	if first[0].Identity.PodUID() != "pod-a" || first[1].Identity.PodUID() != "pod-b" {
		t.Fatalf("unexpected tie order: %s, %s", first[0].Identity.PodUID(), first[1].Identity.PodUID())
	}
}
