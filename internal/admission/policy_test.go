package admission_test

import (
	"errors"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/admission"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func TestPolicyPureBoundaryDecisions(t *testing.T) {
	tenant, _ := domain.NewTenantScope("tenant-a")
	features, _ := domain.NewFeatureSet(domain.FeatureVersion1, 1<<domain.FeatureStreaming)
	digest, _ := domain.NewRequestDigestCandidate(1, make([]byte, 32))
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{RequestID: "req_a", AttemptID: "att_a", Tenant: tenant.Value(), DigestCandidates: []domain.RequestDigestCandidate{digest}, DigestWriteVersion: 1, Model: "model-a", SlotCost: 1, Features: features, ExecutionBudget: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	policy := admission.TenantPolicy{Tenant: tenant, AllowedFeatures: features, MaxPriority: domain.PriorityNormal, MaxActiveSlots: 2, MaxOutstandingSlots: 3, MinExecutionBudget: time.Second, MaxExecutionBudget: 2 * time.Minute, OverloadLimit: 10}
	usage := admission.UsageSnapshot{Tenant: tenant, ActiveSlots: 1, OutstandingSlots: 2, Overload: 9}
	claim, err := (admission.Policy{}).Evaluate(command, policy, usage)
	if err != nil || claim.Slots() != 1 || claim.Tenant() != tenant {
		t.Fatalf("claim=%v err=%v", claim, err)
	}
	usage.ActiveSlots = 2
	if _, err := (admission.Policy{}).Evaluate(command, policy, usage); !errors.Is(err, admission.ErrQuotaExceeded) {
		t.Fatalf("quota error=%v", err)
	}
	usage.ActiveSlots = 1
	usage.Overload = 10
	if _, err := (admission.Policy{}).Evaluate(command, policy, usage); !errors.Is(err, admission.ErrOverloaded) {
		t.Fatalf("overload error=%v", err)
	}
}

func TestPolicyUsesPropagatedPriority(t *testing.T) {
	tenant, _ := domain.NewTenantScope("tenant-a")
	features := domain.EmptyFeatureSet()
	digest, _ := domain.NewRequestDigestCandidate(1, make([]byte, 32))
	command, err := domain.NewScheduleCommand(domain.ScheduleParams{
		RequestID: "req_high", AttemptID: "att_high", Tenant: tenant.Value(),
		DigestCandidates: []domain.RequestDigestCandidate{digest}, DigestWriteVersion: 1,
		Model: "model-a", SlotCost: 1, Features: features, Priority: domain.PriorityHigh,
		CachePolicy: domain.CachePolicyDisabled, ExecutionBudget: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := admission.TenantPolicy{
		Tenant: tenant, AllowedFeatures: features, MaxPriority: domain.PriorityNormal,
		MaxActiveSlots: 2, MaxOutstandingSlots: 2, MinExecutionBudget: time.Second, MaxExecutionBudget: 2 * time.Minute,
	}
	usage := admission.UsageSnapshot{Tenant: tenant}
	if _, err := (admission.Policy{}).Evaluate(command, policy, usage); !errors.Is(err, admission.ErrPriorityDenied) {
		t.Fatalf("priority error=%v", err)
	}
}
