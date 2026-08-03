package admission

import (
	"errors"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

var (
	ErrQuotaExceeded  = errors.New("admission quota exceeded")
	ErrFeatureDenied  = errors.New("admission feature denied")
	ErrPriorityDenied = errors.New("admission priority denied")
	ErrDeadlineDenied = errors.New("admission deadline denied")
	ErrOverloaded     = errors.New("admission overloaded")
	ErrInvalidPolicy  = errors.New("invalid admission policy")
)

// TenantPolicy is an immutable-by-convention input snapshot supplied by the
// authoritative scheduler transaction. Policy performs no reads or mutation.
type TenantPolicy struct {
	Tenant              domain.TenantScope
	AllowedFeatures     domain.FeatureSet
	MaxPriority         domain.Priority
	MaxActiveSlots      uint32
	MaxOutstandingSlots uint32
	MinExecutionBudget  time.Duration
	MaxExecutionBudget  time.Duration
	OverloadLimit       uint32
}

// UsageSnapshot is a bounded, point-in-time hint. PostgreSQL must recheck the
// same counters transactionally when materializing the claim.
type UsageSnapshot struct {
	Tenant           domain.TenantScope
	ActiveSlots      uint32
	OutstandingSlots uint32
	Overload         uint32
}

type AdmissionClaim struct {
	tenant   domain.TenantScope
	slots    uint32
	priority domain.Priority
	features domain.FeatureSet
	budget   time.Duration
}

func (c AdmissionClaim) Tenant() domain.TenantScope     { return c.tenant }
func (c AdmissionClaim) Slots() uint32                  { return c.slots }
func (c AdmissionClaim) Priority() domain.Priority      { return c.priority }
func (c AdmissionClaim) Features() domain.FeatureSet    { return c.features }
func (c AdmissionClaim) ExecutionBudget() time.Duration { return c.budget }

type Policy struct{}

func (Policy) Evaluate(req domain.ScheduleCommand, tenant TenantPolicy, usage UsageSnapshot) (AdmissionClaim, error) {
	if tenant.Tenant.Value() == "" || tenant.Tenant != usage.Tenant || req.TenantScope() != tenant.Tenant || !tenant.AllowedFeatures.Valid() || tenant.MaxPriority < domain.PriorityBackground || tenant.MaxPriority > domain.PriorityHigh || tenant.MaxActiveSlots == 0 || tenant.MaxOutstandingSlots == 0 || tenant.MinExecutionBudget <= 0 || tenant.MaxExecutionBudget < tenant.MinExecutionBudget || tenant.MaxExecutionBudget > 30*time.Minute {
		return AdmissionClaim{}, ErrInvalidPolicy
	}
	if !tenant.AllowedFeatures.Contains(req.Features()) {
		return AdmissionClaim{}, ErrFeatureDenied
	}
	// Priority values are ordered from least to most privileged.
	// ScheduleCommand is already authorized, but admission independently checks
	// the tenant ceiling so a stale/malformed caller cannot bypass it.
	priority := inferPriority(req)
	if priority > tenant.MaxPriority {
		return AdmissionClaim{}, ErrPriorityDenied
	}
	if req.ExecutionBudget() < tenant.MinExecutionBudget || req.ExecutionBudget() > tenant.MaxExecutionBudget {
		return AdmissionClaim{}, ErrDeadlineDenied
	}
	if req.SlotCost() > tenant.MaxActiveSlots || usage.ActiveSlots > tenant.MaxActiveSlots-req.SlotCost() || req.SlotCost() > tenant.MaxOutstandingSlots || usage.OutstandingSlots > tenant.MaxOutstandingSlots-req.SlotCost() {
		return AdmissionClaim{}, ErrQuotaExceeded
	}
	if tenant.OverloadLimit != 0 && usage.Overload >= tenant.OverloadLimit {
		return AdmissionClaim{}, ErrOverloaded
	}
	return AdmissionClaim{tenant: tenant.Tenant, slots: req.SlotCost(), priority: priority, features: req.Features(), budget: req.ExecutionBudget()}, nil
}

// ScheduleCommand currently carries feature/capacity semantics but not a
// separate priority field; authorization has already checked priority. Keep
// this explicit conservative value until the command contract gains it.
func inferPriority(domain.ScheduleCommand) domain.Priority { return domain.PriorityNormal }
