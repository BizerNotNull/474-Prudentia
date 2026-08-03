package domain

import (
	"crypto/sha256"
	"errors"
	"strings"
	"time"
)

var (
	ErrInvalidAdminPrincipal    = errors.New("invalid admin principal")
	ErrInvalidUnsafeDebtOverride = errors.New("invalid unsafe capacity debt override")
	ErrInvalidDebtAuditEvent    = errors.New("invalid capacity debt audit event")
)

type AdminAction string

const AdminActionCapacityDebtUnsafeOverride AdminAction = "capacity_debt.unsafe_override"

func (a AdminAction) String() string {
	if a == AdminActionCapacityDebtUnsafeOverride {
		return string(a)
	}
	return "invalid"
}

const UnsafeDebtOverrideDangerPhrase = "I UNDERSTAND THIS MAY RELEASE CAPACITY WHILE WORK IS STILL EXECUTING"

const (
	unsafeDebtOverrideTargetDomain = "prudentia/unsafe-debt-override-target/v1"
	unsafeDebtOverrideAuditDomain  = "prudentia/unsafe-debt-override-audit/v1"
)

type AdminPrincipal struct {
	identityHash [sha256.Size]byte
}

func NewAdminPrincipal(identityHash [sha256.Size]byte) (AdminPrincipal, error) {
	if isZeroHash(identityHash) {
		return AdminPrincipal{}, ErrInvalidAdminPrincipal
	}
	return AdminPrincipal{identityHash: identityHash}, nil
}

func (p AdminPrincipal) IdentityHash() [sha256.Size]byte { return p.identityHash }
func (p AdminPrincipal) String() string                  { return "admin-principal[redacted]" }

type UnsafeDebtOverrideParams struct {
	DebtID           string
	ExpectedIdentity WorkloadIdentity
	Principal        AdminPrincipal
	Confirmation     string
	Ticket           string
	Reason           string
}

type UnsafeDebtOverride struct {
	debtID           string
	expectedIdentity WorkloadIdentity
	principal        AdminPrincipal
	ticket           string
	reason           string
}

func NewUnsafeDebtOverride(p UnsafeDebtOverrideParams) (UnsafeDebtOverride, error) {
	if !boundedToken(p.DebtID, 256) || isZeroWorkloadIdentity(p.ExpectedIdentity) || isZeroHash(p.Principal.identityHash) || p.Confirmation != UnsafeDebtOverrideDangerPhrase || !boundedToken(p.Ticket, 256) || !boundedReason(p.Reason, 1024) {
		return UnsafeDebtOverride{}, ErrInvalidUnsafeDebtOverride
	}
	return UnsafeDebtOverride{
		debtID:           p.DebtID,
		expectedIdentity: p.ExpectedIdentity,
		principal:        p.Principal,
		ticket:           p.Ticket,
		reason:           p.Reason,
	}, nil
}

func (o UnsafeDebtOverride) DebtID() string                    { return o.debtID }
func (o UnsafeDebtOverride) ExpectedIdentity() WorkloadIdentity { return o.expectedIdentity }
func (o UnsafeDebtOverride) Principal() AdminPrincipal          { return o.principal }
func (o UnsafeDebtOverride) Action() AdminAction                 { return AdminActionCapacityDebtUnsafeOverride }
func (o UnsafeDebtOverride) Confirmation() string               { return UnsafeDebtOverrideDangerPhrase }
func (o UnsafeDebtOverride) Ticket() string                     { return o.ticket }
func (o UnsafeDebtOverride) Reason() string                     { return o.reason }
func (o UnsafeDebtOverride) String() string                     { return "unsafe-debt-override[redacted]" }

type DebtAuditEventType string

const DebtAuditEventCapacityDebtUnsafeOverridden DebtAuditEventType = "capacity_debt_unsafe_overridden"

func (t DebtAuditEventType) String() string {
	if t == DebtAuditEventCapacityDebtUnsafeOverridden {
		return string(t)
	}
	return "invalid"
}

type UnsafeDebtOverrideAuditEvent struct {
	principalHash [sha256.Size]byte
	debtHash      [sha256.Size]byte
	identityHash  [sha256.Size]byte
	ticket        string
	reason        string
	occurredAt    time.Time
	eventHash     [sha256.Size]byte
}

func NewUnsafeDebtOverrideAuditEvent(command UnsafeDebtOverride, occurredAt time.Time) (UnsafeDebtOverrideAuditEvent, error) {
	if !validUnsafeDebtOverride(command) || occurredAt.IsZero() {
		return UnsafeDebtOverrideAuditEvent{}, ErrInvalidDebtAuditEvent
	}

	debtDigest := sha256.New()
	writeHashString(debtDigest, unsafeDebtOverrideTargetDomain)
	writeHashString(debtDigest, command.debtID)
	debtHash := sumHash(debtDigest)

	identityDigest := sha256.New()
	writeHashString(identityDigest, unsafeDebtOverrideTargetDomain)
	writeWorkloadIdentityHash(identityDigest, command.expectedIdentity)
	identityHash := sumHash(identityDigest)

	stableTime := time.Unix(0, occurredAt.UnixNano()).UTC()
	eventDigest := sha256.New()
	writeHashString(eventDigest, unsafeDebtOverrideAuditDomain)
	_, _ = eventDigest.Write(command.principal.identityHash[:])
	_, _ = eventDigest.Write(debtHash[:])
	_, _ = eventDigest.Write(identityHash[:])
	writeHashString(eventDigest, command.ticket)
	writeHashString(eventDigest, command.reason)
	writeHashUint64(eventDigest, uint64(stableTime.UnixNano()))

	return UnsafeDebtOverrideAuditEvent{
		principalHash: command.principal.identityHash,
		debtHash:      debtHash,
		identityHash:  identityHash,
		ticket:        command.ticket,
		reason:        command.reason,
		occurredAt:    stableTime,
		eventHash:     sumHash(eventDigest),
	}, nil
}

func (e UnsafeDebtOverrideAuditEvent) Type() DebtAuditEventType {
	return DebtAuditEventCapacityDebtUnsafeOverridden
}
func (e UnsafeDebtOverrideAuditEvent) PrincipalHash() [sha256.Size]byte { return e.principalHash }
func (e UnsafeDebtOverrideAuditEvent) DebtHash() [sha256.Size]byte      { return e.debtHash }
func (e UnsafeDebtOverrideAuditEvent) IdentityHash() [sha256.Size]byte  { return e.identityHash }
func (e UnsafeDebtOverrideAuditEvent) Ticket() string                  { return e.ticket }
func (e UnsafeDebtOverrideAuditEvent) Reason() string                  { return e.reason }
func (e UnsafeDebtOverrideAuditEvent) OccurredAt() time.Time           { return e.occurredAt }
func (e UnsafeDebtOverrideAuditEvent) EventHash() [sha256.Size]byte     { return e.eventHash }
func (e UnsafeDebtOverrideAuditEvent) String() string                  { return "capacity-debt-audit-event[redacted]" }

func validUnsafeDebtOverride(command UnsafeDebtOverride) bool {
	return boundedToken(command.debtID, 256) && !isZeroWorkloadIdentity(command.expectedIdentity) && !isZeroHash(command.principal.identityHash) && boundedToken(command.ticket, 256) && boundedReason(command.reason, 1024)
}

func boundedReason(value string, max int) bool {
	return value != "" && len(value) <= max && value == strings.TrimSpace(value)
}
