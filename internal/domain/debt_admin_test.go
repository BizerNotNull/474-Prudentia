package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestUnsafeDebtOverrideRequiresPrivilegedInputs(t *testing.T) {
	principal, err := NewAdminPrincipal(sha256.Sum256([]byte("authenticated-actor-secret")))
	if err != nil {
		t.Fatalf("NewAdminPrincipal: %v", err)
	}
	valid := UnsafeDebtOverrideParams{
		DebtID:           "debt-secret-1",
		ExpectedIdentity: debtTestIdentity(t),
		Principal:        principal,
		Confirmation:     UnsafeDebtOverrideDangerPhrase,
		Ticket:           "TICKET-secret-1",
		Reason:           "operator determined bounded capacity must be restored",
	}
	command, err := NewUnsafeDebtOverride(valid)
	if err != nil {
		t.Fatalf("NewUnsafeDebtOverride: %v", err)
	}
	if command.DebtID() != valid.DebtID || command.ExpectedIdentity() != valid.ExpectedIdentity || command.Principal() != principal || command.Confirmation() != UnsafeDebtOverrideDangerPhrase || command.Ticket() != valid.Ticket || command.Reason() != valid.Reason {
		t.Fatalf("unsafe override binding mismatch: %v", command)
	}
	if command.Action() != AdminActionCapacityDebtUnsafeOverride || command.Action().String() != "capacity_debt.unsafe_override" {
		t.Fatalf("unsafe override action = %q", command.Action())
	}

	tests := map[string]func(*UnsafeDebtOverrideParams){
		"debt":         func(p *UnsafeDebtOverrideParams) { p.DebtID = "" },
		"identity":     func(p *UnsafeDebtOverrideParams) { p.ExpectedIdentity = WorkloadIdentity{} },
		"principal":    func(p *UnsafeDebtOverrideParams) { p.Principal = AdminPrincipal{} },
		"confirmation": func(p *UnsafeDebtOverrideParams) { p.Confirmation = "true" },
		"ticket":       func(p *UnsafeDebtOverrideParams) { p.Ticket = "" },
		"reason":       func(p *UnsafeDebtOverrideParams) { p.Reason = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			params := valid
			mutate(&params)
			if _, err := NewUnsafeDebtOverride(params); !errors.Is(err, ErrInvalidUnsafeDebtOverride) {
				t.Fatalf("error = %v, want ErrInvalidUnsafeDebtOverride", err)
			}
		})
	}
}

func TestUnsafeDebtOverrideAuditEventIsStableImmutableValue(t *testing.T) {
	principal, err := NewAdminPrincipal(sha256.Sum256([]byte("authenticated-actor-secret")))
	if err != nil {
		t.Fatal(err)
	}
	command, err := NewUnsafeDebtOverride(UnsafeDebtOverrideParams{
		DebtID: "debt-secret-1", ExpectedIdentity: debtTestIdentity(t), Principal: principal,
		Confirmation: UnsafeDebtOverrideDangerPhrase, Ticket: "TICKET-secret-1", Reason: "approved emergency action",
	})
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, time.August, 3, 12, 30, 0, 123, time.FixedZone("private-zone", 2*60*60))
	event, err := NewUnsafeDebtOverrideAuditEvent(command, when)
	if err != nil {
		t.Fatalf("NewUnsafeDebtOverrideAuditEvent: %v", err)
	}
	duplicate, err := NewUnsafeDebtOverrideAuditEvent(command, when)
	if err != nil {
		t.Fatal(err)
	}
	if event.Type() != DebtAuditEventCapacityDebtUnsafeOverridden || event.PrincipalHash() != principal.IdentityHash() || event.Ticket() != command.Ticket() || event.Reason() != command.Reason() || event.EventHash() != duplicate.EventHash() {
		t.Fatalf("audit event binding mismatch: %v", event)
	}
	if event.DebtHash() == [sha256.Size]byte{} || event.IdentityHash() == [sha256.Size]byte{} || event.OccurredAt().Location() != time.UTC {
		t.Fatalf("audit target/time binding mismatch: %v", event)
	}
	if _, err := NewUnsafeDebtOverrideAuditEvent(command, time.Time{}); !errors.Is(err, ErrInvalidDebtAuditEvent) {
		t.Fatalf("zero time error = %v, want ErrInvalidDebtAuditEvent", err)
	}
}

func TestDebtAdminValuesRedactStringOutput(t *testing.T) {
	principal, err := NewAdminPrincipal(sha256.Sum256([]byte("authenticated-actor-secret")))
	if err != nil {
		t.Fatal(err)
	}
	command, err := NewUnsafeDebtOverride(UnsafeDebtOverrideParams{
		DebtID: "debt-secret", ExpectedIdentity: debtTestIdentity(t), Principal: principal,
		Confirmation: UnsafeDebtOverrideDangerPhrase, Ticket: "ticket-secret", Reason: "reason-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewUnsafeDebtOverrideAuditEvent(command, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{"principal": principal, "command": command, "event": event} {
		output := fmt.Sprintf("%v %+v", value, value)
		for _, secret := range []string{"authenticated-actor-secret", "debt-secret", "ticket-secret", "reason-secret", "pod-secret"} {
			if strings.Contains(output, secret) {
				t.Fatalf("%s output leaked %q: %q", name, secret, output)
			}
		}
	}
}
