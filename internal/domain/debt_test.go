package domain

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
)

func TestIdentityGoneProof(t *testing.T) {
	identity := debtTestIdentity(t)
	h1 := sha256.Sum256([]byte("pod absent"))
	h2 := sha256.Sum256([]byte("endpoint withdrawn"))
	h3 := sha256.Sum256([]byte("execution fenced"))

	proof, err := NewIdentityGoneProof(IdentityGoneProofParams{
		WriterGeneration:               7,
		Identity:                       identity,
		PodAbsenceEvidenceHash:         h1,
		EndpointWithdrawalEvidenceHash: h2,
		ExecutionFenceEvidenceHash:     h3,
	})
	if err != nil {
		t.Fatalf("NewIdentityGoneProof: %v", err)
	}
	if proof.WriterGeneration() != 7 || proof.Identity() != identity {
		t.Fatalf("proof binding mismatch: generation=%d identity=%+v", proof.WriterGeneration(), proof.Identity())
	}
	combined := make([]byte, 0, len(identityGoneProofDomain)+3*sha256.Size)
	combined = append(combined, identityGoneProofDomain...)
	combined = append(combined, h1[:]...)
	combined = append(combined, h2[:]...)
	combined = append(combined, h3[:]...)
	if want := sha256.Sum256(combined); proof.EvidenceHash() != want {
		t.Fatalf("evidence hash = %x, want %x", proof.EvidenceHash(), want)
	}

	valid := IdentityGoneProofParams{
		WriterGeneration:               7,
		Identity:                       identity,
		PodAbsenceEvidenceHash:         h1,
		EndpointWithdrawalEvidenceHash: h2,
		ExecutionFenceEvidenceHash:     h3,
	}
	tests := map[string]func(*IdentityGoneProofParams){
		"zero generation":      func(p *IdentityGoneProofParams) { p.WriterGeneration = 0 },
		"zero identity":        func(p *IdentityGoneProofParams) { p.Identity = WorkloadIdentity{} },
		"zero pod absence":     func(p *IdentityGoneProofParams) { p.PodAbsenceEvidenceHash = [32]byte{} },
		"zero withdrawal":      func(p *IdentityGoneProofParams) { p.EndpointWithdrawalEvidenceHash = [32]byte{} },
		"zero execution fence": func(p *IdentityGoneProofParams) { p.ExecutionFenceEvidenceHash = [32]byte{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			params := valid
			mutate(&params)
			if _, err := NewIdentityGoneProof(params); !errors.Is(err, ErrInvalidDebtEvidence) {
				t.Fatalf("error = %v, want ErrInvalidDebtEvidence", err)
			}
		})
	}
}

func TestIdentityGoneDebtResolution(t *testing.T) {
	identity := debtTestIdentity(t)
	proof, err := NewIdentityGoneProof(IdentityGoneProofParams{
		WriterGeneration:               9,
		Identity:                       identity,
		PodAbsenceEvidenceHash:         sha256.Sum256([]byte("pod absent")),
		EndpointWithdrawalEvidenceHash: sha256.Sum256([]byte("endpoint withdrawn")),
		ExecutionFenceEvidenceHash:     sha256.Sum256([]byte("execution fenced")),
	})
	if err != nil {
		t.Fatalf("NewIdentityGoneProof: %v", err)
	}

	resolution, err := NewIdentityGoneDebtResolution("debt_reservation-1", "reservation-1", proof)
	if err != nil {
		t.Fatalf("NewIdentityGoneDebtResolution: %v", err)
	}
	if resolution.DebtID() != "debt_reservation-1" || resolution.ReservationID() != "reservation-1" || resolution.Proof() != proof {
		t.Fatalf("resolution binding mismatch: %+v", resolution)
	}

	for _, tc := range []struct {
		name          string
		debtID        string
		reservationID string
		proof         IdentityGoneProof
	}{
		{name: "empty debt", reservationID: "reservation-1", proof: proof},
		{name: "spaced debt", debtID: " debt", reservationID: "reservation-1", proof: proof},
		{name: "long debt", debtID: strings.Repeat("d", 257), reservationID: "reservation-1", proof: proof},
		{name: "empty reservation", debtID: "debt", proof: proof},
		{name: "spaced reservation", debtID: "debt", reservationID: "reservation ", proof: proof},
		{name: "long reservation", debtID: "debt", reservationID: strings.Repeat("r", 257), proof: proof},
		{name: "zero proof", debtID: "debt", reservationID: "reservation-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewIdentityGoneDebtResolution(tc.debtID, tc.reservationID, tc.proof); !errors.Is(err, ErrInvalidDebtEvidence) {
				t.Fatalf("error = %v, want ErrInvalidDebtEvidence", err)
			}
		})
	}
}

func TestWorkloadIdentityEqual(t *testing.T) {
	base := WorkloadIdentityParams{
		Cluster: "cluster-a", Namespace: "namespace-a", LogicalEngine: "engine-a",
		PodUID: "pod-uid-a", EndpointEpoch: 3, RecoveryEpoch: 4,
	}
	identity, err := NewWorkloadIdentity(base)
	if err != nil {
		t.Fatalf("NewWorkloadIdentity: %v", err)
	}
	same, err := NewWorkloadIdentity(base)
	if err != nil {
		t.Fatalf("NewWorkloadIdentity duplicate: %v", err)
	}
	if !identity.Equal(same) {
		t.Fatal("equal identities did not compare equal")
	}

	for name, mutate := range map[string]func(*WorkloadIdentityParams){
		"cluster":        func(p *WorkloadIdentityParams) { p.Cluster = "cluster-b" },
		"namespace":      func(p *WorkloadIdentityParams) { p.Namespace = "namespace-b" },
		"logical engine": func(p *WorkloadIdentityParams) { p.LogicalEngine = "engine-b" },
		"pod uid":        func(p *WorkloadIdentityParams) { p.PodUID = "pod-uid-b" },
		"endpoint epoch": func(p *WorkloadIdentityParams) { p.EndpointEpoch++ },
		"recovery epoch": func(p *WorkloadIdentityParams) { p.RecoveryEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			other, err := NewWorkloadIdentity(changed)
			if err != nil {
				t.Fatalf("NewWorkloadIdentity changed: %v", err)
			}
			if identity.Equal(other) {
				t.Fatal("different identities compared equal")
			}
		})
	}
}

func debtTestIdentity(t *testing.T) WorkloadIdentity {
	t.Helper()
	identity, err := NewWorkloadIdentity(WorkloadIdentityParams{
		Cluster: "cluster-a", Namespace: "namespace-a", LogicalEngine: "engine-a",
		PodUID: "pod-uid-a", EndpointEpoch: 3, RecoveryEpoch: 4,
	})
	if err != nil {
		t.Fatalf("NewWorkloadIdentity: %v", err)
	}
	return identity
}
