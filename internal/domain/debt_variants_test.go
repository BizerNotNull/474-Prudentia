package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestProviderTerminationDebtResolutionBindsExactEvidence(t *testing.T) {
	identity := debtTestIdentity(t)
	params := ProviderTerminationProofParams{
		RequestID:                        "request-secret-1",
		ReservationID:                    "reservation-secret-1",
		Identity:                         identity,
		ManifestID:                       "manifest-secret-1",
		AcknowledgementSequence:          17,
		AuthenticatedAcknowledgementHash: sha256.Sum256([]byte("authenticated-ack-secret")),
	}
	proof, err := NewProviderTerminationProof(params)
	if err != nil {
		t.Fatalf("NewProviderTerminationProof: %v", err)
	}
	duplicate, err := NewProviderTerminationProof(params)
	if err != nil {
		t.Fatalf("duplicate NewProviderTerminationProof: %v", err)
	}
	if proof.EvidenceHash() != duplicate.EvidenceHash() {
		t.Fatal("identical provider termination evidence produced unstable hashes")
	}
	if proof.RequestID() != params.RequestID || proof.ReservationID() != params.ReservationID || proof.Identity() != params.Identity || proof.ManifestID() != params.ManifestID || proof.AcknowledgementSequence() != params.AcknowledgementSequence {
		t.Fatalf("provider proof binding mismatch: %v", proof)
	}

	resolution, err := NewProviderTerminationDebtResolution("debt-secret-1", params.ReservationID, proof)
	if err != nil {
		t.Fatalf("NewProviderTerminationDebtResolution: %v", err)
	}
	got, ok := resolution.ProviderTerminationProof()
	if !ok || got != proof || resolution.Kind() != DebtResolutionProviderTermination {
		t.Fatalf("provider resolution binding mismatch: %v", resolution)
	}
	if _, ok := resolution.IdentityGoneProof(); ok {
		t.Fatal("provider resolution exposed identity-gone proof")
	}
	if _, err := NewProviderTerminationDebtResolution("debt-secret-1", "wrong-reservation", proof); !errors.Is(err, ErrInvalidDebtEvidence) {
		t.Fatalf("wrong reservation error = %v, want ErrInvalidDebtEvidence", err)
	}

	changed := params
	changed.AcknowledgementSequence++
	other, err := NewProviderTerminationProof(changed)
	if err != nil {
		t.Fatalf("changed NewProviderTerminationProof: %v", err)
	}
	if proof.EvidenceHash() == other.EvidenceHash() {
		t.Fatal("acknowledgement sequence was not bound into evidence hash")
	}
}

func TestProviderTerminationProofRejectsPartialEvidence(t *testing.T) {
	valid := ProviderTerminationProofParams{
		RequestID:                        "request-1",
		ReservationID:                    "reservation-1",
		Identity:                         debtTestIdentity(t),
		ManifestID:                       "manifest-1",
		AcknowledgementSequence:          1,
		AuthenticatedAcknowledgementHash: sha256.Sum256([]byte("ack")),
	}
	tests := map[string]func(*ProviderTerminationProofParams){
		"request":        func(p *ProviderTerminationProofParams) { p.RequestID = "" },
		"reservation":    func(p *ProviderTerminationProofParams) { p.ReservationID = "" },
		"identity":       func(p *ProviderTerminationProofParams) { p.Identity = WorkloadIdentity{} },
		"manifest":       func(p *ProviderTerminationProofParams) { p.ManifestID = "" },
		"sequence":       func(p *ProviderTerminationProofParams) { p.AcknowledgementSequence = 0 },
		"authentication": func(p *ProviderTerminationProofParams) { p.AuthenticatedAcknowledgementHash = [sha256.Size]byte{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			params := valid
			mutate(&params)
			if _, err := NewProviderTerminationProof(params); !errors.Is(err, ErrInvalidDebtEvidence) {
				t.Fatalf("error = %v, want ErrInvalidDebtEvidence", err)
			}
		})
	}
}

func TestDebtValuesRedactStringOutput(t *testing.T) {
	identity := debtTestIdentity(t)
	provider, err := NewProviderTerminationProof(ProviderTerminationProofParams{
		RequestID: "request-secret", ReservationID: "reservation-secret", Identity: identity,
		ManifestID: "manifest-secret", AcknowledgementSequence: 1,
		AuthenticatedAcknowledgementHash: sha256.Sum256([]byte("ack-secret")),
	})
	if err != nil {
		t.Fatal(err)
	}
	resolution, err := NewProviderTerminationDebtResolution("debt-secret", "reservation-secret", provider)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{
		"provider proof": provider,
		"resolution":     resolution,
	} {
		output := fmt.Sprintf("%v %+v", value, value)
		for _, secret := range []string{"request-secret", "reservation-secret", "manifest-secret", "debt-secret", "ack-secret"} {
			if strings.Contains(output, secret) {
				t.Fatalf("%s output leaked %q: %q", name, secret, output)
			}
		}
	}
}
