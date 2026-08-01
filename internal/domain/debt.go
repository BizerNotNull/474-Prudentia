package domain

import (
	"crypto/sha256"
	"errors"
)

var (
	ErrCapacityDebtNotFound = errors.New("capacity debt not found")
	ErrCapacityDebtConflict = errors.New("capacity debt resolution conflict")
	ErrInvalidDebtEvidence  = errors.New("invalid capacity debt evidence")
)

const identityGoneProofDomain = "prudentia/identity-gone-proof/v1"

type IdentityGoneProofParams struct {
	WriterGeneration               WriterGeneration
	Identity                       WorkloadIdentity
	PodAbsenceEvidenceHash         [sha256.Size]byte
	EndpointWithdrawalEvidenceHash [sha256.Size]byte
	ExecutionFenceEvidenceHash     [sha256.Size]byte
}

type IdentityGoneProof struct {
	writerGeneration WriterGeneration
	identity         WorkloadIdentity
	evidenceHash     [sha256.Size]byte
}

func NewIdentityGoneProof(p IdentityGoneProofParams) (IdentityGoneProof, error) {
	if p.WriterGeneration == 0 || isZeroWorkloadIdentity(p.Identity) || isZeroHash(p.PodAbsenceEvidenceHash) || isZeroHash(p.EndpointWithdrawalEvidenceHash) || isZeroHash(p.ExecutionFenceEvidenceHash) {
		return IdentityGoneProof{}, ErrInvalidDebtEvidence
	}

	digest := sha256.New()
	_, _ = digest.Write([]byte(identityGoneProofDomain))
	_, _ = digest.Write(p.PodAbsenceEvidenceHash[:])
	_, _ = digest.Write(p.EndpointWithdrawalEvidenceHash[:])
	_, _ = digest.Write(p.ExecutionFenceEvidenceHash[:])
	var evidenceHash [sha256.Size]byte
	copy(evidenceHash[:], digest.Sum(nil))

	return IdentityGoneProof{
		writerGeneration: p.WriterGeneration,
		identity:         p.Identity,
		evidenceHash:     evidenceHash,
	}, nil
}

func (p IdentityGoneProof) WriterGeneration() WriterGeneration { return p.writerGeneration }
func (p IdentityGoneProof) Identity() WorkloadIdentity         { return p.identity }
func (p IdentityGoneProof) EvidenceHash() [sha256.Size]byte    { return p.evidenceHash }

type DebtResolution struct {
	debtID        string
	reservationID string
	proof         IdentityGoneProof
}

func NewIdentityGoneDebtResolution(debtID, reservationID string, proof IdentityGoneProof) (DebtResolution, error) {
	if !boundedToken(debtID, 256) || !boundedToken(reservationID, 256) || proof.writerGeneration == 0 || isZeroWorkloadIdentity(proof.identity) || isZeroHash(proof.evidenceHash) {
		return DebtResolution{}, ErrInvalidDebtEvidence
	}
	return DebtResolution{debtID: debtID, reservationID: reservationID, proof: proof}, nil
}

func (r DebtResolution) DebtID() string           { return r.debtID }
func (r DebtResolution) ReservationID() string    { return r.reservationID }
func (r DebtResolution) Proof() IdentityGoneProof { return r.proof }

func isZeroWorkloadIdentity(identity WorkloadIdentity) bool {
	return identity.Cluster() == "" || identity.Namespace() == "" || identity.LogicalEngine() == "" || identity.PodUID() == "" || identity.EndpointEpoch() == 0 || identity.RecoveryEpoch() == 0
}

func isZeroHash(value [sha256.Size]byte) bool {
	return value == [sha256.Size]byte{}
}
