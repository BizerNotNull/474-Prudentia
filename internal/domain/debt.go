package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
)

var (
	ErrCapacityDebtNotFound = errors.New("capacity debt not found")
	ErrCapacityDebtConflict = errors.New("capacity debt resolution conflict")
	ErrInvalidDebtEvidence  = errors.New("invalid capacity debt evidence")
)

const (
	identityGoneProofDomain        = "prudentia/identity-gone-proof/v2"
	providerTerminationProofDomain = "prudentia/provider-termination-proof/v1"
)

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
	writeHashString(digest, identityGoneProofDomain)
	writeHashUint64(digest, uint64(p.WriterGeneration))
	writeWorkloadIdentityHash(digest, p.Identity)
	_, _ = digest.Write(p.PodAbsenceEvidenceHash[:])
	_, _ = digest.Write(p.EndpointWithdrawalEvidenceHash[:])
	_, _ = digest.Write(p.ExecutionFenceEvidenceHash[:])

	return IdentityGoneProof{
		writerGeneration: p.WriterGeneration,
		identity:         p.Identity,
		evidenceHash:     sumHash(digest),
	}, nil
}

func (p IdentityGoneProof) WriterGeneration() WriterGeneration { return p.writerGeneration }
func (p IdentityGoneProof) Identity() WorkloadIdentity         { return p.identity }
func (p IdentityGoneProof) EvidenceHash() [sha256.Size]byte    { return p.evidenceHash }
func (p IdentityGoneProof) String() string                     { return "identity-gone-proof[redacted]" }

type ProviderTerminationProofParams struct {
	RequestID                        string
	ReservationID                    string
	Identity                         WorkloadIdentity
	ManifestID                       string
	AcknowledgementSequence          uint64
	AuthenticatedAcknowledgementHash [sha256.Size]byte
}

type ProviderTerminationProof struct {
	requestID               string
	reservationID           string
	identity                WorkloadIdentity
	manifestID              string
	acknowledgementSequence uint64
	evidenceHash            [sha256.Size]byte
}

func NewProviderTerminationProof(p ProviderTerminationProofParams) (ProviderTerminationProof, error) {
	if !boundedToken(p.RequestID, 128) || !boundedToken(p.ReservationID, 256) || isZeroWorkloadIdentity(p.Identity) || !boundedToken(p.ManifestID, 256) || p.AcknowledgementSequence == 0 || isZeroHash(p.AuthenticatedAcknowledgementHash) {
		return ProviderTerminationProof{}, ErrInvalidDebtEvidence
	}

	digest := sha256.New()
	writeHashString(digest, providerTerminationProofDomain)
	writeHashString(digest, p.RequestID)
	writeHashString(digest, p.ReservationID)
	writeWorkloadIdentityHash(digest, p.Identity)
	writeHashString(digest, p.ManifestID)
	writeHashUint64(digest, p.AcknowledgementSequence)
	_, _ = digest.Write(p.AuthenticatedAcknowledgementHash[:])

	return ProviderTerminationProof{
		requestID:               p.RequestID,
		reservationID:           p.ReservationID,
		identity:                p.Identity,
		manifestID:              p.ManifestID,
		acknowledgementSequence: p.AcknowledgementSequence,
		evidenceHash:            sumHash(digest),
	}, nil
}

func (p ProviderTerminationProof) RequestID() string               { return p.requestID }
func (p ProviderTerminationProof) ReservationID() string           { return p.reservationID }
func (p ProviderTerminationProof) Identity() WorkloadIdentity      { return p.identity }
func (p ProviderTerminationProof) ManifestID() string              { return p.manifestID }
func (p ProviderTerminationProof) AcknowledgementSequence() uint64 { return p.acknowledgementSequence }
func (p ProviderTerminationProof) EvidenceHash() [sha256.Size]byte { return p.evidenceHash }
func (p ProviderTerminationProof) String() string                  { return "provider-termination-proof[redacted]" }

type DebtResolutionKind uint8

const (
	DebtResolutionProviderTermination DebtResolutionKind = iota + 1
	DebtResolutionIdentityGone
)

func (k DebtResolutionKind) String() string {
	switch k {
	case DebtResolutionProviderTermination:
		return "provider_termination"
	case DebtResolutionIdentityGone:
		return "identity_gone"
	default:
		return "invalid"
	}
}

type DebtResolution struct {
	debtID                   string
	reservationID            string
	kind                     DebtResolutionKind
	identityGoneProof        IdentityGoneProof
	providerTerminationProof ProviderTerminationProof
}

func NewIdentityGoneDebtResolution(debtID, reservationID string, proof IdentityGoneProof) (DebtResolution, error) {
	if !validDebtResolutionReference(debtID, reservationID) || !validIdentityGoneProof(proof) {
		return DebtResolution{}, ErrInvalidDebtEvidence
	}
	return DebtResolution{debtID: debtID, reservationID: reservationID, kind: DebtResolutionIdentityGone, identityGoneProof: proof}, nil
}

func NewProviderTerminationDebtResolution(debtID, reservationID string, proof ProviderTerminationProof) (DebtResolution, error) {
	if !validDebtResolutionReference(debtID, reservationID) || !validProviderTerminationProof(proof) || proof.reservationID != reservationID {
		return DebtResolution{}, ErrInvalidDebtEvidence
	}
	return DebtResolution{debtID: debtID, reservationID: reservationID, kind: DebtResolutionProviderTermination, providerTerminationProof: proof}, nil
}

func (r DebtResolution) DebtID() string           { return r.debtID }
func (r DebtResolution) ReservationID() string    { return r.reservationID }
func (r DebtResolution) Kind() DebtResolutionKind { return r.kind }

func (r DebtResolution) IdentityGoneProof() (IdentityGoneProof, bool) {
	return r.identityGoneProof, r.kind == DebtResolutionIdentityGone && validIdentityGoneProof(r.identityGoneProof)
}

func (r DebtResolution) ProviderTerminationProof() (ProviderTerminationProof, bool) {
	return r.providerTerminationProof, r.kind == DebtResolutionProviderTermination && validProviderTerminationProof(r.providerTerminationProof)
}

// Proof preserves the identity-gone accessor used by existing callers. New code
// should use IdentityGoneProof and check its presence bit.
func (r DebtResolution) Proof() IdentityGoneProof { return r.identityGoneProof }
func (r DebtResolution) String() string           { return "debt-resolution[redacted]" }

func validDebtResolutionReference(debtID, reservationID string) bool {
	return boundedToken(debtID, 256) && boundedToken(reservationID, 256)
}

func validIdentityGoneProof(proof IdentityGoneProof) bool {
	return proof.writerGeneration != 0 && !isZeroWorkloadIdentity(proof.identity) && !isZeroHash(proof.evidenceHash)
}

func validProviderTerminationProof(proof ProviderTerminationProof) bool {
	return boundedToken(proof.requestID, 128) && boundedToken(proof.reservationID, 256) && !isZeroWorkloadIdentity(proof.identity) && boundedToken(proof.manifestID, 256) && proof.acknowledgementSequence != 0 && !isZeroHash(proof.evidenceHash)
}

func isZeroWorkloadIdentity(identity WorkloadIdentity) bool {
	return identity.Cluster() == "" || identity.Namespace() == "" || identity.LogicalEngine() == "" || identity.PodUID() == "" || identity.EndpointEpoch() == 0 || identity.RecoveryEpoch() == 0
}

func isZeroHash(value [sha256.Size]byte) bool {
	return value == [sha256.Size]byte{}
}

func writeWorkloadIdentityHash(digest hash.Hash, identity WorkloadIdentity) {
	writeHashString(digest, identity.Cluster())
	writeHashString(digest, identity.Namespace())
	writeHashString(digest, identity.LogicalEngine())
	writeHashString(digest, identity.PodUID())
	writeHashUint64(digest, identity.EndpointEpoch())
	writeHashUint64(digest, identity.RecoveryEpoch())
}

func writeHashString(digest hash.Hash, value string) {
	writeHashUint64(digest, uint64(len(value)))
	_, _ = digest.Write([]byte(value))
}

func writeHashUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

func sumHash(digest hash.Hash) [sha256.Size]byte {
	var value [sha256.Size]byte
	copy(value[:], digest.Sum(nil))
	return value
}
