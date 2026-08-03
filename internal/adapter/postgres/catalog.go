package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CapabilityAAD is the immutable database identity of a reservation capability.
// Changing any field makes an encrypted capability undecryptable.
type CapabilityAAD struct {
	ReservationID string
	Generation    uint64
	OwnerAttempt  [sha256.Size]byte
	Identity      domain.WorkloadIdentity
}

func (a CapabilityAAD) bytes() ([]byte, error) {
	if a.ReservationID == "" || a.Generation == 0 || a.Identity.PodUID() == "" {
		return nil, errors.New("invalid capability associated data")
	}
	values := []string{a.ReservationID, a.Identity.Cluster(), a.Identity.Namespace(), a.Identity.LogicalEngine(), a.Identity.PodUID()}
	n := 8 + len(a.OwnerAttempt)
	for _, value := range values {
		if len(value) > 65535 {
			return nil, errors.New("capability associated data is too large")
		}
		n += 2 + len(value)
	}
	n += 16
	out := make([]byte, 0, n)
	for _, value := range values {
		var size [2]byte
		binary.BigEndian.PutUint16(size[:], uint16(len(value)))
		out = append(out, size[:]...)
		out = append(out, value...)
	}
	var numbers [24]byte
	binary.BigEndian.PutUint64(numbers[0:8], a.Generation)
	binary.BigEndian.PutUint64(numbers[8:16], a.Identity.EndpointEpoch())
	binary.BigEndian.PutUint64(numbers[16:24], a.Identity.RecoveryEpoch())
	out = append(out, numbers[:]...)
	out = append(out, a.OwnerAttempt[:]...)
	return out, nil
}

// SealedCapability contains only persistence-safe envelope material.
type SealedCapability struct {
	Algorithm         string
	KEKVersion        uint32
	ComparisonVersion uint32
	WrappedDataKey    []byte
	Nonce             []byte
	Ciphertext        []byte
	ComparisonHash    [sha256.Size]byte
}

// CapabilityKeyring provides coordinated write versions and retained read
// versions. Implementations may use a remote KMS; callers never persist a
// plaintext capability or data key.
type CapabilityKeyring interface {
	Seal(context.Context, []byte, CapabilityAAD, uint32, uint32) (SealedCapability, error)
	Open(context.Context, SealedCapability, CapabilityAAD) ([]byte, error)
	Matches(context.Context, []byte, SealedCapability, CapabilityAAD) (bool, error)
	CanRead(kekVersion, comparisonVersion uint32) bool
}

type localCapabilityKeyring struct {
	keks        map[uint32][32]byte
	comparisons map[uint32][32]byte
}

// NewLocalCapabilityKeyring is a deterministic local envelope implementation
// for deployments which inject already-unwrapped KEKs. Maps are defensively
// copied and all listed versions are retained for reads.
func NewLocalCapabilityKeyring(keks, comparisons map[uint32][]byte) (CapabilityKeyring, error) {
	if len(keks) == 0 || len(comparisons) == 0 {
		return nil, errors.New("empty capability keyring")
	}
	result := &localCapabilityKeyring{keks: make(map[uint32][32]byte, len(keks)), comparisons: make(map[uint32][32]byte, len(comparisons))}
	for version, key := range keks {
		if version == 0 || len(key) != 32 {
			return nil, errors.New("invalid capability KEK")
		}
		var copied [32]byte
		copy(copied[:], key)
		result.keks[version] = copied
	}
	for version, key := range comparisons {
		if version == 0 || len(key) != 32 {
			return nil, errors.New("invalid capability comparison key")
		}
		var copied [32]byte
		copy(copied[:], key)
		result.comparisons[version] = copied
	}
	return result, nil
}

func (k *localCapabilityKeyring) CanRead(kekVersion, comparisonVersion uint32) bool {
	_, kekOK := k.keks[kekVersion]
	_, comparisonOK := k.comparisons[comparisonVersion]
	return kekOK && comparisonOK
}

func (k *localCapabilityKeyring) Seal(_ context.Context, plaintext []byte, aad CapabilityAAD, kekVersion, comparisonVersion uint32) (SealedCapability, error) {
	kek, kekOK := k.keks[kekVersion]
	comparison, comparisonOK := k.comparisons[comparisonVersion]
	if !kekOK || !comparisonOK || len(plaintext) != 32 {
		return SealedCapability{}, errors.New("unavailable capability write version")
	}
	associated, err := aad.bytes()
	if err != nil {
		return SealedCapability{}, err
	}
	var dataKey [32]byte
	if _, err := rand.Read(dataKey[:]); err != nil {
		return SealedCapability{}, fmt.Errorf("generate capability data key: %w", err)
	}
	defer clear(dataKey[:])
	dataBlock, _ := aes.NewCipher(dataKey[:])
	dataAEAD, _ := cipher.NewGCM(dataBlock)
	nonce := make([]byte, dataAEAD.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return SealedCapability{}, fmt.Errorf("generate capability nonce: %w", err)
	}
	ciphertext := dataAEAD.Seal(nil, nonce, plaintext, associated)
	kekBlock, _ := aes.NewCipher(kek[:])
	kekAEAD, _ := cipher.NewGCM(kekBlock)
	wrapNonce := make([]byte, kekAEAD.NonceSize())
	if _, err := rand.Read(wrapNonce); err != nil {
		return SealedCapability{}, fmt.Errorf("generate key-wrap nonce: %w", err)
	}
	wrapped := append(wrapNonce, kekAEAD.Seal(nil, wrapNonce, dataKey[:], associated)...)
	mac := hmac.New(sha256.New, comparison[:])
	_, _ = mac.Write(associated)
	_, _ = mac.Write(plaintext)
	var comparisonHash [sha256.Size]byte
	copy(comparisonHash[:], mac.Sum(nil))
	return SealedCapability{Algorithm: "aes_256_gcm_v1", KEKVersion: kekVersion, ComparisonVersion: comparisonVersion, WrappedDataKey: wrapped, Nonce: nonce, Ciphertext: ciphertext, ComparisonHash: comparisonHash}, nil
}

func (k *localCapabilityKeyring) Open(_ context.Context, sealed SealedCapability, aad CapabilityAAD) ([]byte, error) {
	kek, ok := k.keks[sealed.KEKVersion]
	if !ok || sealed.Algorithm != "aes_256_gcm_v1" {
		return nil, errors.New("unavailable capability read version")
	}
	associated, err := aad.bytes()
	if err != nil {
		return nil, err
	}
	kekBlock, _ := aes.NewCipher(kek[:])
	kekAEAD, _ := cipher.NewGCM(kekBlock)
	if len(sealed.WrappedDataKey) <= kekAEAD.NonceSize() {
		return nil, errors.New("invalid wrapped capability key")
	}
	wrapNonce := sealed.WrappedDataKey[:kekAEAD.NonceSize()]
	dataKey, err := kekAEAD.Open(nil, wrapNonce, sealed.WrappedDataKey[kekAEAD.NonceSize():], associated)
	if err != nil {
		return nil, errors.New("capability associated data mismatch")
	}
	defer clear(dataKey)
	dataBlock, _ := aes.NewCipher(dataKey)
	dataAEAD, _ := cipher.NewGCM(dataBlock)
	if len(sealed.Nonce) != dataAEAD.NonceSize() {
		return nil, errors.New("invalid capability nonce")
	}
	plaintext, err := dataAEAD.Open(nil, sealed.Nonce, sealed.Ciphertext, associated)
	if err != nil {
		return nil, errors.New("capability associated data mismatch")
	}
	return plaintext, nil
}

func (k *localCapabilityKeyring) Matches(_ context.Context, plaintext []byte, sealed SealedCapability, aad CapabilityAAD) (bool, error) {
	key, ok := k.comparisons[sealed.ComparisonVersion]
	if !ok {
		return false, errors.New("unavailable capability comparison version")
	}
	associated, err := aad.bytes()
	if err != nil {
		return false, err
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(associated)
	_, _ = mac.Write(plaintext)
	return hmac.Equal(mac.Sum(nil), sealed.ComparisonHash[:]), nil
}

// Catalog is the sole PostgreSQL ledger/catalog authority. SchedulerStore and
// ControllerCatalog are facets over this object, never independent stores.
type Catalog struct {
	pool    *pgxpool.Pool
	keyring CapabilityKeyring
	aead    cipher.AEAD
}

func NewCatalog(pool *pgxpool.Pool, keyring CapabilityKeyring) (*Catalog, error) {
	if pool == nil || keyring == nil {
		return nil, errors.New("invalid catalog configuration")
	}
	return &Catalog{pool: pool, keyring: keyring}, nil
}

func (c *Catalog) SchedulerStore() *SchedulerStore       { return &SchedulerStore{Catalog: c} }
func (c *Catalog) ControllerCatalog() *ControllerCatalog { return &ControllerCatalog{Catalog: c} }
