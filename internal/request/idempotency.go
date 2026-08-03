package request

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"sort"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

const maxIdempotencyKeyBytes = 256

var (
	lookupDomain = []byte("prudentia/idempotency-lookup/v1")
	digestDomain = []byte("prudentia/request-digest/v1")
)

type VersionedKey struct {
	Version uint32
	Key     []byte
}
type LookupPepperKeyring []VersionedKey
type DigestKeyring []VersionedKey

type IdempotencyConfig struct {
	LookupKeys         []VersionedKey
	LookupWriteVersion uint32
	DigestKeys         []VersionedKey
	DigestWriteVersion uint32
}

type idempotencyDeriver struct {
	lookupKeys         []VersionedKey
	lookupWriteVersion uint32
	digestKeys         []VersionedKey
	digestWriteVersion uint32
}

func newIdempotencyDeriver(config IdempotencyConfig) (idempotencyDeriver, error) {
	validated, err := ValidateIdempotencyConfig(config)
	if err != nil {
		return idempotencyDeriver{}, err
	}
	return idempotencyDeriver{
		lookupKeys: validated.LookupKeys, lookupWriteVersion: validated.LookupWriteVersion,
		digestKeys: validated.DigestKeys, digestWriteVersion: validated.DigestWriteVersion,
	}, nil
}

func ValidateIdempotencyConfig(config IdempotencyConfig) (IdempotencyConfig, error) {
	lookup, err := validatedKeys(config.LookupKeys, config.LookupWriteVersion, domain.MaxLookupCandidates)
	if err != nil {
		return IdempotencyConfig{}, err
	}
	digests, err := validatedKeys(config.DigestKeys, config.DigestWriteVersion, domain.MaxDigestCandidates)
	if err != nil {
		return IdempotencyConfig{}, err
	}
	return IdempotencyConfig{
		LookupKeys: lookup, LookupWriteVersion: config.LookupWriteVersion,
		DigestKeys: digests, DigestWriteVersion: config.DigestWriteVersion,
	}, nil
}

func validatedKeys(input []VersionedKey, writeVersion uint32, maxCandidates int) ([]VersionedKey, error) {
	if len(input) == 0 || len(input) > maxCandidates || writeVersion == 0 {
		return nil, errors.New("invalid idempotency keyring")
	}
	keys := make([]VersionedKey, len(input))
	for i, item := range input {
		if item.Version == 0 || len(item.Key) != sha256.Size {
			return nil, errors.New("invalid idempotency keyring")
		}
		keys[i] = VersionedKey{Version: item.Version, Key: append([]byte(nil), item.Key...)}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Version < keys[j].Version })
	writeFound := false
	for i, item := range keys {
		if i > 0 && keys[i-1].Version == item.Version {
			return nil, errors.New("invalid idempotency keyring")
		}
		writeFound = writeFound || item.Version == writeVersion
	}
	if !writeFound {
		return nil, errors.New("invalid idempotency keyring")
	}
	return keys, nil
}

func (d idempotencyDeriver) derive(authorized domain.AuthorizedRequest, rawKey []byte) ([]domain.IdempotencyLookupCandidate, []domain.RequestDigestCandidate, error) {
	defer clear(rawKey)
	var lookups []domain.IdempotencyLookupCandidate
	if len(rawKey) != 0 {
		if len(rawKey) > maxIdempotencyKeyBytes {
			return nil, nil, domain.NewPublicError(domain.ErrorInvalidRequest)
		}
		for _, value := range rawKey {
			if value < 0x21 || value > 0x7e {
				return nil, nil, domain.NewPublicError(domain.ErrorInvalidRequest)
			}
		}
		var err error
		lookups, err = deriveLookupCandidates(authorized.TenantScope(), rawKey, d.lookupKeys)
		if err != nil {
			return nil, nil, err
		}
	}
	digests, err := deriveDigests(authorized, d.digestKeys)
	if err != nil {
		return nil, nil, err
	}
	return lookups, digests, nil
}

// IdempotencyLookupCandidates derives tenant-scoped lookup values and clears
// the temporary plaintext obtained from key before returning.
func IdempotencyLookupCandidates(tenant domain.TenantScope, key domain.SecretString, peppers LookupPepperKeyring, write domain.LookupPepperVersion) (domain.LookupCandidateSet, error) {
	validated, err := validatedKeys([]VersionedKey(peppers), uint32(write), domain.MaxLookupCandidates)
	if err != nil {
		return domain.LookupCandidateSet{}, err
	}
	raw := key.Bytes()
	defer clear(raw)
	if len(raw) == 0 || len(raw) > maxIdempotencyKeyBytes {
		return domain.LookupCandidateSet{}, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	for _, value := range raw {
		if value < 0x21 || value > 0x7e {
			return domain.LookupCandidateSet{}, domain.NewPublicError(domain.ErrorInvalidRequest)
		}
	}
	candidates, err := deriveLookupCandidates(tenant, raw, validated)
	if err != nil {
		return domain.LookupCandidateSet{}, err
	}
	return domain.NewLookupCandidateSet(candidates, write)
}

// CanonicalDigests returns a digest for every readable key version regardless
// of whether the public request supplied an idempotency key.
func CanonicalDigests(authorized domain.AuthorizedRequest, keys DigestKeyring, write domain.DigestVersion) (domain.DigestSet, error) {
	validated, err := validatedKeys([]VersionedKey(keys), uint32(write), domain.MaxDigestCandidates)
	if err != nil {
		return domain.DigestSet{}, err
	}
	candidates, err := deriveDigests(authorized, validated)
	if err != nil {
		return domain.DigestSet{}, err
	}
	return domain.NewDigestSet(candidates, write)
}

func deriveLookupCandidates(tenant domain.TenantScope, raw []byte, keys []VersionedKey) ([]domain.IdempotencyLookupCandidate, error) {
	lookups := make([]domain.IdempotencyLookupCandidate, 0, len(keys))
	for _, item := range keys {
		mac := hmac.New(sha256.New, item.Key)
		_, _ = mac.Write(lookupDomain)
		_, _ = mac.Write([]byte(tenant.Value()))
		_, _ = mac.Write([]byte{0})
		_, _ = mac.Write(raw)
		candidate, err := domain.NewIdempotencyLookupCandidate(item.Version, mac.Sum(nil))
		if err != nil {
			return nil, err
		}
		lookups = append(lookups, candidate)
	}
	return lookups, nil
}

func deriveDigests(authorized domain.AuthorizedRequest, keys []VersionedKey) ([]domain.RequestDigestCandidate, error) {
	digests := make([]domain.RequestDigestCandidate, 0, len(keys))
	for _, item := range keys {
		mac := hmac.New(sha256.New, item.Key)
		writeCanonicalRequest(mac, authorized)
		candidate, err := domain.NewRequestDigestCandidate(item.Version, mac.Sum(nil))
		if err != nil {
			return nil, err
		}
		digests = append(digests, candidate)
	}
	return digests, nil
}

func writeCanonicalRequest(target hash.Hash, authorized domain.AuthorizedRequest) {
	_, _ = target.Write(digestDomain)
	writeField(target, []byte(authorized.Tenant()))
	request := authorized.Request()
	writeField(target, []byte(request.Model()))
	writeUint32(target, request.MaxCompletionTokens())
	writeUint32(target, uint32(request.Priority()))
	writeUint32(target, uint32(request.CachePolicy()))
	writeUint32(target, uint32(request.Features().Version()))
	writeUint64(target, request.Features().Bits())
	writeUint64(target, uint64(request.ExecutionBudget()))
	messages := request.Messages()
	writeUint32(target, uint32(len(messages)))
	for _, message := range messages {
		writeField(target, []byte(message.Role()))
		writeField(target, []byte(message.Content()))
	}
	extensions := request.Input().Extensions()
	names := make([]string, 0, len(extensions))
	for name := range extensions {
		names = append(names, name)
	}
	sort.Strings(names)
	writeUint32(target, uint32(len(names)))
	for _, name := range names {
		writeField(target, []byte(name))
		writeField(target, []byte(extensions[name]))
	}
}

func writeField(target hash.Hash, value []byte) {
	writeUint32(target, uint32(len(value)))
	_, _ = target.Write(value)
}

func writeUint32(target hash.Hash, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = target.Write(encoded[:])
}

func writeUint64(target hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = target.Write(encoded[:])
}
