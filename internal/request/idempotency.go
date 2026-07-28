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
	if len(rawKey) == 0 {
		return nil, nil, nil
	}
	defer clear(rawKey)
	if len(rawKey) > maxIdempotencyKeyBytes {
		return nil, nil, domain.NewPublicError(domain.ErrorInvalidRequest)
	}
	for _, value := range rawKey {
		if value < 0x21 || value > 0x7e {
			return nil, nil, domain.NewPublicError(domain.ErrorInvalidRequest)
		}
	}

	lookups := make([]domain.IdempotencyLookupCandidate, 0, len(d.lookupKeys))
	for _, item := range d.lookupKeys {
		mac := hmac.New(sha256.New, item.Key)
		_, _ = mac.Write(lookupDomain)
		writeField(mac, []byte(authorized.Tenant()))
		_, _ = mac.Write([]byte{0})
		_, _ = mac.Write(rawKey)
		candidate, err := domain.NewIdempotencyLookupCandidate(item.Version, mac.Sum(nil))
		if err != nil {
			return nil, nil, err
		}
		lookups = append(lookups, candidate)
	}

	digests := make([]domain.RequestDigestCandidate, 0, len(d.digestKeys))
	for _, item := range d.digestKeys {
		mac := hmac.New(sha256.New, item.Key)
		writeCanonicalRequest(mac, authorized)
		candidate, err := domain.NewRequestDigestCandidate(item.Version, mac.Sum(nil))
		if err != nil {
			return nil, nil, err
		}
		digests = append(digests, candidate)
	}
	return lookups, digests, nil
}

func writeCanonicalRequest(target hash.Hash, authorized domain.AuthorizedRequest) {
	_, _ = target.Write(digestDomain)
	writeField(target, []byte(authorized.Tenant()))
	request := authorized.Request()
	writeField(target, []byte(request.Model()))
	writeUint32(target, request.MaxCompletionTokens())
	messages := request.Messages()
	writeUint32(target, uint32(len(messages)))
	for _, message := range messages {
		writeField(target, []byte(message.Role()))
		writeField(target, []byte(message.Content()))
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
