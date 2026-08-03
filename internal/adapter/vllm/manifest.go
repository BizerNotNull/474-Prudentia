package vllm

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

// SignedManifest is the deployment-provided capability document. Payload is
// retained only by the caller; Verify returns the validated, immutable domain form.
type SignedManifest struct {
	KeyID     string
	Payload   []byte
	Signature []byte
}

type ManifestVerifier struct {
	keys map[string]ed25519.PublicKey
	pins map[string][32]byte
	now  func() time.Time
}

func NewManifestVerifier(keys map[string]ed25519.PublicKey, pins map[string]string, now func() time.Time) (*ManifestVerifier, error) {
	if len(keys) == 0 || len(pins) == 0 || now == nil {
		return nil, errors.New("invalid manifest verifier configuration")
	}
	v := &ManifestVerifier{keys: make(map[string]ed25519.PublicKey, len(keys)), pins: make(map[string][32]byte, len(pins)), now: now}
	for id, key := range keys {
		if id == "" || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("invalid manifest verification key")
		}
		v.keys[id] = append(ed25519.PublicKey(nil), key...)
	}
	for id, text := range pins {
		decoded, err := hex.DecodeString(text)
		if id == "" || err != nil || len(decoded) != sha256.Size {
			return nil, errors.New("invalid manifest pin")
		}
		var pin [32]byte
		copy(pin[:], decoded)
		v.pins[id] = pin
	}
	return v, nil
}

type manifestPayload struct {
	ID                string                 `json:"id"`
	SchemaVersion     uint16                 `json:"schema_version"`
	CapabilityVersion uint16                 `json:"capability_version"`
	SignatureVersion  uint16                 `json:"signature_version"`
	ValidFrom         time.Time              `json:"valid_from"`
	ValidUntil        time.Time              `json:"valid_until"`
	ImageDigest       string                 `json:"image_digest"`
	ProxyDigest       string                 `json:"proxy_digest"`
	Routes            []string               `json:"routes"`
	Fields            []string               `json:"fields"`
	Parser            string                 `json:"parser"`
	IdentityProfile   domain.IdentityProfile `json:"identity_profile"`
	APCIsolation      domain.APCIsolation    `json:"apc_isolation"`
	TenantSaltVersion uint16                 `json:"tenant_salt_version,omitempty"`
	TenantSaltProven  bool                   `json:"tenant_salt_proven,omitempty"`
	Metrics           []string               `json:"metrics,omitempty"`
	Termination       bool                   `json:"termination,omitempty"`
	CacheMetadata     bool                   `json:"cache_metadata,omitempty"`
	Mover             bool                   `json:"mover,omitempty"`
}

func (v *ManifestVerifier) Verify(s SignedManifest) (domain.CapabilityManifest, error) {
	key, ok := v.keys[s.KeyID]
	if !ok || len(s.Payload) == 0 || len(s.Payload) > 64<<10 || len(s.Signature) != ed25519.SignatureSize || !ed25519.Verify(key, s.Payload, s.Signature) {
		return domain.CapabilityManifest{}, errors.New("capability manifest signature rejected")
	}
	var p manifestPayload
	decoder := json.NewDecoder(newBoundedReader(s.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return domain.CapabilityManifest{}, fmt.Errorf("decode capability manifest: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return domain.CapabilityManifest{}, errors.New("capability manifest has trailing data")
	}
	pin, ok := v.pins[p.ID]
	digest := sha256.Sum256(s.Payload)
	if !ok || digest != pin {
		return domain.CapabilityManifest{}, errors.New("capability manifest is not pinned")
	}
	now := v.now().UTC()
	manifest, err := domain.NewCapabilityManifest(domain.CapabilityManifestParams{
		ID: p.ID, SchemaVersion: p.SchemaVersion, CapabilityVersion: p.CapabilityVersion,
		SignatureVersion: p.SignatureVersion, SignatureVerified: true, VerifiedAt: now,
		ValidFrom: p.ValidFrom, ValidUntil: p.ValidUntil, ImageDigest: p.ImageDigest,
		ProxyDigest: p.ProxyDigest, Routes: p.Routes, Fields: p.Fields, Parser: p.Parser,
		IdentityProfile: p.IdentityProfile, APCIsolation: p.APCIsolation,
		TenantSaltVersion: p.TenantSaltVersion, TenantSaltProven: p.TenantSaltProven,
		Metrics: p.Metrics, Termination: p.Termination, CacheMetadata: p.CacheMetadata, Mover: p.Mover,
	})
	if err != nil || !manifest.ValidAt(now) {
		return domain.CapabilityManifest{}, errors.New("capability manifest invalid or outside validity window")
	}
	return manifest, nil
}

type byteReader struct{ b []byte }

func newBoundedReader(b []byte) *byteReader { return &byteReader{b: b} }
func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
