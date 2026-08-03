package kvmover

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type SignedConnectorManifest struct {
	KeyID, PinnedDigest string
	Payload, Signature  []byte
}

type connectorManifestPayload struct {
	SchemaVersion        uint16        `json:"schema_version"`
	ControlPlaneIdentity string        `json:"control_plane_identity"`
	ValidFrom            time.Time     `json:"valid_from"`
	ValidUntil           time.Time     `json:"valid_until"`
	MaxBytes             uint64        `json:"max_bytes"`
	MaxOperation         time.Duration `json:"max_operation"`
}

type ConnectorManifestVerifier struct {
	keys map[string]ed25519.PublicKey
	pins map[string]struct{}
	now  func() time.Time
}

func NewConnectorManifestVerifier(keys map[string]ed25519.PublicKey, pinnedDigests []string, now func() time.Time) (*ConnectorManifestVerifier, error) {
	if len(keys) == 0 || len(pinnedDigests) == 0 || now == nil {
		return nil, errors.New("invalid connector manifest verifier")
	}
	clonedKeys := make(map[string]ed25519.PublicKey, len(keys))
	for id, key := range keys {
		if id == "" || len(key) != ed25519.PublicKeySize {
			return nil, errors.New("invalid connector signer")
		}
		clonedKeys[id] = append(ed25519.PublicKey(nil), key...)
	}
	pins := make(map[string]struct{}, len(pinnedDigests))
	for _, pin := range pinnedDigests {
		if len(pin) != sha256.Size*2 {
			return nil, errors.New("invalid connector manifest pin")
		}
		if _, err := hex.DecodeString(pin); err != nil {
			return nil, errors.New("invalid connector manifest pin")
		}
		pins[pin] = struct{}{}
	}
	return &ConnectorManifestVerifier{clonedKeys, pins, now}, nil
}

func (v *ConnectorManifestVerifier) Verify(signed SignedConnectorManifest) (domain.ConnectorManifest, error) {
	key, ok := v.keys[signed.KeyID]
	if !ok || len(signed.Payload) == 0 || !ed25519.Verify(key, signed.Payload, signed.Signature) {
		return domain.ConnectorManifest{}, errors.New("connector manifest signature invalid")
	}
	digest := sha256.Sum256(signed.Payload)
	digestHex := hex.EncodeToString(digest[:])
	if digestHex != signed.PinnedDigest {
		return domain.ConnectorManifest{}, errors.New("connector manifest claimed pin mismatch")
	}
	if _, ok = v.pins[digestHex]; !ok {
		return domain.ConnectorManifest{}, errors.New("connector manifest not allowlisted")
	}
	var payload connectorManifestPayload
	decoder := json.NewDecoder(bytes.NewReader(signed.Payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || payload.SchemaVersion != 1 {
		return domain.ConnectorManifest{}, errors.New("connector manifest payload invalid")
	}
	manifest, err := domain.NewConnectorManifest(domain.ConnectorManifestParams{Digest: "sha256:" + digestHex, ControlPlaneIdentity: payload.ControlPlaneIdentity, SignatureVerified: true, ValidFrom: payload.ValidFrom, ValidUntil: payload.ValidUntil, MaxBytes: payload.MaxBytes, MaxOperation: payload.MaxOperation})
	if err != nil || !manifest.ValidAt(v.now()) {
		return domain.ConnectorManifest{}, errors.New("connector manifest expired or invalid")
	}
	return manifest, nil
}
