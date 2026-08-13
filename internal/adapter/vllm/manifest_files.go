package vllm

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

// LoadVerifiedManifest reads bounded deployment material and returns only the
// immutable verified form. Raw signatures and payloads are not retained.
func LoadVerifiedManifest(payloadFile, signatureFile, keyID, manifestID, publicKeyBase64, payloadPin string, now func() time.Time) (domain.CapabilityManifest, error) {
	if payloadFile == "" || signatureFile == "" || keyID == "" || manifestID == "" || publicKeyBase64 == "" || payloadPin == "" || now == nil {
		return domain.CapabilityManifest{}, errors.New("complete capability manifest configuration is required")
	}
	payload, err := os.ReadFile(payloadFile)
	if err != nil || len(payload) == 0 || len(payload) > 64<<10 {
		return domain.CapabilityManifest{}, errors.New("read bounded capability manifest")
	}
	signatureEncoded, err := os.ReadFile(signatureFile)
	if err != nil || len(signatureEncoded) == 0 || len(signatureEncoded) > 1024 {
		return domain.CapabilityManifest{}, errors.New("read bounded capability manifest signature")
	}
	signature, err := base64.StdEncoding.DecodeString(string(signatureEncoded))
	if err != nil {
		return domain.CapabilityManifest{}, errors.New("decode capability manifest signature")
	}
	publicKey, err := base64.StdEncoding.DecodeString(publicKeyBase64)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return domain.CapabilityManifest{}, errors.New("decode capability manifest public key")
	}
	verifier, err := NewManifestVerifier(map[string]ed25519.PublicKey{keyID: publicKey}, map[string]string{manifestID: payloadPin}, now)
	if err != nil {
		return domain.CapabilityManifest{}, err
	}
	return verifier.Verify(SignedManifest{KeyID: keyID, Payload: payload, Signature: signature})
}
