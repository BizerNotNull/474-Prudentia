package vllm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	requestapp "github.com/BizerNotNull/474-Prudentia/internal/request"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

// PinnedProvider adapts the exact-manifest backend to the gateway's single
// synchronous response-owner port. It has no unpinned fallback.
type PinnedProvider struct {
	backend  *Backend
	manifest domain.CapabilityManifest
}

func NewPinnedProvider(backend *Backend, manifest domain.CapabilityManifest) (*PinnedProvider, error) {
	if backend == nil || !manifest.ValidAt(backend.config.Now()) || !manifest.Supports(domain.CapabilityInference) {
		return nil, errors.New("invalid pinned provider configuration")
	}
	return &PinnedProvider{backend: backend, manifest: manifest}, nil
}

func (p *PinnedProvider) Infer(ctx context.Context, target domain.DispatchTarget, request domain.AuthorizedRequest, sink publichttp.StreamSink) error {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return requestapp.NewNotSentError(errors.New("generate provider request binding"))
	}
	call, err := domain.NewBackendCall(domain.BackendCallParams{
		Request: request, Target: target, Manifest: p.manifest,
		ProviderRequestID: "provider_" + hex.EncodeToString(id[:]),
	})
	if err != nil {
		return requestapp.NewNotSentError(errors.New("construct provider request"))
	}
	if _, err := p.backend.Infer(ctx, call, sink); err != nil {
		// Once RoundTrip begins, only the backend's positive terminal proof can
		// release capacity. Conservatively classify every error as possibly sent.
		return requestapp.NewDispatchError(err, requestapp.DispatchEvidencePossiblySent)
	}
	return nil
}
