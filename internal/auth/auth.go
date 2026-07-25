package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

type APIKey struct {
	Token  string
	Tenant string
	Models []string
}

type credential struct {
	digest    [sha256.Size]byte
	principal domain.Principal
}

type Authenticator struct {
	credentials []credential
}

func NewAuthenticator(keys []APIKey) (*Authenticator, error) {
	if len(keys) == 0 || len(keys) > 256 {
		return nil, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	credentials := make([]credential, len(keys))
	seen := make(map[[sha256.Size]byte]struct{}, len(keys))
	for i, key := range keys {
		if len(key.Token) < 16 || len(key.Token) > 512 {
			return nil, domain.NewPublicError(domain.ErrorUnauthenticated)
		}
		principal, err := domain.NewPrincipal(key.Tenant, key.Models)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256([]byte(key.Token))
		if _, exists := seen[digest]; exists {
			return nil, domain.NewPublicError(domain.ErrorUnauthenticated)
		}
		seen[digest] = struct{}{}
		credentials[i] = credential{digest: digest, principal: principal}
	}
	return &Authenticator{credentials: credentials}, nil
}

func (a *Authenticator) Authenticate(_ context.Context, r *http.Request) (domain.Principal, error) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || len(token) > 512 {
		return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	digest := sha256.Sum256([]byte(token))
	for _, candidate := range a.credentials {
		if subtle.ConstantTimeCompare(digest[:], candidate.digest[:]) == 1 {
			return candidate.principal, nil
		}
	}
	return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
}

type Authorizer struct{}

func (Authorizer) Authorize(_ context.Context, principal domain.Principal, request domain.InferenceRequest) (domain.AuthorizedRequest, error) {
	if !principal.AllowsModel(request.Model()) {
		return domain.AuthorizedRequest{}, domain.NewPublicError(domain.ErrorForbidden)
	}
	return domain.NewAuthorizedRequest(principal, request), nil
}
