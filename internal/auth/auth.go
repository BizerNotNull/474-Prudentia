package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

// APIKey is a statically provisioned credential. The optional policy fields
// default to normal-priority, cache-disabled chat with streaming support.
type APIKey struct {
	Token         string
	Subject       string
	Tenant        string
	Models        []string
	Features      []domain.Feature
	MaxPriority   domain.Priority
	CachePolicies []domain.CachePolicy
}

type credential struct {
	digest    [sha256.Size]byte
	principal domain.Principal
}

type Authenticator struct {
	credentials []credential
	issuers     []*issuerVerifier
}

func NewAuthenticator(keys []APIKey) (*Authenticator, error) {
	return NewAuthenticatorWithOIDC(keys, nil)
}

// NewAuthenticatorWithOIDC builds a fail-closed authenticator accepting the
// configured API keys and OIDC issuers. At least one credential source is required.
func NewAuthenticatorWithOIDC(keys []APIKey, issuers []OIDCConfig) (*Authenticator, error) {
	if len(keys) > 256 || len(issuers) > 16 || len(keys)+len(issuers) == 0 {
		return nil, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	a := &Authenticator{credentials: make([]credential, len(keys)), issuers: make([]*issuerVerifier, len(issuers))}
	seen := make(map[[sha256.Size]byte]struct{}, len(keys))
	for i, key := range keys {
		if len(key.Token) < 16 || len(key.Token) > 512 {
			return nil, domain.NewPublicError(domain.ErrorUnauthenticated)
		}
		principal, err := principalForAPIKey(key)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256([]byte(key.Token))
		if _, exists := seen[digest]; exists {
			return nil, domain.NewPublicError(domain.ErrorUnauthenticated)
		}
		seen[digest] = struct{}{}
		a.credentials[i] = credential{digest: digest, principal: principal}
	}
	for i, cfg := range issuers {
		verifier, err := newIssuerVerifier(cfg)
		if err != nil {
			return nil, err
		}
		a.issuers[i] = verifier
	}
	return a, nil
}

func principalForAPIKey(key APIKey) (domain.Principal, error) {
	tenant, err := domain.NewTenantScope(strings.TrimSpace(key.Tenant))
	if err != nil {
		return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	models := make([]domain.ModelKey, len(key.Models))
	for i, value := range key.Models {
		models[i], err = domain.NewModelKey(strings.TrimSpace(value))
		if err != nil {
			return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
		}
	}
	features := key.Features
	if features == nil {
		features = []domain.Feature{domain.FeatureStreaming}
	}
	var bits uint64
	for _, feature := range features {
		bits |= 1 << feature
	}
	featureSet, err := domain.NewFeatureSet(domain.FeatureVersion1, bits)
	if err != nil {
		return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	priority := key.MaxPriority
	if priority == 0 {
		priority = domain.PriorityNormal
	}
	cachePolicies := key.CachePolicies
	if cachePolicies == nil {
		cachePolicies = []domain.CachePolicy{domain.CachePolicyDisabled}
	}
	subject := strings.TrimSpace(key.Subject)
	if subject == "" {
		subject = tenant.Value()
	}
	principal, err := domain.NewPrincipalFromParams(domain.PrincipalParams{Subject: subject, Tenant: tenant, Models: models, Features: featureSet, MaxPriority: priority, CachePolicies: cachePolicies})
	if err != nil {
		return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	return principal, nil
}

func (a *Authenticator) Authenticate(ctx context.Context, r *http.Request) (domain.Principal, error) {
	if a == nil || r == nil {
		return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	authorization := r.Header.Get("Authorization")
	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "" {
		if authorization != "" || len(r.Header.Values("X-API-Key")) != 1 || len(apiKey) > 512 || strings.ContainsAny(apiKey, " \t\r\n") {
			return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
		}
		if principal, ok := a.authenticateAPIKey(apiKey); ok {
			return principal, nil
		}
		return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	scheme, token, ok := strings.Cut(authorization, " ")
	if len(r.Header.Values("Authorization")) != 1 || !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.Contains(token, " ") || len(token) > maxJWTBytes {
		return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
	}
	if principal, ok := a.authenticateAPIKey(token); ok {
		return principal, nil
	}
	if strings.Count(token, ".") == 2 {
		for _, issuer := range a.issuers {
			principal, err := issuer.verify(ctx, token)
			if err == nil {
				return principal, nil
			}
		}
	}
	return domain.Principal{}, domain.NewPublicError(domain.ErrorUnauthenticated)
}

func (a *Authenticator) authenticateAPIKey(token string) (domain.Principal, bool) {
	if len(token) > 512 {
		return domain.Principal{}, false
	}
	digest := sha256.Sum256([]byte(token))
	for _, candidate := range a.credentials {
		if subtle.ConstantTimeCompare(digest[:], candidate.digest[:]) == 1 {
			return candidate.principal, true
		}
	}
	return domain.Principal{}, false
}

// Authorizer performs the complete explicit policy check without backend I/O.
type Authorizer struct{}

func (Authorizer) Authorize(_ context.Context, principal domain.Principal, request domain.InferenceRequest) (domain.AuthorizedRequest, error) {
	if principal.Allows(request) {
		return domain.NewAuthorizedRequest(principal, request), nil
	}
	// A literal "*" model grant is the only wildcard. Exact denial otherwise
	// wins; the synthetic value is used solely to recheck every non-model
	// permission and never escapes in the authorized request.
	if principal.AllowsModel("*") {
		wildcard, err := wildcardPolicyRequest(request)
		if err == nil && principal.Allows(wildcard) {
			return domain.NewAuthorizedRequest(principal, request), nil
		}
	}
	return domain.AuthorizedRequest{}, domain.NewPublicError(domain.ErrorForbidden)
}

func wildcardPolicyRequest(request domain.InferenceRequest) (domain.InferenceRequest, error) {
	requestID, _ := request.RequestID()
	key, _ := request.IdempotencyKey()
	return domain.NewInferenceRequest(domain.InferenceRequestParams{
		RequestID: requestID, Model: "*", Input: request.Input(),
		MaxOutputTokens: request.MaxOutputTokens(), Priority: request.Priority(),
		Features: request.Features(), CachePolicy: request.CachePolicy(),
		ExecutionBudget: request.ExecutionBudget(), IdempotencyKey: key,
	})
}

var errUnauthenticated = errors.New("unauthenticated")
