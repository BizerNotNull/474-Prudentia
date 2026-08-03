package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

const (
	maxJWTBytes  = 16 << 10
	maxJWKSBytes = 1 << 20
)

type OIDCConfig struct {
	Issuer          string
	Audience        string
	JWKSURL         string
	HTTPClient      *http.Client
	Clock           func() time.Time
	ClockSkew       time.Duration
	RefreshInterval time.Duration
	TenantClaim     string
	ModelsClaim     string
}

type issuerVerifier struct {
	config    OIDCConfig
	mu        sync.Mutex
	keys      map[string]crypto.PublicKey
	refreshed time.Time
}

func newIssuerVerifier(cfg OIDCConfig) (*issuerVerifier, error) {
	cfg.Issuer = strings.TrimSpace(cfg.Issuer)
	if cfg.Issuer == "" || cfg.Audience == "" || cfg.JWKSURL == "" || len(cfg.Issuer) > 512 || len(cfg.Audience) > 256 {
		return nil, errUnauthenticated
	}
	issuerURL, err := url.Parse(cfg.Issuer)
	if err != nil || issuerURL.Scheme != "https" && issuerURL.Scheme != "http" || issuerURL.Host == "" {
		return nil, errUnauthenticated
	}
	jwksURL, err := url.Parse(cfg.JWKSURL)
	if err != nil || jwksURL.Scheme != "https" && jwksURL.Scheme != "http" || jwksURL.Host == "" {
		return nil, errUnauthenticated
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	client := *cfg.HTTPClient
	if client.Timeout == 0 {
		client.Timeout = 5 * time.Second
	}
	if client.Timeout < 0 || client.Timeout > 30*time.Second {
		return nil, errUnauthenticated
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return errUnauthenticated }
	cfg.HTTPClient = &client
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.ClockSkew < 0 || cfg.ClockSkew > 5*time.Minute {
		return nil, errUnauthenticated
	}
	if cfg.RefreshInterval == 0 {
		cfg.RefreshInterval = 5 * time.Minute
	}
	if cfg.RefreshInterval < time.Second || cfg.RefreshInterval > 24*time.Hour {
		return nil, errUnauthenticated
	}
	if cfg.TenantClaim == "" {
		cfg.TenantClaim = "tenant"
	}
	if cfg.ModelsClaim == "" {
		cfg.ModelsClaim = "models"
	}
	return &issuerVerifier{config: cfg, keys: make(map[string]crypto.PublicKey)}, nil
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}
type jwtClaims struct {
	Issuer    string                     `json:"iss"`
	Subject   string                     `json:"sub"`
	Audience  json.RawMessage            `json:"aud"`
	Expires   json.Number                `json:"exp"`
	NotBefore json.Number                `json:"nbf"`
	IssuedAt  json.Number                `json:"iat"`
	Extra     map[string]json.RawMessage `json:"-"`
}

func (v *issuerVerifier) verify(ctx context.Context, token string) (domain.Principal, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return domain.Principal{}, errUnauthenticated
	}
	headerBytes, err := decodeSegment(parts[0], 4096)
	if err != nil {
		return domain.Principal{}, errUnauthenticated
	}
	var header jwtHeader
	if json.Unmarshal(headerBytes, &header) != nil || header.Kid == "" || len(header.Kid) > 256 || header.Alg != "RS256" && header.Alg != "ES256" {
		return domain.Principal{}, errUnauthenticated
	}
	claimsBytes, err := decodeSegment(parts[1], 12<<10)
	if err != nil {
		return domain.Principal{}, errUnauthenticated
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(claimsBytes, &raw) != nil {
		return domain.Principal{}, errUnauthenticated
	}
	var claims jwtClaims
	if json.Unmarshal(claimsBytes, &claims) != nil {
		return domain.Principal{}, errUnauthenticated
	}
	claims.Extra = raw
	if claims.Issuer != v.config.Issuer || claims.Subject == "" || len(claims.Subject) > 256 || !audienceContains(claims.Audience, v.config.Audience) {
		return domain.Principal{}, errUnauthenticated
	}
	now := v.config.Clock()
	expires, ok := numericDate(claims.Expires)
	if !ok || !now.Before(expires.Add(v.config.ClockSkew)) {
		return domain.Principal{}, errUnauthenticated
	}
	if claims.NotBefore != "" {
		nbf, ok := numericDate(claims.NotBefore)
		if !ok || now.Add(v.config.ClockSkew).Before(nbf) {
			return domain.Principal{}, errUnauthenticated
		}
	}
	if claims.IssuedAt != "" {
		issuedAt, ok := numericDate(claims.IssuedAt)
		if !ok || now.Add(v.config.ClockSkew).Before(issuedAt) {
			return domain.Principal{}, errUnauthenticated
		}
	}
	key, err := v.key(ctx, header.Kid)
	if err != nil || !verifySignature(header.Alg, key, []byte(parts[0]+"."+parts[1]), parts[2]) {
		return domain.Principal{}, errUnauthenticated
	}
	return v.principal(claims)
}

func decodeSegment(segment string, max int) ([]byte, error) {
	if segment == "" || len(segment) > max*2 {
		return nil, errUnauthenticated
	}
	value, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil || len(value) > max {
		return nil, errUnauthenticated
	}
	return value, nil
}

func numericDate(value json.Number) (time.Time, bool) {
	seconds, err := value.Int64()
	if err != nil || seconds <= 0 {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

func audienceContains(raw json.RawMessage, expected string) bool {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return one == expected
	}
	var many []string
	if json.Unmarshal(raw, &many) != nil || len(many) == 0 || len(many) > 32 {
		return false
	}
	for _, audience := range many {
		if audience == expected {
			return true
		}
	}
	return false
}

func verifySignature(alg string, key crypto.PublicKey, signed []byte, encoded string) bool {
	signature, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return false
	}
	digest := crypto.SHA256.New()
	_, _ = digest.Write(signed)
	hashed := digest.Sum(nil)
	switch alg {
	case "RS256":
		public, ok := key.(*rsa.PublicKey)
		return ok && rsa.VerifyPKCS1v15(public, crypto.SHA256, hashed, signature) == nil
	case "ES256":
		public, ok := key.(*ecdsa.PublicKey)
		if !ok || len(signature) != 64 {
			return false
		}
		return ecdsa.Verify(public, hashed, new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:]))
	}
	return false
}

func (v *issuerVerifier) key(ctx context.Context, kid string) (crypto.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.config.Clock()
	key, exists := v.keys[kid]
	if exists && now.Sub(v.refreshed) < v.config.RefreshInterval {
		return key, nil
	}
	if err := v.refreshLocked(ctx, now); err != nil {
		return nil, err
	}
	key, exists = v.keys[kid]
	if !exists {
		return nil, errUnauthenticated
	}
	return key, nil
}

type jwksDocument struct {
	Keys []jsonWebKey `json:"keys"`
}
type jsonWebKey struct{ Kty, Kid, Use, Alg, N, E, Crv, X, Y string }

func (v *issuerVerifier) refreshLocked(ctx context.Context, now time.Time) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.config.JWKSURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	response, err := v.config.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errUnauthenticated
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxJWKSBytes+1))
	if err != nil || len(body) > maxJWKSBytes {
		return errUnauthenticated
	}
	var document jwksDocument
	if json.Unmarshal(body, &document) != nil || len(document.Keys) == 0 || len(document.Keys) > 128 {
		return errUnauthenticated
	}
	keys := make(map[string]crypto.PublicKey, len(document.Keys))
	for _, item := range document.Keys {
		if item.Kid == "" || len(item.Kid) > 256 || item.Use != "" && item.Use != "sig" {
			continue
		}
		key, err := parseJWK(item)
		if err != nil {
			continue
		}
		if _, duplicate := keys[item.Kid]; duplicate {
			return errUnauthenticated
		}
		keys[item.Kid] = key
	}
	if len(keys) == 0 {
		return errUnauthenticated
	}
	v.keys, v.refreshed = keys, now
	return nil
}

func parseJWK(item jsonWebKey) (crypto.PublicKey, error) {
	switch item.Kty {
	case "RSA":
		n, err := base64.RawURLEncoding.DecodeString(item.N)
		if err != nil {
			return nil, err
		}
		e, err := base64.RawURLEncoding.DecodeString(item.E)
		if err != nil || len(e) == 0 || len(e) > 4 {
			return nil, errUnauthenticated
		}
		exponent := 0
		for _, b := range e {
			exponent = exponent<<8 | int(b)
		}
		if len(n) < 256 || exponent < 3 {
			return nil, errUnauthenticated
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exponent}, nil
	case "EC":
		if item.Crv != "P-256" {
			return nil, errUnauthenticated
		}
		x, err := base64.RawURLEncoding.DecodeString(item.X)
		if err != nil {
			return nil, err
		}
		y, err := base64.RawURLEncoding.DecodeString(item.Y)
		if err != nil {
			return nil, err
		}
		public := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}
		if len(x) != 32 || len(y) != 32 || !public.Curve.IsOnCurve(public.X, public.Y) {
			return nil, errUnauthenticated
		}
		return public, nil
	}
	return nil, errors.New("unsupported key")
}

func (v *issuerVerifier) principal(claims jwtClaims) (domain.Principal, error) {
	var tenantValue string
	if json.Unmarshal(claims.Extra[v.config.TenantClaim], &tenantValue) != nil {
		return domain.Principal{}, errUnauthenticated
	}
	tenant, err := domain.NewTenantScope(tenantValue)
	if err != nil {
		return domain.Principal{}, errUnauthenticated
	}
	var modelValues []string
	if json.Unmarshal(claims.Extra[v.config.ModelsClaim], &modelValues) != nil || len(modelValues) == 0 || len(modelValues) > 128 {
		return domain.Principal{}, errUnauthenticated
	}
	models := make([]domain.ModelKey, len(modelValues))
	for i, value := range modelValues {
		models[i], err = domain.NewModelKey(value)
		if err != nil {
			return domain.Principal{}, errUnauthenticated
		}
	}
	features := domain.EmptyFeatureSet()
	if value, ok := claims.Extra["features"]; ok {
		var names []string
		if json.Unmarshal(value, &names) != nil || len(names) > 16 {
			return domain.Principal{}, errUnauthenticated
		}
		var bits uint64
		for _, name := range names {
			switch name {
			case "streaming":
				bits |= 1 << domain.FeatureStreaming
			case "usage":
				bits |= 1 << domain.FeatureUsage
			case "prefix_cache":
				bits |= 1 << domain.FeaturePrefixCache
			case "tool_calling":
				bits |= 1 << domain.FeatureToolCalling
			default:
				return domain.Principal{}, errUnauthenticated
			}
		}
		features, err = domain.NewFeatureSet(domain.FeatureVersion1, bits)
		if err != nil {
			return domain.Principal{}, errUnauthenticated
		}
	}
	priority := domain.PriorityNormal
	if value, ok := claims.Extra["max_priority"]; ok {
		var name string
		if json.Unmarshal(value, &name) != nil {
			return domain.Principal{}, errUnauthenticated
		}
		switch name {
		case "background":
			priority = domain.PriorityBackground
		case "normal":
			priority = domain.PriorityNormal
		case "high":
			priority = domain.PriorityHigh
		default:
			return domain.Principal{}, errUnauthenticated
		}
	}
	policies := []domain.CachePolicy{domain.CachePolicyDisabled}
	if value, ok := claims.Extra["cache_policies"]; ok {
		var names []string
		if json.Unmarshal(value, &names) != nil || len(names) == 0 || len(names) > 3 {
			return domain.Principal{}, errUnauthenticated
		}
		policies = nil
		for _, name := range names {
			switch name {
			case "disabled":
				policies = append(policies, domain.CachePolicyDisabled)
			case "prefer":
				policies = append(policies, domain.CachePolicyPrefer)
			case "require_compatible":
				policies = append(policies, domain.CachePolicyRequireCompatible)
			default:
				return domain.Principal{}, errUnauthenticated
			}
		}
	}
	principal, err := domain.NewPrincipalFromParams(domain.PrincipalParams{Subject: claims.Subject, Tenant: tenant, Models: models, Features: features, MaxPriority: priority, CachePolicies: policies})
	if err != nil {
		return domain.Principal{}, errUnauthenticated
	}
	return principal, nil
}
