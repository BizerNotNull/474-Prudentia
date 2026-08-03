package auth_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/auth"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func TestOIDCAuthenticatorFailsClosedAndRefreshesUnknownKey(t *testing.T) {
	first, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	second, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	kid, key := "first", &first.PublicKey
	fetches := 0
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		fetches++
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{jwk(kid, key)}})
	}))
	defer issuer.Close()
	now := time.Unix(2_000_000_000, 0)
	authenticator, err := auth.NewAuthenticatorWithOIDC(nil, []auth.OIDCConfig{{Issuer: issuer.URL, Audience: "gateway", JWKSURL: issuer.URL, HTTPClient: issuer.Client(), Clock: func() time.Time { return now }, RefreshInterval: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	valid := signedToken(t, first, "first", issuer.URL, "gateway", now.Add(time.Minute))
	if principal, err := authenticate(authenticator, valid); err != nil || principal.Tenant() != "tenant-a" {
		t.Fatalf("valid token: principal=%v err=%v", principal, err)
	}

	mu.Lock()
	kid, key = "second", &second.PublicKey
	mu.Unlock()
	now = now.Add(time.Hour + time.Second)
	rotated := signedToken(t, second, "second", issuer.URL, "gateway", now.Add(time.Minute))
	if _, err := authenticate(authenticator, rotated); err != nil {
		t.Fatalf("rotated key rejected: %v", err)
	}

	mu.Lock()
	fetchesAfterRotation := fetches
	mu.Unlock()
	for i := range 20 {
		unknown := signedToken(t, second, "attacker-"+strconv.Itoa(i), issuer.URL, "gateway", now.Add(time.Minute))
		if _, err := authenticate(authenticator, unknown); domain.ErrorKindOf(err) != domain.ErrorUnauthenticated {
			t.Fatalf("unknown kid %d: error=%v", i, err)
		}
	}
	mu.Lock()
	if fetches != fetchesAfterRotation {
		t.Fatalf("unknown kids caused %d extra JWKS fetches", fetches-fetchesAfterRotation)
	}
	mu.Unlock()
	for name, token := range map[string]string{
		"expired":     signedToken(t, second, "second", issuer.URL, "gateway", now.Add(-time.Second)),
		"audience":    signedToken(t, second, "second", issuer.URL, "other", now.Add(time.Minute)),
		"issuer":      signedToken(t, second, "second", issuer.URL+"/other", "gateway", now.Add(time.Minute)),
		"removed key": valid,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := authenticate(authenticator, token); domain.ErrorKindOf(err) != domain.ErrorUnauthenticated {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestOIDCRejectsUnauthenticatedKeySources(t *testing.T) {
	configs := []auth.OIDCConfig{
		{Issuer: "http://issuer.example", Audience: "gateway", JWKSURL: "https://issuer.example/keys"},
		{Issuer: "https://issuer.example", Audience: "gateway", JWKSURL: "http://issuer.example/keys"},
		{Issuer: "https://issuer.example", Audience: "gateway", JWKSURL: "https://issuer.example/keys", HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}},
	}
	for _, cfg := range configs {
		if _, err := auth.NewAuthenticatorWithOIDC(nil, []auth.OIDCConfig{cfg}); err == nil {
			t.Fatal("accepted an unauthenticated OIDC source")
		}
	}
}

func authenticate(a *auth.Authenticator, token string) (domain.Principal, error) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	return a.Authenticate(context.Background(), r)
}

func jwk(kid string, key *rsa.PublicKey) map[string]string {
	e := big.NewInt(int64(key.E)).Bytes()
	return map[string]string{"kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(e)}
}

func signedToken(t *testing.T, key *rsa.PrivateKey, kid, issuer, audience string, expires time.Time) string {
	t.Helper()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "kid": kid, "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{"iss": issuer, "sub": "subject-a", "aud": audience, "exp": expires.Unix(), "tenant": "tenant-a", "models": []string{"model-a"}, "features": []string{"streaming"}, "max_priority": "normal", "cache_policies": []string{"disabled"}})
	encoded := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := crypto.SHA256.New()
	_, _ = digest.Write([]byte(encoded))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest.Sum(nil))
	if err != nil {
		t.Fatal(err)
	}
	return encoded + "." + base64.RawURLEncoding.EncodeToString(signature)
}
