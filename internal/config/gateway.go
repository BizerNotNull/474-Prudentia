package config

import (
	"encoding/base64"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	requestapp "github.com/BizerNotNull/474-Prudentia/internal/request"
)

type Gateway struct {
	ListenAddress       string
	APIKey              string
	Tenant              string
	Models              []string
	SchedulerAddress    string
	SchedulerServerName string
	SchedulerCAFile     string
	TLSCertFile         string
	TLSKeyFile          string
	ProviderCAFile      string
	ProviderTrustDomain string
	IdempotencyConfig   requestapp.IdempotencyConfig
}

func LoadGatewayFromEnv() (Gateway, error) {
	var err error
	cfg := Gateway{
		ListenAddress: strings.TrimSpace(os.Getenv("PRUDENTIA_GATEWAY_LISTEN")),
		APIKey:        os.Getenv("PRUDENTIA_GATEWAY_API_KEY"),
		Tenant:        strings.TrimSpace(os.Getenv("PRUDENTIA_GATEWAY_TENANT")),
	}
	cfg.SchedulerAddress = strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_ADDRESS"))
	cfg.SchedulerServerName = strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_SERVER_NAME"))
	cfg.SchedulerCAFile = strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_CA"))
	cfg.TLSCertFile = strings.TrimSpace(os.Getenv("PRUDENTIA_GATEWAY_TLS_CERT"))
	cfg.TLSKeyFile = strings.TrimSpace(os.Getenv("PRUDENTIA_GATEWAY_TLS_KEY"))
	cfg.ProviderCAFile = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_CA"))
	cfg.ProviderTrustDomain = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_TRUST_DOMAIN"))
	lookupKeys, lookupWriteVersion, err := loadVersionedKeys(
		"PRUDENTIA_GATEWAY_IDEMPOTENCY_LOOKUP_KEYS",
		"PRUDENTIA_GATEWAY_IDEMPOTENCY_LOOKUP_WRITE_VERSION",
		domain.MaxLookupCandidates,
	)
	if err != nil {
		return Gateway{}, err
	}
	digestKeys, digestWriteVersion, err := loadVersionedKeys(
		"PRUDENTIA_GATEWAY_REQUEST_DIGEST_KEYS",
		"PRUDENTIA_GATEWAY_REQUEST_DIGEST_WRITE_VERSION",
		domain.MaxDigestCandidates,
	)
	if err != nil {
		return Gateway{}, err
	}
	cfg.IdempotencyConfig, err = requestapp.ValidateIdempotencyConfig(requestapp.IdempotencyConfig{
		LookupKeys: lookupKeys, LookupWriteVersion: lookupWriteVersion,
		DigestKeys: digestKeys, DigestWriteVersion: digestWriteVersion,
	})
	if err != nil {
		return Gateway{}, err
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:8080"
	}
	if cfg.SchedulerAddress == "" {
		cfg.SchedulerAddress = "127.0.0.1:9090"
	}
	for _, model := range strings.Split(os.Getenv("PRUDENTIA_GATEWAY_MODELS"), ",") {
		if model = strings.TrimSpace(model); model != "" {
			cfg.Models = append(cfg.Models, model)
		}
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddress); err != nil {
		return Gateway{}, errors.New("invalid gateway listen address")
	}
	if _, _, err := net.SplitHostPort(cfg.SchedulerAddress); err != nil {
		return Gateway{}, errors.New("invalid scheduler address")
	}
	if len(cfg.APIKey) < 16 || len(cfg.APIKey) > 512 {
		return Gateway{}, errors.New("gateway API key must contain 16 to 512 bytes")
	}
	if cfg.Tenant == "" || len(cfg.Models) == 0 {
		return Gateway{}, errors.New("gateway tenant and model allowlist are required")
	}
	if cfg.SchedulerServerName == "" || cfg.SchedulerCAFile == "" || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.ProviderCAFile == "" || cfg.ProviderTrustDomain == "" {
		return Gateway{}, errors.New("gateway scheduler and provider TLS configuration is required")
	}
	return cfg, nil
}

func loadVersionedKeys(keysEnv, writeVersionEnv string, maxCandidates int) ([]requestapp.VersionedKey, uint32, error) {
	rawWriteVersion, err := strconv.ParseUint(strings.TrimSpace(os.Getenv(writeVersionEnv)), 10, 32)
	if err != nil || rawWriteVersion == 0 {
		return nil, 0, errors.New("invalid idempotency keyring write version")
	}
	parts := strings.Split(os.Getenv(keysEnv), ",")
	if len(parts) == 0 || len(parts) > maxCandidates {
		return nil, 0, errors.New("invalid idempotency keyring")
	}
	keys := make([]requestapp.VersionedKey, 0, len(parts))
	for _, part := range parts {
		versionText, encoded, ok := strings.Cut(strings.TrimSpace(part), ":")
		version, versionErr := strconv.ParseUint(versionText, 10, 32)
		key, keyErr := base64.StdEncoding.DecodeString(encoded)
		if !ok || versionErr != nil || version == 0 || keyErr != nil || len(key) != 32 {
			return nil, 0, errors.New("invalid idempotency keyring")
		}
		keys = append(keys, requestapp.VersionedKey{Version: uint32(version), Key: key})
	}
	return keys, uint32(rawWriteVersion), nil
}
