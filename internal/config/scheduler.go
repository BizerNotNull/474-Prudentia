package config

import (
	"encoding/base64"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
)

type Scheduler struct {
	ListenAddress         string
	AdminListenAddress    string
	HealthAddress         string
	DatabaseURL           string
	CapabilityKey         []byte
	CapabilityKEKs        map[uint32][]byte
	CapabilityComparisons map[uint32][]byte
	TLSCertFile           string
	TLSKeyFile            string
	ClientCAFile          string
	GatewaySPIFFEID       string
	AdminClientCAFile     string
	AdminTrustDomain      string
	AdminPathPrefixes     []string
}

func LoadSchedulerFromEnv() (Scheduler, error) {
	cfg := Scheduler{
		ListenAddress:   strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_LISTEN")),
		DatabaseURL:     strings.TrimSpace(os.Getenv("PRUDENTIA_DATABASE_URL")),
		TLSCertFile:     strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_TLS_CERT")),
		TLSKeyFile:      strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_TLS_KEY")),
		ClientCAFile:    strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_CLIENT_CA")),
		GatewaySPIFFEID: strings.TrimSpace(os.Getenv("PRUDENTIA_GATEWAY_SPIFFE_ID")),
	}
	cfg.HealthAddress = strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_HEALTH_LISTEN"))
	cfg.AdminListenAddress = strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_ADMIN_LISTEN"))
	cfg.AdminClientCAFile = strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_ADMIN_CLIENT_CA"))
	cfg.AdminTrustDomain = strings.TrimSpace(os.Getenv("PRUDENTIA_SCHEDULER_ADMIN_TRUST_DOMAIN"))
	for _, prefix := range strings.Split(os.Getenv("PRUDENTIA_SCHEDULER_ADMIN_PATH_PREFIXES"), ",") {
		if prefix = strings.TrimSpace(prefix); prefix != "" {
			cfg.AdminPathPrefixes = append(cfg.AdminPathPrefixes, prefix)
		}
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:9090"
	}
	if cfg.AdminListenAddress == "" {
		cfg.AdminListenAddress = "127.0.0.1:9091"
	}
	if cfg.HealthAddress == "" {
		cfg.HealthAddress = "0.0.0.0:8080"
	}
	key, err := base64.StdEncoding.DecodeString(os.Getenv("PRUDENTIA_SCHEDULER_CAPABILITY_KEY"))
	if err != nil || len(key) != 32 {
		return Scheduler{}, errors.New("scheduler capability key must be base64-encoded 32 bytes")
	}
	cfg.CapabilityKey = key
	cfg.CapabilityKEKs, err = loadCapabilityKeys("PRUDENTIA_SCHEDULER_CAPABILITY_KEKS")
	if err != nil {
		return Scheduler{}, err
	}
	cfg.CapabilityComparisons, err = loadCapabilityKeys("PRUDENTIA_SCHEDULER_CAPABILITY_COMPARISON_KEYS")
	if err != nil {
		return Scheduler{}, err
	}
	if len(cfg.CapabilityKEKs) == 0 {
		cfg.CapabilityKEKs = map[uint32][]byte{1: append([]byte(nil), key...)}
	}
	if len(cfg.CapabilityComparisons) == 0 {
		cfg.CapabilityComparisons = map[uint32][]byte{1: append([]byte(nil), key...)}
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddress); err != nil {
		return Scheduler{}, errors.New("invalid scheduler listen address")
	}
	if _, _, err := net.SplitHostPort(cfg.AdminListenAddress); err != nil || cfg.AdminListenAddress == cfg.ListenAddress {
		return Scheduler{}, errors.New("invalid or shared scheduler admin listen address")
	}
	if _, _, err := net.SplitHostPort(cfg.HealthAddress); err != nil {
		return Scheduler{}, errors.New("invalid scheduler health listen address")
	}
	if cfg.DatabaseURL == "" || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.ClientCAFile == "" || cfg.GatewaySPIFFEID == "" ||
		cfg.AdminClientCAFile == "" || cfg.AdminTrustDomain == "" || len(cfg.AdminPathPrefixes) == 0 {
		return Scheduler{}, errors.New("scheduler database, request/admin TLS, and identity policy are required")
	}
	if !strings.HasPrefix(cfg.GatewaySPIFFEID, "spiffe://") {
		return Scheduler{}, errors.New("invalid gateway SPIFFE ID")
	}
	return cfg, nil
}

func loadCapabilityKeys(name string) (map[uint32][]byte, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil, nil
	}
	result := make(map[uint32][]byte)
	for _, part := range strings.Split(raw, ",") {
		versionText, encoded, ok := strings.Cut(strings.TrimSpace(part), ":")
		version, versionErr := strconv.ParseUint(versionText, 10, 32)
		key, keyErr := base64.StdEncoding.DecodeString(encoded)
		if !ok || versionErr != nil || version == 0 || keyErr != nil || len(key) != 32 {
			return nil, errors.New("invalid scheduler capability keyring")
		}
		if _, duplicate := result[uint32(version)]; duplicate {
			return nil, errors.New("duplicate scheduler capability key version")
		}
		result[uint32(version)] = key
	}
	return result, nil
}
