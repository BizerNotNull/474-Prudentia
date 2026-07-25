package config

import (
	"encoding/base64"
	"errors"
	"net"
	"os"
	"strings"
)

type Scheduler struct {
	ListenAddress   string
	DatabaseURL     string
	CapabilityKey   []byte
	TLSCertFile     string
	TLSKeyFile      string
	ClientCAFile    string
	GatewaySPIFFEID string
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
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:9090"
	}
	key, err := base64.StdEncoding.DecodeString(os.Getenv("PRUDENTIA_SCHEDULER_CAPABILITY_KEY"))
	if err != nil || len(key) != 32 {
		return Scheduler{}, errors.New("scheduler capability key must be base64-encoded 32 bytes")
	}
	cfg.CapabilityKey = key
	if _, _, err := net.SplitHostPort(cfg.ListenAddress); err != nil {
		return Scheduler{}, errors.New("invalid scheduler listen address")
	}
	if cfg.DatabaseURL == "" || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.ClientCAFile == "" || cfg.GatewaySPIFFEID == "" {
		return Scheduler{}, errors.New("scheduler database, TLS files, and gateway SPIFFE ID are required")
	}
	if !strings.HasPrefix(cfg.GatewaySPIFFEID, "spiffe://") {
		return Scheduler{}, errors.New("invalid gateway SPIFFE ID")
	}
	return cfg, nil
}
