package config

import (
	"errors"
	"net"
	"os"
	"strings"
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
}

func LoadGatewayFromEnv() (Gateway, error) {
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
