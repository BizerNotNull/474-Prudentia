package config

import (
	"errors"
	"net"
	"os"
	"strings"
)

type Gateway struct {
	ListenAddress string
	APIKey        string
	Tenant        string
	Models        []string
}

func LoadGatewayFromEnv() (Gateway, error) {
	cfg := Gateway{
		ListenAddress: strings.TrimSpace(os.Getenv("PRUDENTIA_GATEWAY_LISTEN")),
		APIKey:        os.Getenv("PRUDENTIA_GATEWAY_API_KEY"),
		Tenant:        strings.TrimSpace(os.Getenv("PRUDENTIA_GATEWAY_TENANT")),
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:8080"
	}
	for _, model := range strings.Split(os.Getenv("PRUDENTIA_GATEWAY_MODELS"), ",") {
		if model = strings.TrimSpace(model); model != "" {
			cfg.Models = append(cfg.Models, model)
		}
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddress); err != nil {
		return Gateway{}, errors.New("invalid gateway listen address")
	}
	if len(cfg.APIKey) < 16 || len(cfg.APIKey) > 512 {
		return Gateway{}, errors.New("gateway API key must contain 16 to 512 bytes")
	}
	if cfg.Tenant == "" || len(cfg.Models) == 0 {
		return Gateway{}, errors.New("gateway tenant and model allowlist are required")
	}
	return cfg, nil
}
