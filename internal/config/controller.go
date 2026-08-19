package config

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Controller struct {
	DatabaseURL           string
	HealthAddress         string
	Cluster               string
	Namespace             string
	LabelSelector         string
	ProxyPort             uint16
	ObservationTTL        time.Duration
	ResyncPeriod          time.Duration
	LeaseNamespace        string
	LeaseName             string
	Holder                string
	LeaseDuration         time.Duration
	RenewDeadline         time.Duration
	RetryPeriod           time.Duration
	Workers               int
	QueueSize             int
	TLSCertFile           string
	TLSKeyFile            string
	ProviderCAFile        string
	ProviderTrustDomain   string
	ManifestPayloadFile   string
	ManifestSignatureFile string
	ManifestKeyID         string
	ManifestID            string
	ManifestPublicKey     string
	ManifestPin           string
}

func LoadControllerFromEnv() (Controller, error) {
	cfg := Controller{
		DatabaseURL:    strings.TrimSpace(os.Getenv("PRUDENTIA_DATABASE_URL")),
		HealthAddress:  valueOrDefault("PRUDENTIA_CONTROLLER_HEALTH_LISTEN", "127.0.0.1:8081"),
		Cluster:        strings.TrimSpace(os.Getenv("PRUDENTIA_CLUSTER")),
		Namespace:      strings.TrimSpace(os.Getenv("PRUDENTIA_CONTROLLER_NAMESPACE")),
		LabelSelector:  valueOrDefault("PRUDENTIA_CONTROLLER_LABEL_SELECTOR", "prudentia.io/managed=true"),
		LeaseNamespace: strings.TrimSpace(os.Getenv("PRUDENTIA_CONTROLLER_LEASE_NAMESPACE")),
		LeaseName:      valueOrDefault("PRUDENTIA_CONTROLLER_LEASE_NAME", "prudentia-controller"),
		Holder:         strings.TrimSpace(os.Getenv("HOSTNAME")),
		ObservationTTL: 30 * time.Second,
		ResyncPeriod:   30 * time.Second,
		LeaseDuration:  15 * time.Second,
		RenewDeadline:  10 * time.Second,
		RetryPeriod:    2 * time.Second,
		Workers:        2,
		QueueSize:      1024,
	}
	cfg.TLSCertFile = strings.TrimSpace(os.Getenv("PRUDENTIA_CONTROLLER_TLS_CERT"))
	cfg.TLSKeyFile = strings.TrimSpace(os.Getenv("PRUDENTIA_CONTROLLER_TLS_KEY"))
	cfg.ProviderCAFile = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_CA"))
	cfg.ProviderTrustDomain = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_TRUST_DOMAIN"))
	cfg.ManifestPayloadFile = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_MANIFEST_PAYLOAD"))
	cfg.ManifestSignatureFile = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_MANIFEST_SIGNATURE"))
	cfg.ManifestKeyID = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_MANIFEST_KEY_ID"))
	cfg.ManifestID = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_MANIFEST_ID"))
	cfg.ManifestPublicKey = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_MANIFEST_PUBLIC_KEY"))
	cfg.ManifestPin = strings.TrimSpace(os.Getenv("PRUDENTIA_PROVIDER_MANIFEST_PIN"))
	if cfg.LeaseNamespace == "" {
		cfg.LeaseNamespace = cfg.Namespace
	}
	port, err := strconv.ParseUint(valueOrDefault("PRUDENTIA_CONTROLLER_PROXY_PORT", "8443"), 10, 16)
	if err != nil || port == 0 {
		return Controller{}, errors.New("invalid controller proxy port")
	}
	cfg.ProxyPort = uint16(port)
	if _, _, err := net.SplitHostPort(cfg.HealthAddress); err != nil {
		return Controller{}, errors.New("invalid controller health listen address")
	}
	if cfg.DatabaseURL == "" || cfg.Cluster == "" || cfg.Namespace == "" || cfg.LeaseNamespace == "" || cfg.Holder == "" {
		return Controller{}, errors.New("controller database, cluster, namespace, and holder are required")
	}
	if len(cfg.Cluster) > 128 || len(cfg.Namespace) > 253 || len(cfg.LeaseNamespace) > 253 || len(cfg.LeaseName) > 253 || len(cfg.Holder) > 256 || len(cfg.LabelSelector) > 1024 {
		return Controller{}, errors.New("controller configuration exceeds bounds")
	}
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.ProviderCAFile == "" || cfg.ProviderTrustDomain == "" ||
		cfg.ManifestPayloadFile == "" || cfg.ManifestSignatureFile == "" || cfg.ManifestKeyID == "" || cfg.ManifestID == "" || cfg.ManifestPublicKey == "" || cfg.ManifestPin == "" {
		return Controller{}, errors.New("controller provider TLS and signed manifest configuration is required")
	}
	return cfg, nil
}

func valueOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
