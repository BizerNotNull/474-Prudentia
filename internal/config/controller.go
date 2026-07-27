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
	DatabaseURL    string
	HealthAddress  string
	Cluster        string
	Namespace      string
	LabelSelector  string
	ProxyPort      uint16
	ObservationTTL time.Duration
	ResyncPeriod   time.Duration
	LeaseNamespace string
	LeaseName      string
	Holder         string
	LeaseDuration  time.Duration
	RenewDeadline  time.Duration
	RetryPeriod    time.Duration
	Workers        int
	QueueSize      int
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
	return cfg, nil
}

func valueOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
