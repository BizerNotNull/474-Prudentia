package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	vllmadapter "github.com/BizerNotNull/474-Prudentia/internal/adapter/vllm"
	"github.com/BizerNotNull/474-Prudentia/internal/config"
)

func main() {
	if err := run(); err != nil {
		log.Printf("identity proxy stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadIdentityProxyFromEnv()
	if err != nil {
		return err
	}
	proxyConfig, err := vllmadapter.LoadIdentityProxyConfig(
		cfg.TLSCertFile, cfg.TLSKeyFile, cfg.ServerCAFile, cfg.GatewayClientCAFile,
		vllmadapter.IdentityProxyConfig{
			Upstream: cfg.Upstream, AllowedGatewaySPIFFEIDs: cfg.AllowedGatewaySPIFFEIDs,
			Identity: cfg.Identity, ManifestID: cfg.ManifestID,
			ProviderImageDigest: cfg.ProviderImageDigest, ProxyImageDigest: cfg.ProxyImageDigest,
			MaxRequestBytes: cfg.MaxRequestBytes,
		},
	)
	if err != nil {
		return err
	}
	proxy, err := vllmadapter.NewIdentityProxy(proxyConfig)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for identity proxy: %w", err)
	}
	server := &http.Server{
		Addr: cfg.ListenAddress, Handler: proxy.Handler(), TLSConfig: proxy.TLSConfig(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 0, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: vllmadapter.ProxyMaxHeaderBytes(),
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(tls.NewListener(listener, server.TLSConfig)) }()
	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
