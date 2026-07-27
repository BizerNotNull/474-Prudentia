package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	"github.com/BizerNotNull/474-Prudentia/internal/adapter/schedulerclient"
	"github.com/BizerNotNull/474-Prudentia/internal/adapter/vllm"
	"github.com/BizerNotNull/474-Prudentia/internal/auth"
	"github.com/BizerNotNull/474-Prudentia/internal/config"
	"github.com/BizerNotNull/474-Prudentia/internal/health"
	requestapp "github.com/BizerNotNull/474-Prudentia/internal/request"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func clientTLSConfig(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load gateway TLS identity: %w", err)
	}
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("load CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, errors.New("CA file contains no certificate")
	}
	return &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS13}, nil
}

func main() {
	if err := run(); err != nil {
		log.Printf("gateway stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadGatewayFromEnv()
	if err != nil {
		return err
	}
	authenticator, err := auth.NewAuthenticator([]auth.APIKey{{Token: cfg.APIKey, Tenant: cfg.Tenant, Models: cfg.Models}})
	if err != nil {
		return err
	}
	schedulerTLS, err := clientTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.SchedulerCAFile, cfg.SchedulerServerName)
	if err != nil {
		return err
	}
	connection, err := grpc.NewClient(
		"dns:///"+cfg.SchedulerAddress,
		grpc.WithTransportCredentials(credentials.NewTLS(schedulerTLS)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10), grpc.MaxCallSendMsgSize(64<<10)),
	)
	if err != nil {
		return fmt.Errorf("create scheduler client: %w", err)
	}
	defer connection.Close()
	scheduler, err := schedulerclient.New(schedulerv1.NewSchedulerServiceClient(connection))
	if err != nil {
		return err
	}
	providerTLS, err := clientTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.ProviderCAFile, "provider.invalid")
	if err != nil {
		return err
	}
	provider, err := vllm.NewClient(providerTLS, cfg.ProviderTrustDomain, 15*time.Second, 1<<20, 65536)
	if err != nil {
		return err
	}
	inference, err := requestapp.NewService(scheduler, provider, 2*time.Minute, 5*time.Second)
	if err != nil {
		return err
	}

	state := &health.State{}
	healthClient := healthpb.NewHealthClient(connection)
	healthCtx, cancelHealth := context.WithTimeout(context.Background(), 5*time.Second)
	healthResponse, err := healthClient.Check(healthCtx, &healthpb.HealthCheckRequest{})
	cancelHealth()
	if err != nil || healthResponse.Status != healthpb.HealthCheckResponse_SERVING {
		return errors.New("scheduler is not ready")
	}
	handler := publichttp.NewHandler(authenticator, auth.Authorizer{}, inference, publichttp.DefaultLimits())
	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           handler.Routes(health.NewHandler(state)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return err
	}
	state.SetStarted(true)
	state.SetReady(true)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		state.SetDraining(true)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return err
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
