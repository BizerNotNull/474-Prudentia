package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/auth"
	"github.com/BizerNotNull/474-Prudentia/internal/config"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/health"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

type closedInferenceService struct{}

func (closedInferenceService) Infer(context.Context, domain.AuthorizedRequest, domain.ResponseMode, publichttp.StreamSink) error {
	return domain.NewPublicError(domain.ErrorUnavailable)
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

	state := &health.State{}
	handler := publichttp.NewHandler(authenticator, auth.Authorizer{}, closedInferenceService{}, publichttp.DefaultLimits())
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
	// Readiness remains closed until the scheduler and exact-identity provider path are composed.
	state.SetReady(false)

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
