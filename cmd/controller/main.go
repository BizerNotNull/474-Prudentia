package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	kubernetesadapter "github.com/BizerNotNull/474-Prudentia/internal/adapter/kubernetes"
	postgresadapter "github.com/BizerNotNull/474-Prudentia/internal/adapter/postgres"
	"github.com/BizerNotNull/474-Prudentia/internal/config"
	controllerapp "github.com/BizerNotNull/474-Prudentia/internal/controller"
	"github.com/BizerNotNull/474-Prudentia/internal/health"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		log.Printf("controller stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadControllerFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open PostgreSQL pool: %w", err)
	}
	defer pool.Close()
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	if err := pool.Ping(pingCtx); err != nil {
		cancelPing()
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	cancelPing()
	var generationTable *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.controller_writer_generations')::text`).Scan(&generationTable); err != nil || generationTable == nil {
		return errors.New("controller database migration is not applied")
	}
	catalog, err := postgresadapter.NewControllerCatalog(pool)
	if err != nil {
		return err
	}
	adapter, err := kubernetesadapter.NewInCluster(kubernetesadapter.Config{
		Cluster: cfg.Cluster, Namespace: cfg.Namespace, LabelSelector: cfg.LabelSelector,
		ProxyPort: cfg.ProxyPort, ObservationTTL: cfg.ObservationTTL, ResyncPeriod: cfg.ResyncPeriod,
		LeaseNamespace: cfg.LeaseNamespace, LeaseName: cfg.LeaseName, Holder: cfg.Holder,
		LeaseDuration: cfg.LeaseDuration, RenewDeadline: cfg.RenewDeadline, RetryPeriod: cfg.RetryPeriod,
	})
	if err != nil {
		return err
	}
	state := &health.State{}
	controller, err := controllerapp.New(cfg.Cluster, cfg.Holder, cfg.Workers, cfg.QueueSize, catalog, adapter, adapter, state)
	if err != nil {
		return err
	}

	healthServer := &http.Server{
		Addr: cfg.HealthAddress, Handler: health.NewHandler(state),
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
	}
	listener, err := net.Listen("tcp", cfg.HealthAddress)
	if err != nil {
		return fmt.Errorf("listen for controller health: %w", err)
	}
	state.SetStarted(true)
	serveErr := make(chan error, 1)
	go func() { serveErr <- healthServer.Serve(listener) }()
	controllerErr := make(chan error, 1)
	go func() { controllerErr <- controller.Run(ctx) }()

	var result error
	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
		stop()
	case err := <-controllerErr:
		result = err
		stop()
	case <-ctx.Done():
	}
	state.SetDraining(true)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := healthServer.Shutdown(shutdownCtx); err != nil && result == nil {
		result = err
	}
	return result
}
