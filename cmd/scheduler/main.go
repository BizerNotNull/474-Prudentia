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

	adminv1 "github.com/BizerNotNull/474-Prudentia/api/admin/v1"
	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	postgresadapter "github.com/BizerNotNull/474-Prudentia/internal/adapter/postgres"
	"github.com/BizerNotNull/474-Prudentia/internal/config"
	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	apphealth "github.com/BizerNotNull/474-Prudentia/internal/health"
	"github.com/BizerNotNull/474-Prudentia/internal/scheduling"
	admintransport "github.com/BizerNotNull/474-Prudentia/internal/transport/admingrpc"
	transport "github.com/BizerNotNull/474-Prudentia/internal/transport/schedulergrpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if err := run(); err != nil {
		log.Printf("scheduler stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadSchedulerFromEnv()
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
	var systemTable, reservationsTable *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.system_admission_state')::text,
		to_regclass('public.reservations')::text`).Scan(&systemTable, &reservationsTable); err != nil || systemTable == nil || reservationsTable == nil {
		return errors.New("authoritative scheduler database migrations are not applied")
	}
	keyring, err := postgresadapter.NewLocalCapabilityKeyring(cfg.CapabilityKEKs, cfg.CapabilityComparisons)
	if err != nil {
		return err
	}
	catalog, err := postgresadapter.NewCatalog(pool, keyring)
	if err != nil {
		return err
	}
	store := catalog.SchedulerStore()
	service, err := scheduling.NewService(store, 3)
	if err != nil {
		return err
	}
	rpcService, err := transport.NewServer(service)
	if err != nil {
		return err
	}
	requestTLS, err := serverTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.ClientCAFile)
	if err != nil {
		return err
	}
	requestInterceptor, err := transport.NewGatewayUnaryInterceptor(transport.GatewayInterceptorConfig{
		AllowedSPIFFEIDs: []string{cfg.GatewaySPIFFEID}, MaxDeadline: 30 * time.Minute, MaxMessageBytes: 64 << 10,
	})
	if err != nil {
		return err
	}
	requestServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(requestTLS)),
		grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10),
		grpc.UnaryInterceptor(requestInterceptor),
	)
	schedulerv1.RegisterSchedulerServiceServer(requestServer, rpcService)
	healthServer := grpchealth.NewServer()
	healthpb.RegisterHealthServer(requestServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	adminTLS, err := serverTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.AdminClientCAFile)
	if err != nil {
		return err
	}
	adminApp, err := admintransport.NewAdminServer(
		&admintransport.AdminAuthenticator{TrustDomain: cfg.AdminTrustDomain, AllowedPathPrefixes: cfg.AdminPathPrefixes},
		&admintransport.AdminAuthorizer{Policy: configuredAdminPolicy{}},
		admintransport.AdminCodec{Resolver: catalog},
		catalog,
	)
	if err != nil {
		return err
	}
	adminServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(adminTLS)),
		grpc.MaxRecvMsgSize(16<<10), grpc.MaxSendMsgSize(16<<10),
	)
	adminv1.RegisterCapacityDebtAdminServiceServer(adminServer, adminApp)
	state := &apphealth.State{}
	state.SetStarted(true)
	healthHTTP := &http.Server{
		Addr: cfg.HealthAddress, Handler: apphealth.NewHandler(state),
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
	}

	requestListener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for scheduler request service: %w", err)
	}
	adminListener, err := net.Listen("tcp", cfg.AdminListenAddress)
	if err != nil {
		requestListener.Close()
		return fmt.Errorf("listen for scheduler admin service: %w", err)
	}
	healthListener, err := net.Listen("tcp", cfg.HealthAddress)
	if err != nil {
		requestListener.Close()
		adminListener.Close()
		return fmt.Errorf("listen for scheduler health service: %w", err)
	}
	serveErr := make(chan error, 3)
	go func() { serveErr <- requestServer.Serve(requestListener) }()
	go func() { serveErr <- adminServer.Serve(adminListener) }()
	go func() { serveErr <- healthHTTP.Serve(healthListener) }()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, sweepErr := store.ClassifyExpired(sweepCtx, 100)
				ready := sweepErr == nil && schedulerReady(sweepCtx, pool)
				cancel()
				if ready {
					healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
					state.SetReady(true)
				} else {
					healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
					state.SetReady(false)
				}
			}
		}
	}()

	select {
	case err := <-serveErr:
		stop()
		return err
	case <-ctx.Done():
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		state.SetDraining(true)
		gracefulStop(requestServer, 10*time.Second)
		gracefulStop(adminServer, 10*time.Second)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = healthHTTP.Shutdown(shutdownCtx)
		cancel()
		for range 3 {
			if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
		}
		return nil
	}
}

func serverTLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load scheduler TLS identity: %w", err)
	}
	roots, err := loadCertPool(clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("load gateway CA: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13}, nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("CA file contains no certificate")
	}
	return pool, nil
}

type configuredAdminPolicy struct{}

func (configuredAdminPolicy) Allows(_ context.Context, _ domain.AdminPrincipal, action domain.AdminAction, target admintransport.DebtTarget) bool {
	return action == domain.AdminActionCapacityDebtUnsafeOverride &&
		target.DebtID != "" && target.PodUID != "" && target.EndpointEpoch != 0
}

func schedulerReady(ctx context.Context, pool *pgxpool.Pool) bool {
	var admission, dispatch string
	err := pool.QueryRow(ctx, `SELECT admission_state,dispatch_state
		FROM system_admission_state WHERE cluster_id='default'`).Scan(&admission, &dispatch)
	return err == nil && admission == "open" && dispatch == "open"
}

func gracefulStop(server *grpc.Server, timeout time.Duration) {
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(timeout):
		server.Stop()
	}
}
