package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	schedulerv1 "github.com/BizerNotNull/474-Prudentia/api/scheduler/v1"
	postgresadapter "github.com/BizerNotNull/474-Prudentia/internal/adapter/postgres"
	"github.com/BizerNotNull/474-Prudentia/internal/config"
	"github.com/BizerNotNull/474-Prudentia/internal/scheduling"
	transport "github.com/BizerNotNull/474-Prudentia/internal/transport/schedulergrpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
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
	defer cancelPing()
	if err := pool.Ping(pingCtx); err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	var ledgerTable, cryptoVersionTable *string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.scheduler_reservations')::text,
		to_regclass('public.scheduler_crypto_versions')::text`).Scan(&ledgerTable, &cryptoVersionTable); err != nil || ledgerTable == nil || cryptoVersionTable == nil {
		return errors.New("scheduler database migrations are not applied")
	}

	store, err := postgresadapter.NewSchedulerStore(pool, cfg.CapabilityKey)
	if err != nil {
		return err
	}
	service, err := scheduling.NewService(store, 3)
	if err != nil {
		return err
	}
	rpcService, err := transport.NewServer(service)
	if err != nil {
		return err
	}
	tlsConfig, err := serverTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.ClientCAFile)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10),
		grpc.UnaryInterceptor(requireSPIFFEID(cfg.GatewaySPIFFEID)),
	)
	schedulerv1.RegisterSchedulerServiceServer(grpcServer, rpcService)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for scheduler: %w", err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := store.ClassifyExpired(sweepCtx, 100)
				cancel()
				if err != nil {
					healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
					log.Printf("scheduler classification failed")
					continue
				}
				healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
			}
		}
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		stopped := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			grpcServer.Stop()
		}
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return err
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

func requireSPIFFEID(expected string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		connection, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated caller")
		}
		tlsInfo, ok := connection.AuthInfo.(credentials.TLSInfo)
		if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated caller")
		}
		for _, identity := range tlsInfo.State.PeerCertificates[0].URIs {
			if identity.String() == expected {
				return handler(ctx, request)
			}
		}
		return nil, status.Error(codes.PermissionDenied, "unauthorized caller")
	}
}
