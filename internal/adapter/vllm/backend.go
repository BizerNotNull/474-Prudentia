package vllm

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

var ErrUnsupported = errors.New("provider capability unsupported")

type BackendConfig struct {
	TLSConfig             *tls.Config
	ResponseHeaderTimeout time.Duration
	DialTimeout           time.Duration
	MaxEventBytes         int
	MaxEvents             int
	MaxResponseBytes      int64
	Now                   func() time.Time
	ResolveEndpoint       func(domain.WorkloadIdentity) (string, error)
}

type Backend struct{ config BackendConfig }

func NewBackend(config BackendConfig) (*Backend, error) {
	if config.TLSConfig == nil || config.TLSConfig.RootCAs == nil || len(config.TLSConfig.Certificates) == 0 || config.ResponseHeaderTimeout <= 0 || config.DialTimeout <= 0 || config.MaxEventBytes < 1024 || config.MaxEvents < 1 || config.MaxResponseBytes < 1024 || config.Now == nil {
		return nil, errors.New("invalid vLLM backend configuration")
	}
	config.TLSConfig = config.TLSConfig.Clone()
	return &Backend{config: config}, nil
}

func (b *Backend) Infer(ctx context.Context, call domain.BackendCall, sink publichttp.StreamSink) (domain.TerminalProof, error) {
	if sink == nil || !call.Manifest().ValidAt(b.config.Now()) || !contains(call.Manifest().Routes(), "/v1/chat/completions") {
		return 0, errors.New("invalid or expired inference capability")
	}
	payload, err := encodeBoundRequest(call)
	if err != nil {
		return 0, err
	}
	response, err := b.do(ctx, http.MethodPost, call.Target(), call.Manifest(), "/v1/chat/completions", bytes.NewReader(payload), int64(len(payload)), "application/json", "text/event-stream, application/json")
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, errors.New("provider rejected inference request")
	}
	media := strings.ToLower(strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]))
	if media == "text/event-stream" {
		return b.decodeSSE(ctx, response.Body, sink)
	}
	if media == "application/json" {
		return b.decodeJSON(ctx, response.Body, sink)
	}
	return 0, errors.New("provider returned unsupported content type")
}
func encodeBoundRequest(call domain.BackendCall) ([]byte, error) {
	payload, err := encodeRequest(call.Request().Request())
	if err != nil {
		return nil, err
	}
	var body map[string]any
	if err = json.Unmarshal(payload, &body); err != nil {
		return nil, errors.New("encode provider request binding")
	}
	requestID, ok := call.Request().Request().RequestID()
	if !ok {
		return nil, errors.New("provider request lacks correlation binding")
	}
	body["prudentia_request_binding"] = struct {
		RequestID              string `json:"request_id"`
		ProviderRequestID      string `json:"provider_request_id"`
		PodUID                 string `json:"pod_uid"`
		EndpointEpoch          uint64 `json:"endpoint_epoch"`
		RecoveryEpoch          uint64 `json:"recovery_epoch"`
		ProviderManifestDigest string `json:"provider_manifest_digest"`
	}{
		requestID.Value(), call.ProviderRequestID(), call.Target().Identity().PodUID(),
		call.Target().Identity().EndpointEpoch(), call.Target().Identity().RecoveryEpoch(),
		call.Manifest().PayloadDigestString(),
	}
	return json.Marshal(body)
}

func (b *Backend) do(ctx context.Context, method string, target domain.DispatchTarget, manifest domain.CapabilityManifest, route string, body io.Reader, length int64, contentType, accept string) (*http.Response, error) {
	endpoint, err := url.Parse(target.Endpoint() + route)
	if err != nil {
		return nil, errors.New("invalid provider endpoint")
	}
	tlsConfig := b.config.TLSConfig.Clone()
	tlsConfig.ServerName = ""
	tlsConfig.InsecureSkipVerify = true // verified below without DNS/IP identity
	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return errors.New("provider certificate chain invalid")
		}
		chains, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: tlsConfig.RootCAs, Intermediates: intermediates(state.PeerCertificates[1:]), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
		if err != nil {
			return fmt.Errorf("verify provider trust chain: %w", err)
		}
		state.VerifiedChains = chains
		claims, err := verifyPeerIdentity(state, target.Identity())
		if err != nil {
			return err
		}
		if claims.ManifestID != manifest.ID() || claims.ImageDigest != manifest.ImageDigest() || claims.ProxyDigest != manifest.ProxyDigest() {
			return errors.New("provider manifest claim mismatch")
		}
		return nil
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, ResponseHeaderTimeout: b.config.ResponseHeaderTimeout, DialContext: (&net.Dialer{Timeout: b.config.DialTimeout}).DialContext, DisableCompression: true, DisableKeepAlives: true, MaxResponseHeaderBytes: 32 << 10}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, err
	}
	request.ContentLength = length
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if accept != "" {
		request.Header.Set("Accept", accept)
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	transport.CloseIdleConnections()
	return response, err
}

func intermediates(certs []*x509.Certificate) *x509.CertPool {
	p := x509.NewCertPool()
	for _, cert := range certs {
		p.AddCert(cert)
	}
	return p
}
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func (b *Backend) decodeSSE(ctx context.Context, body io.Reader, sink publichttp.StreamSink) (domain.TerminalProof, error) {
	scanner := bufio.NewScanner(io.LimitReader(body, b.config.MaxResponseBytes+1))
	scanner.Buffer(make([]byte, 4096), b.config.MaxEventBytes)
	events := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			return 0, errors.New("invalid provider stream framing")
		}
		events++
		if events > b.config.MaxEvents {
			return 0, errors.New("provider stream event limit exceeded")
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			event, _ := domain.NewTerminalEventWithProof(domain.TerminalProofProviderFinish)
			if err := sink.Write(ctx, event); err != nil {
				return 0, err
			}
			return domain.TerminalProofProviderFinish, nil
		}
		if err := writeChunk(ctx, []byte(data), sink); err != nil {
			return 0, err
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read provider stream: %w", err)
	}
	return 0, errors.New("provider stream ended without terminal proof")
}

func writeChunk(ctx context.Context, data []byte, sink publichttp.StreamSink) error {
	var chunk streamChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		return errors.New("invalid provider payload")
	}
	for _, choice := range chunk.Choices {
		if choice.Delta.Content != "" {
			event, err := domain.NewDeltaEvent(choice.Delta.Content)
			if err != nil {
				return err
			}
			if err = sink.Write(ctx, event); err != nil {
				return err
			}
		}
	}
	if chunk.Usage != nil {
		event, err := domain.NewUsageEvent(domain.Usage{PromptTokens: chunk.Usage.Prompt, CompletionTokens: chunk.Usage.Completion, TotalTokens: chunk.Usage.Total})
		if err != nil {
			return err
		}
		return sink.Write(ctx, event)
	}
	return nil
}

func (b *Backend) decodeJSON(ctx context.Context, body io.Reader, sink publichttp.StreamSink) (domain.TerminalProof, error) {
	limited := &io.LimitedReader{R: body, N: b.config.MaxResponseBytes + 1}
	data, err := io.ReadAll(limited)
	if err != nil || int64(len(data)) > b.config.MaxResponseBytes {
		return 0, errors.New("provider response exceeds bound")
	}
	if err = writeChunk(ctx, data, sink); err != nil {
		return 0, err
	}
	event, _ := domain.NewTerminalEventWithProof(domain.TerminalProofCompleteNonStreaming)
	if err = sink.Write(ctx, event); err != nil {
		return 0, err
	}
	return domain.TerminalProofCompleteNonStreaming, nil
}

func (b *Backend) Probe(ctx context.Context, target domain.ProbeTarget) (domain.RuntimeHealthObservation, error) {
	if !target.Manifest().ValidAt(b.config.Now()) || !contains(target.Manifest().Routes(), "/health") {
		return domain.RuntimeHealthObservation{}, ErrUnsupported
	}
	response, err := b.do(ctx, http.MethodGet, target.Target(), target.Manifest(), "/health", nil, 0, "", "application/json")
	if err != nil {
		return domain.RuntimeHealthObservation{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return domain.RuntimeHealthObservation{}, errors.New("provider health unknown")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, b.config.MaxResponseBytes+1))
	if err != nil || int64(len(data)) > b.config.MaxResponseBytes {
		return domain.RuntimeHealthObservation{}, errors.New("provider health response invalid")
	}
	if len(bytes.TrimSpace(data)) != 0 {
		var result struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(data, &result) != nil || (result.Status != "ok" && result.Status != "ready") {
			return domain.RuntimeHealthObservation{}, errors.New("provider health unknown")
		}
	}
	return domain.NewRuntimeHealthObservation(target.Target().Identity(), domain.RuntimeHealthResponsive, b.config.Now())
}

func (b *Backend) ScrapeLoad(ctx context.Context, target domain.ProbeTarget) (domain.LoadObservation, error) {
	manifest := target.Manifest()
	if !manifest.ValidAt(b.config.Now()) || !manifest.Supports(domain.CapabilityMetrics) || !contains(manifest.Routes(), "/metrics") {
		return domain.LoadObservation{}, ErrUnsupported
	}
	response, err := b.do(ctx, http.MethodGet, target.Target(), manifest, "/metrics", nil, 0, "", "text/plain")
	if err != nil {
		return domain.LoadObservation{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return domain.LoadObservation{}, errors.New("provider load unknown")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, b.config.MaxResponseBytes+1))
	if err != nil || int64(len(data)) > b.config.MaxResponseBytes {
		return domain.LoadObservation{}, errors.New("provider metrics invalid")
	}
	return parseLoad(data, target.Target().Identity(), manifest.Metrics(), b.config.Now())
}

func parseLoad(data []byte, identity domain.WorkloadIdentity, allowed []string, at time.Time) (domain.LoadObservation, error) {
	values := map[string]uint64{}
	allow := map[string]bool{}
	for _, name := range allowed {
		allow[name] = true
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || !allow[fields[0]] {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 32)
		if err != nil {
			return domain.LoadObservation{}, errors.New("provider metric malformed")
		}
		values[fields[0]] = value
	}
	used, hasUsed := values["vllm:num_requests_running"]
	queued, hasQueued := values["vllm:num_requests_waiting"]
	if !hasUsed && !hasQueued {
		return domain.LoadObservation{}, errors.New("provider load unknown")
	}
	return domain.NewLoadObservation(domain.LoadObservationParams{Identity: identity, ObservedAt: at, UsedSlots: uint32(used), HasUsedSlots: hasUsed, QueueDepth: uint32(queued), HasQueueDepth: hasQueued})
}

func (b *Backend) Terminate(ctx context.Context, req domain.ProviderRequestRef) (domain.ProviderTerminationProof, error) {
	manifest := req.Manifest()
	const route = "/v1/prudentia/terminate"
	now := b.config.Now()
	if b.config.ResolveEndpoint == nil || !manifest.ValidAt(now) || !manifest.Supports(domain.CapabilityTermination) || !contains(manifest.Routes(), route) || !now.Before(req.ExpiresAt()) {
		return domain.ProviderTerminationProof{}, ErrUnsupported
	}
	endpoint, err := b.config.ResolveEndpoint(req.Target())
	if err != nil {
		return domain.ProviderTerminationProof{}, fmt.Errorf("resolve termination endpoint: %w", err)
	}
	target, err := domain.NewDispatchTarget(endpoint, req.Target())
	if err != nil {
		return domain.ProviderTerminationProof{}, errors.New("invalid termination endpoint")
	}
	payload, err := json.Marshal(struct {
		RequestID              string `json:"request_id"`
		ProviderRequestID      string `json:"provider_request_id"`
		PodUID                 string `json:"pod_uid"`
		EndpointEpoch          uint64 `json:"endpoint_epoch"`
		RecoveryEpoch          uint64 `json:"recovery_epoch"`
		ProviderManifestDigest string `json:"provider_manifest_digest"`
	}{req.RequestID(), req.ProviderRequestID(), req.Target().PodUID(), req.Target().EndpointEpoch(), req.Target().RecoveryEpoch(), manifest.PayloadDigestString()})
	if err != nil {
		return domain.ProviderTerminationProof{}, err
	}
	response, err := b.do(ctx, http.MethodPost, target, manifest, route, bytes.NewReader(payload), int64(len(payload)), "application/json", "application/json")
	if err != nil {
		return domain.ProviderTerminationProof{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return domain.ProviderTerminationProof{}, errors.New("provider termination not acknowledged")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, b.config.MaxResponseBytes+1))
	if err != nil || int64(len(data)) > b.config.MaxResponseBytes {
		return domain.ProviderTerminationProof{}, errors.New("provider termination acknowledgement invalid")
	}
	var ack struct {
		RequestID              string `json:"request_id"`
		ProviderRequestID      string `json:"provider_request_id"`
		PodUID                 string `json:"pod_uid"`
		EndpointEpoch          uint64 `json:"endpoint_epoch"`
		RecoveryEpoch          uint64 `json:"recovery_epoch"`
		ProviderManifestDigest string `json:"provider_manifest_digest"`
		Stopped                bool   `json:"stopped"`
		Sequence               uint64 `json:"sequence"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&ack) != nil || decoder.Decode(&struct{}{}) != io.EOF || !ack.Stopped || ack.Sequence == 0 || ack.RequestID != req.RequestID() || ack.ProviderRequestID != req.ProviderRequestID() || ack.PodUID != req.Target().PodUID() || ack.EndpointEpoch != req.Target().EndpointEpoch() || ack.RecoveryEpoch != req.Target().RecoveryEpoch() || ack.ProviderManifestDigest != manifest.PayloadDigestString() {
		return domain.ProviderTerminationProof{}, errors.New("provider termination acknowledgement mismatch")
	}
	return domain.NewProviderTerminationProof(domain.ProviderTerminationProofParams{RequestID: req.RequestID(), ReservationID: req.ProviderRequestID(), Identity: req.Target(), ManifestID: manifest.ID(), AcknowledgementSequence: ack.Sequence, AuthenticatedAcknowledgementHash: acknowledgementHash(data)})
}

func acknowledgementHash(data []byte) [32]byte { return sha256.Sum256(data) }
