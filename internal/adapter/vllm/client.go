package vllm

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	requestapp "github.com/BizerNotNull/474-Prudentia/internal/request"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

type Client struct {
	tlsConfig      *tls.Config
	trustDomain    string
	responseHeader time.Duration
	maxEventBytes  int
	maxEvents      int
}

func NewClient(tlsConfig *tls.Config, trustDomain string, responseHeader time.Duration, maxEventBytes, maxEvents int) (*Client, error) {
	if tlsConfig == nil || tlsConfig.RootCAs == nil || len(tlsConfig.Certificates) == 0 || trustDomain == "" || responseHeader <= 0 || maxEventBytes < 1024 || maxEvents < 1 {
		return nil, errors.New("invalid vLLM client configuration")
	}
	return &Client{tlsConfig: tlsConfig.Clone(), trustDomain: trustDomain, responseHeader: responseHeader, maxEventBytes: maxEventBytes, maxEvents: maxEvents}, nil
}

func (c *Client) Infer(ctx context.Context, target domain.DispatchTarget, authorized domain.AuthorizedRequest, sink publichttp.StreamSink) error {
	endpoint, err := url.Parse(target.Endpoint() + "/v1/chat/completions")
	if err != nil {
		return requestapp.NewNotSentError(errors.New("invalid dispatch endpoint"))
	}
	expectedID, err := target.Identity().SPIFFEID(c.trustDomain)
	if err != nil {
		return requestapp.NewNotSentError(err)
	}

	tlsConfig := c.tlsConfig.Clone()
	tlsConfig.ServerName = endpoint.Hostname()
	tlsConfig.MinVersion = tls.VersionTLS13
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return errors.New("unverified provider identity")
		}
		for _, identity := range state.PeerCertificates[0].URIs {
			if identity.String() == expectedID.String() {
				return nil
			}
		}
		return errors.New("provider identity mismatch")
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, ResponseHeaderTimeout: c.responseHeader,
		DialContext:        (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		DisableCompression: true, MaxIdleConns: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 10 * time.Second,
	}
	defer transport.CloseIdleConnections()

	payload, err := encodeRequest(authorized.Request())
	if err != nil {
		return requestapp.NewNotSentError(err)
	}
	tracked := &trackingReader{reader: bytes.NewReader(payload)}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), io.NopCloser(tracked))
	if err != nil {
		return requestapp.NewNotSentError(err)
	}
	httpRequest.ContentLength = int64(len(payload))
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")

	response, err := transport.RoundTrip(httpRequest)
	if err != nil {
		if tracked.bytesRead.Load() == 0 {
			return requestapp.NewNotSentError(err)
		}
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("provider rejected request")
	}
	return c.decodeStream(ctx, response.Body, sink)
}

type requestMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model         string           `json:"model"`
	Messages      []requestMessage `json:"messages"`
	MaxTokens     uint32           `json:"max_tokens"`
	Stream        bool             `json:"stream"`
	StreamOptions streamOptions    `json:"stream_options"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

func encodeRequest(request domain.InferenceRequest) ([]byte, error) {
	messages := request.Messages()
	wireMessages := make([]requestMessage, len(messages))
	for i, message := range messages {
		wireMessages[i] = requestMessage{Role: message.Role(), Content: message.Content()}
	}
	payload, err := json.Marshal(chatRequest{Model: request.Model(), Messages: wireMessages, MaxTokens: request.MaxCompletionTokens(), Stream: true, StreamOptions: streamOptions{IncludeUsage: true}})
	if err != nil {
		return nil, errors.New("encode provider request")
	}
	return payload, nil
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *struct {
		Prompt     uint32 `json:"prompt_tokens"`
		Completion uint32 `json:"completion_tokens"`
		Total      uint32 `json:"total_tokens"`
	} `json:"usage"`
}

func (c *Client) decodeStream(ctx context.Context, body io.Reader, sink publichttp.StreamSink) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), c.maxEventBytes)
	events := 0
	terminal := false
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data: ") {
			return errors.New("invalid provider stream")
		}
		events++
		if events > c.maxEvents {
			return errors.New("provider stream event limit exceeded")
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			if err := sink.Write(ctx, domain.NewTerminalEvent()); err != nil {
				return err
			}
			terminal = true
			break
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return errors.New("invalid provider stream payload")
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content == "" {
				continue
			}
			event, err := domain.NewDeltaEvent(choice.Delta.Content)
			if err != nil {
				return errors.New("invalid provider delta")
			}
			if err := sink.Write(ctx, event); err != nil {
				return err
			}
		}
		if chunk.Usage != nil {
			event, err := domain.NewUsageEvent(domain.Usage{PromptTokens: chunk.Usage.Prompt, CompletionTokens: chunk.Usage.Completion, TotalTokens: chunk.Usage.Total})
			if err != nil {
				return errors.New("invalid provider usage")
			}
			if err := sink.Write(ctx, event); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read provider stream: %w", err)
	}
	if !terminal {
		return errors.New("provider stream ended without terminal proof")
	}
	return nil
}

type trackingReader struct {
	reader    io.Reader
	bytesRead atomic.Int64
}

func (r *trackingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytesRead.Add(int64(n))
	return n, err
}
