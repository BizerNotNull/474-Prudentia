//go:build integration

package integration_test

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

func TestColdInferenceThroughExactIdentityProxy(t *testing.T) {
	root := repositoryRoot(t)
	simulatorEndpoint, simulatorDigest := startInferenceSimulator(t, root)
	paths, manifest := newColdPaths(t, simulatorDigest)
	pool, databaseURL := startColdPostgres(t, root)

	proxyAddress := reserveAddress(t)
	proxy := startProcess(t, "identity-proxy", paths.proxyBinary, map[string]string{
		"PRUDENTIA_PROXY_LISTEN":                     proxyAddress,
		"PRUDENTIA_PROXY_UPSTREAM":                   simulatorEndpoint,
		"PRUDENTIA_PROXY_TLS_CERT":                   paths.proxyCert,
		"PRUDENTIA_PROXY_TLS_KEY":                    paths.proxyKey,
		"PRUDENTIA_PROXY_SERVER_CA":                  paths.caFile,
		"PRUDENTIA_PROXY_GATEWAY_CLIENT_CA":          paths.caFile,
		"PRUDENTIA_PROXY_ALLOWED_GATEWAY_SPIFFE_IDS": coldGatewaySPIFFEID,
		"PRUDENTIA_PROXY_MANIFEST_ID":                coldManifestID,
		"PRUDENTIA_PROXY_PROVIDER_IMAGE_DIGEST":      manifest.providerDigest,
		"PRUDENTIA_PROXY_IMAGE_DIGEST":               manifest.proxyDigest,
		"PRUDENTIA_CLUSTER":                          coldCluster,
		"POD_NAMESPACE":                              coldNamespace,
		"PRUDENTIA_LOGICAL_ENGINE":                   coldEngine,
		"POD_UID":                                    coldPodUID,
		"PRUDENTIA_ENDPOINT_EPOCH":                   strconv.FormatUint(coldEndpointEpoch, 10),
		"PRUDENTIA_RECOVERY_EPOCH":                   strconv.FormatUint(coldRecoveryEpoch, 10),
	})
	waitForTCP(t, "identity-proxy", proxyAddress, proxy)
	seedColdBackend(t, pool, addressURL("https", proxyAddress), manifest)

	schedulerAddress := reserveAddress(t)
	scheduler := startProcess(t, "scheduler", paths.schedulerBinary, map[string]string{
		"PRUDENTIA_DATABASE_URL":             databaseURL,
		"PRUDENTIA_SCHEDULER_CAPABILITY_KEY": base64Key(0x11),
		"PRUDENTIA_SCHEDULER_LISTEN":         schedulerAddress,
		"PRUDENTIA_SCHEDULER_TLS_CERT":       paths.schedulerCert,
		"PRUDENTIA_SCHEDULER_TLS_KEY":        paths.schedulerKey,
		"PRUDENTIA_SCHEDULER_CLIENT_CA":      paths.caFile,
		"PRUDENTIA_GATEWAY_SPIFFE_ID":        coldGatewaySPIFFEID,
	})
	waitForTCP(t, "scheduler", schedulerAddress, scheduler)

	gatewayAddress := reserveAddress(t)
	gateway := startProcess(t, "gateway", paths.gatewayBinary, map[string]string{
		"PRUDENTIA_GATEWAY_LISTEN":                           gatewayAddress,
		"PRUDENTIA_GATEWAY_API_KEY":                          coldAPIKey,
		"PRUDENTIA_GATEWAY_TENANT":                           coldTenant,
		"PRUDENTIA_GATEWAY_MODELS":                           coldModel,
		"PRUDENTIA_SCHEDULER_ADDRESS":                        schedulerAddress,
		"PRUDENTIA_SCHEDULER_SERVER_NAME":                    "scheduler.test",
		"PRUDENTIA_SCHEDULER_CA":                             paths.caFile,
		"PRUDENTIA_GATEWAY_TLS_CERT":                         paths.gatewayClientCert,
		"PRUDENTIA_GATEWAY_TLS_KEY":                          paths.gatewayClientKey,
		"PRUDENTIA_PROVIDER_CA":                              paths.caFile,
		"PRUDENTIA_PROVIDER_TRUST_DOMAIN":                    coldProviderTrustDomain,
		"PRUDENTIA_PROVIDER_MANIFEST_PAYLOAD":                paths.manifestPayload,
		"PRUDENTIA_PROVIDER_MANIFEST_SIGNATURE":              paths.manifestSignature,
		"PRUDENTIA_PROVIDER_MANIFEST_KEY_ID":                 coldManifestKeyID,
		"PRUDENTIA_PROVIDER_MANIFEST_ID":                     coldManifestID,
		"PRUDENTIA_PROVIDER_MANIFEST_PUBLIC_KEY":             manifest.publicKeyBase64,
		"PRUDENTIA_PROVIDER_MANIFEST_PIN":                    manifest.pin,
		"PRUDENTIA_GATEWAY_IDEMPOTENCY_LOOKUP_KEYS":          strconv.Itoa(coldVersions.lookupWrite) + ":" + base64Key(0x21),
		"PRUDENTIA_GATEWAY_IDEMPOTENCY_LOOKUP_WRITE_VERSION": strconv.Itoa(coldVersions.lookupWrite),
		"PRUDENTIA_GATEWAY_REQUEST_DIGEST_KEYS":              strconv.Itoa(coldVersions.digestWrite) + ":" + base64Key(0x31),
		"PRUDENTIA_GATEWAY_REQUEST_DIGEST_WRITE_VERSION":     strconv.Itoa(coldVersions.digestWrite),
	})
	publicClient := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 30 * time.Second}
	gatewayURL := addressURL("http", gatewayAddress)
	waitForHTTP(t, "gateway", gatewayURL+"/readyz", publicClient, gateway)
	requireNoProcessExit(t, proxy, scheduler, gateway)

	client := openai.NewClient(
		option.WithBaseURL(gatewayURL+"/v1"),
		option.WithAPIKey(coldAPIKey),
		option.WithHTTPClient(publicClient),
	)

	t.Run("bounded nonstreaming", func(t *testing.T) {
		completion, err := client.Chat.Completions.New(t.Context(), openai.ChatCompletionNewParams{
			Model: coldModel,
			Messages: []openai.ChatCompletionMessageParamUnion{
				openai.UserMessage("cold path nonstream probe"),
			},
			MaxCompletionTokens: openai.Int(16),
		})
		if err != nil {
			t.Fatalf("create nonstreaming completion: %v\nledger: %s\nscheduler:\n%s\ngateway:\n%s\nproxy:\n%s", err, coldLedgerState(t, pool), scheduler.logs.String(), gateway.logs.String(), proxy.logs.String())
		}
		if len(completion.Choices) != 1 || strings.TrimSpace(completion.Choices[0].Message.Content) == "" {
			t.Fatalf("unexpected nonstreaming completion: %#v", completion.Choices)
		}
		assertColdLedgerReleased(t, pool, 1)
	})

	t.Run("streaming", func(t *testing.T) {
		stream := client.Chat.Completions.NewStreaming(t.Context(), openai.ChatCompletionNewParams{
			Model: coldModel,
			Messages: []openai.ChatCompletionMessageParamUnion{
				openai.UserMessage("cold path streaming probe"),
			},
			MaxCompletionTokens: openai.Int(16),
		})
		defer stream.Close()
		var content strings.Builder
		events := 0
		for stream.Next() {
			events++
			chunk := stream.Current()
			for _, choice := range chunk.Choices {
				content.WriteString(choice.Delta.Content)
			}
		}
		if err := stream.Err(); err != nil {
			t.Fatalf("consume streaming completion: %v\nledger: %s\nscheduler:\n%s\ngateway:\n%s\nproxy:\n%s", err, coldLedgerState(t, pool), scheduler.logs.String(), gateway.logs.String(), proxy.logs.String())
		}
		if events == 0 || strings.TrimSpace(content.String()) == "" {
			t.Fatalf("stream produced no completion content: events=%d", events)
		}
		assertColdLedgerReleased(t, pool, 2)
	})

	requireNoProcessExit(t, proxy, scheduler, gateway)
}
