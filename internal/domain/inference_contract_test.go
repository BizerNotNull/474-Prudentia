package domain

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestInferenceContractCopiesMutableValues(t *testing.T) {
	extensions := map[string]string{"revision": "a"}
	input, err := NewInput(InputParams{Messages: []MessageParams{{Role: "user", Content: "hello"}}, Extensions: extensions})
	if err != nil { t.Fatal(err) }
	secretBytes := []byte("private-key")
	secret, err := NewSecretString(secretBytes)
	if err != nil { t.Fatal(err) }
	request, err := NewInferenceRequest(InferenceRequestParams{Model: "model-a", Input: input, MaxOutputTokens: 16, Priority: PriorityHigh, Features: EmptyFeatureSet(), CachePolicy: CachePolicyPrefer, ExecutionBudget: time.Second, IdempotencyKey: secret})
	if err != nil { t.Fatal(err) }
	extensions["revision"] = "mutated"
	secretBytes[0] = 'X'
	copyOfInput := request.Input()
	copyOfExtensions := copyOfInput.Extensions()
	copyOfExtensions["revision"] = "also-mutated"
	if got := request.Input().Extensions()["revision"]; got != "a" { t.Fatalf("extension changed through alias: %q", got) }
	gotSecret, ok := request.IdempotencyKey()
	if !ok || !bytes.Equal(gotSecret.Bytes(), []byte("private-key")) { t.Fatalf("secret changed through alias") }
	copyBytes := gotSecret.Bytes(); copyBytes[0] = 'X'
	if bytes.Equal(requestSecretBytes(request), copyBytes) { t.Fatal("secret accessor exposed backing bytes") }
	if strings.Contains(fmt.Sprintf("%v %#v", gotSecret, gotSecret), "private-key") { t.Fatal("secret formatting exposed plaintext") }
}

func TestStreamEventContractVariantsAndCopies(t *testing.T) {
	data := []byte("fragment")
	delta, err := NewOutputDelta(data)
	if err != nil { t.Fatal(err) }
	event, err := NewStreamEvent(StreamEventParams{Kind: StreamEventDelta, Delta: delta})
	if err != nil { t.Fatal(err) }
	data[0] = 'X'
	copyBytes := event.OutputDelta().Bytes(); copyBytes[0] = 'Y'
	if event.Delta() != "fragment" { t.Fatalf("delta changed through alias: %q", event.Delta()) }
	for _, proof := range []TerminalProof{TerminalProofProviderFinish, TerminalProofCompleteNonStreaming, TerminalProofNotSent, TerminalProofAuthenticatedProviderTermination} {
		terminal, err := NewTerminalEventWithProof(proof)
		if err != nil { t.Fatalf("proof %d: %v", proof, err) }
		if got, ok := terminal.TerminalProof(); !ok || got != proof { t.Fatalf("proof round trip = %d, %t", got, ok) }
	}
	if _, err := NewTerminalEventWithProof(0); err == nil { t.Fatal("zero terminal proof accepted") }
	if _, err := NewStreamEvent(StreamEventParams{Kind: 99}); err == nil { t.Fatal("unknown event kind accepted") }
}

func TestFeatureSetFailsClosed(t *testing.T) {
	if _, err := NewFeatureSet(99, 0); err == nil { t.Fatal("unknown feature version accepted") }
	if _, err := NewFeatureSet(FeatureVersion1, 1<<63); err == nil { t.Fatal("unknown feature bit accepted") }
}

func requestSecretBytes(request InferenceRequest) []byte {
	secret, _ := request.IdempotencyKey()
	return secret.Bytes()
}
