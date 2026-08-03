package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func capabilityTestAAD(t *testing.T) CapabilityAAD {
	t.Helper()
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{Cluster: "cluster", Namespace: "namespace", LogicalEngine: "engine", PodUID: "pod-a", EndpointEpoch: 3, RecoveryEpoch: 7})
	if err != nil {
		t.Fatal(err)
	}
	return CapabilityAAD{ReservationID: "reservation-a", Generation: 2, OwnerAttempt: sha256.Sum256([]byte("attempt")), Identity: identity}
}

func TestLocalCapabilityKeyringEnvelopeBindsAllAAD(t *testing.T) {
	kek1 := bytes.Repeat([]byte{1}, 32)
	kek2 := bytes.Repeat([]byte{2}, 32)
	comparison1 := bytes.Repeat([]byte{3}, 32)
	comparison2 := bytes.Repeat([]byte{4}, 32)
	keyring, err := NewLocalCapabilityKeyring(map[uint32][]byte{1: kek1, 2: kek2}, map[uint32][]byte{1: comparison1, 2: comparison2})
	if err != nil {
		t.Fatal(err)
	}
	// Constructor ownership must not depend on mutable caller buffers.
	clear(kek2)
	clear(comparison2)
	plaintext := bytes.Repeat([]byte{9}, 32)
	aad := capabilityTestAAD(t)
	sealed, err := keyring.Seal(context.Background(), plaintext, aad, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := keyring.Open(context.Background(), sealed, aad)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatal("recovered capability differs")
	}
	matches, err := keyring.Matches(context.Background(), plaintext, sealed, aad)
	if err != nil || !matches {
		t.Fatalf("comparison = %v, %v", matches, err)
	}
	changed := aad
	changed.Generation++
	if _, err := keyring.Open(context.Background(), sealed, changed); err == nil {
		t.Fatal("changed generation decrypted capability")
	}
	if matches, err := keyring.Matches(context.Background(), plaintext, sealed, changed); err != nil || matches {
		t.Fatalf("AAD-mismatched comparison = %v, %v", matches, err)
	}
	if !keyring.CanRead(1, 1) || !keyring.CanRead(2, 2) {
		t.Fatal("retained read versions unavailable")
	}
}

func TestLocalCapabilityKeyringRejectsUnknownVersions(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	keyring, err := NewLocalCapabilityKeyring(map[uint32][]byte{1: key}, map[uint32][]byte{1: key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keyring.Seal(context.Background(), bytes.Repeat([]byte{2}, 32), capabilityTestAAD(t), 2, 1); err == nil {
		t.Fatal("unknown write version accepted")
	}
}

func TestCatalogFacetsShareOneAuthority(t *testing.T) {
	catalog := new(Catalog)
	if catalog.SchedulerStore().Catalog != catalog {
		t.Fatal("scheduler facet created a second authority")
	}
	if catalog.ControllerCatalog().Catalog != catalog {
		t.Fatal("controller facet created a second authority")
	}
}
