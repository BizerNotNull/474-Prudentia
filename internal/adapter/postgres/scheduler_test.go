package postgres

import (
	"errors"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

func TestWriteCandidateLookupRejectsInvalidCommand(t *testing.T) {
	var command domain.ScheduleCommand

	if _, err := lookupWriteCandidate(command); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("lookup write candidate error = %v, want %v", err, domain.ErrInvalidState)
	}
	if _, err := digestWriteCandidate(command); !errors.Is(err, domain.ErrInvalidState) {
		t.Fatalf("digest write candidate error = %v, want %v", err, domain.ErrInvalidState)
	}
}
