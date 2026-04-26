package pubsubidempotency

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Outcome describes what happened when Execute was called.
type Outcome string

const (
	OutcomeProcessed  Outcome = "processed"
	OutcomeDuplicate  Outcome = "duplicate"
	OutcomeInProgress Outcome = "in_progress"
)

// Processor is the business handler for a message.
type Processor func(ctx context.Context) error

// ClaimResult is the outcome of a store claim attempt.
type ClaimResult string

const (
	ClaimResultStarted    ClaimResult = "started"
	ClaimResultDuplicate  ClaimResult = "duplicate"
	ClaimResultInProgress ClaimResult = "in_progress"
)

// Store persists processed message IDs.
type Store interface {
	// Claim records message arrival and atomically transitions unknown IDs to
	// in-progress. It returns whether processing can start, should be skipped as
	// duplicate, or is already in progress.
	Claim(ctx context.Context, messageID string) (ClaimResult, error)
	// Complete marks an in-progress message as successfully processed.
	Complete(ctx context.Context, messageID string) error
	// Release removes an in-progress claim so the message can be retried.
	Release(ctx context.Context, messageID string) error
}

// Guard coordinates deduplication and concurrent execution control via Store state.
type Guard struct {
	store Store
}

// NewGuard creates an idempotency guard.
func NewGuard(store Store) *Guard {
	if store == nil {
		store = NewMemoryStore()
	}
	return &Guard{store: store}
}

// Execute runs processor only when the message is not already successful and
// not currently being processed in this process.
func (g *Guard) Execute(ctx context.Context, messageID string, processor Processor) (Outcome, error) {
	id := strings.TrimSpace(messageID)
	if id == "" {
		return "", fmt.Errorf("message ID is required")
	}
	if processor == nil {
		return "", fmt.Errorf("processor is required")
	}

	claimResult, err := g.store.Claim(ctx, id)
	if err != nil {
		return "", fmt.Errorf("claim message %s: %w", id, err)
	}

	if claimResult == ClaimResultInProgress {
		return OutcomeInProgress, nil
	}
	if claimResult == ClaimResultDuplicate {
		return OutcomeDuplicate, nil
	}
	if claimResult != ClaimResultStarted {
		return "", fmt.Errorf("unexpected claim result: %s", claimResult)
	}

	if err := processor(ctx); err != nil {
		releaseErr := g.store.Release(ctx, id)
		if releaseErr != nil {
			return "", fmt.Errorf("process message %s failed and release failed: %w", id, errors.Join(err, releaseErr))
		}
		return "", fmt.Errorf("process message %s: %w", id, err)
	}

	if err := g.store.Complete(ctx, id); err != nil {
		releaseErr := g.store.Release(ctx, id)
		if releaseErr != nil {
			return "", fmt.Errorf("complete message %s failed and release failed: %w", id, errors.Join(err, releaseErr))
		}
		return "", fmt.Errorf("complete message %s: %w", id, err)
	}

	return OutcomeProcessed, nil
}
