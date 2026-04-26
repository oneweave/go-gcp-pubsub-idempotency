package pubsubidempotency

import (
	"context"
	"fmt"
	"sync"
)

type messageState string

const (
	stateInProgress messageState = "in_progress"
	stateProcessed  messageState = "processed"
)

// MemoryStore is a process-local Store implementation for testing/single process use.
type MemoryStore struct {
	mu     sync.Mutex
	states map[string]messageState
}

// NewMemoryStore creates a new in-memory processed-state store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{states: make(map[string]messageState)}
}

// Claim atomically marks an arriving message as in-progress when eligible.
func (s *MemoryStore) Claim(_ context.Context, messageID string) (ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.states[messageID]
	if !ok {
		s.states[messageID] = stateInProgress
		return ClaimResultStarted, nil
	}

	if state == stateProcessed {
		return ClaimResultDuplicate, nil
	}

	return ClaimResultInProgress, nil
}

// Complete records successful completion for an in-progress message.
func (s *MemoryStore) Complete(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.states[messageID]
	if !ok {
		return fmt.Errorf("message %s has no prior state", messageID)
	}
	if state == stateProcessed {
		return nil
	}

	s.states[messageID] = stateProcessed
	return nil
}

// Release removes an in-progress claim so the message can be retried.
func (s *MemoryStore) Release(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.states[messageID]
	if !ok {
		return nil
	}
	if state == stateProcessed {
		return nil
	}

	delete(s.states, messageID)
	return nil
}
