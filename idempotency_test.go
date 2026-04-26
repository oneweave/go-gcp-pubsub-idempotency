package pubsubidempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingStore struct {
	states      map[string]messageState
	claimErr    error
	completeErr error
	releaseErr  error
}

func (s *failingStore) Claim(_ context.Context, messageID string) (ClaimResult, error) {
	if s.claimErr != nil {
		return "", s.claimErr
	}

	if s.states == nil {
		s.states = make(map[string]messageState)
	}

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

func (s *failingStore) Complete(_ context.Context, messageID string) error {
	if s.completeErr != nil {
		return s.completeErr
	}

	if s.states == nil {
		s.states = make(map[string]messageState)
	}
	s.states[messageID] = stateProcessed
	return nil
}

func (s *failingStore) Release(_ context.Context, messageID string) error {
	if s.releaseErr != nil {
		return s.releaseErr
	}

	if s.states == nil {
		return nil
	}

	if s.states[messageID] != stateProcessed {
		delete(s.states, messageID)
	}
	return nil
}

func TestExecuteProcessesThenDeduplicates(t *testing.T) {
	guard := NewGuard(NewMemoryStore())
	var calls int32

	outcome, err := guard.Execute(context.Background(), "msg-1", func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, OutcomeProcessed, outcome)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))

	outcome, err = guard.Execute(context.Background(), "msg-1", func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, OutcomeDuplicate, outcome)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestExecuteAllowsRetryAfterProcessorError(t *testing.T) {
	guard := NewGuard(NewMemoryStore())
	var calls int32

	outcome, err := guard.Execute(context.Background(), "msg-2", func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("transient")
	})
	require.Error(t, err)
	assert.Empty(t, outcome)
	assert.Contains(t, err.Error(), "process message msg-2")
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))

	outcome, err = guard.Execute(context.Background(), "msg-2", func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, OutcomeProcessed, outcome)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestExecutePreventsConcurrentProcessingForSameID(t *testing.T) {
	guard := NewGuard(NewMemoryStore())
	started := make(chan struct{})
	release := make(chan struct{})

	var calls int32
	var wg sync.WaitGroup
	wg.Add(1)

	var outcome1 Outcome
	var err1 error
	go func() {
		defer wg.Done()
		outcome1, err1 = guard.Execute(context.Background(), "msg-3", func(context.Context) error {
			atomic.AddInt32(&calls, 1)
			close(started)
			<-release
			return nil
		})
	}()

	<-started

	outcome2, err2 := guard.Execute(context.Background(), "msg-3", func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	require.NoError(t, err2)
	assert.Equal(t, OutcomeInProgress, outcome2)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))

	close(release)
	wg.Wait()
	require.NoError(t, err1)
	assert.Equal(t, OutcomeProcessed, outcome1)

	outcome3, err3 := guard.Execute(context.Background(), "msg-3", func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	require.NoError(t, err3)
	assert.Equal(t, OutcomeDuplicate, outcome3)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestExecuteValidationAndStoreErrors(t *testing.T) {
	guard := NewGuard(NewMemoryStore())

	outcome, err := guard.Execute(context.Background(), "", func(context.Context) error { return nil })
	require.Error(t, err)
	assert.Empty(t, outcome)
	assert.Contains(t, err.Error(), "message ID is required")

	outcome, err = guard.Execute(context.Background(), "msg", nil)
	require.Error(t, err)
	assert.Empty(t, outcome)
	assert.Contains(t, err.Error(), "processor is required")

	storeErr := &failingStore{states: map[string]messageState{}, claimErr: errors.New("db down")}
	guard = NewGuard(storeErr)
	outcome, err = guard.Execute(context.Background(), "msg", func(context.Context) error { return nil })
	require.Error(t, err)
	assert.Empty(t, outcome)
	assert.Contains(t, err.Error(), "claim message msg")

	markErrStore := &failingStore{states: map[string]messageState{}, completeErr: errors.New("write failed")}
	guard = NewGuard(markErrStore)
	outcome, err = guard.Execute(context.Background(), "msg", func(context.Context) error { return nil })
	require.Error(t, err)
	assert.Empty(t, outcome)
	assert.Contains(t, err.Error(), "complete message msg")

	// Completion failure should allow retry because success was never persisted.
	markErrStore.completeErr = nil
	outcome, err = guard.Execute(context.Background(), "msg", func(context.Context) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, OutcomeProcessed, outcome)
}

func TestExecuteFailureWithReleaseError(t *testing.T) {
	store := &failingStore{states: map[string]messageState{}, releaseErr: errors.New("release down")}
	guard := NewGuard(store)

	outcome, err := guard.Execute(context.Background(), "msg-5", func(context.Context) error {
		return errors.New("processor failed")
	})
	require.Error(t, err)
	assert.Empty(t, outcome)
	assert.Contains(t, err.Error(), "process message msg-5 failed and release failed")
}

func TestExecuteReleasesLockAfterFailure(t *testing.T) {
	guard := NewGuard(NewMemoryStore())

	_, err := guard.Execute(context.Background(), "msg-4", func(context.Context) error {
		return errors.New("boom")
	})
	require.Error(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	outcome, err := guard.Execute(ctx, "msg-4", func(context.Context) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, OutcomeProcessed, outcome)
}
