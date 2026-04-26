# go-gcp-pubsub-idempotency

oneweave-go-pubsub-idempotency is a small Go library that helps ensure Pub/Sub message handlers process each message ID only once after successful completion.

Behavior:
- On message arrival, the ID is claimed in the Store.
- If an ID is already marked successful, future attempts are skipped.
- If an ID is currently claimed, concurrent attempts return in-progress.
- If processing fails, the claim is released so the message can be retried.
- On success, the ID is marked complete and remains deduplicated.

This follows the idempotency pattern described in the OneUptime Cloud Run push subscriber example: process once, mark success only after successful completion, and allow retries for transient failures.

## Install

```bash
go get github.com/oneweave/oneweave-go-pubsub-idempotency
```

## Quick Start

```go
store := pubsubidempotency.NewMemoryStore()
guard := pubsubidempotency.NewGuard(store)

outcome, err := guard.Execute(ctx, messageID, func(ctx context.Context) error {
    // Your business logic here.
    return nil
})
if err != nil {
    // Processing or completion failed; retries are allowed after release.
}

switch outcome {
case pubsubidempotency.OutcomeProcessed:
    // Work executed and marked successful.
case pubsubidempotency.OutcomeDuplicate:
    // Already successfully processed before.
case pubsubidempotency.OutcomeInProgress:
    // The ID is already in-progress in the Store state.
}
```

## Notes

- Concurrency and deduplication guarantees are provided by the Store implementation.
- The built-in MemoryStore provides process-local guarantees only.
- For multi-instance deployments, implement Store with shared persistence and atomic claim semantics.

## Store Contract

Any custom Store must implement this interface behavior contract:

```go
type Store interface {
    Claim(ctx context.Context, messageID string) (ClaimResult, error)
    Complete(ctx context.Context, messageID string) error
    Release(ctx context.Context, messageID string) error
}
```

Required semantics:
- Claim must be atomic per message ID.
- Claim returns started only when the ID had no prior known state and was transitioned to in_progress.
- Claim returns duplicate when the ID is already finalized as successfully processed.
- Claim returns in_progress when the ID is already known as in-progress.
- Complete must finalize only successful processing and keep the ID deduplicated for future attempts.
- Release must remove only in-progress state so retries can happen after failures.
- Release must not remove finalized success state.
- Implementations should be safe for concurrent use.

Consistency guidance for distributed stores:
- Use a single-row/document key per message ID.
- Use compare-and-set/transactions (or equivalent) for Claim and Complete transitions.
- Store at least two states: in_progress and processed.
- Prefer setting a lease/TTL for in_progress to recover from worker crashes.

## Development

```bash
./test.sh
./lint.sh
```
