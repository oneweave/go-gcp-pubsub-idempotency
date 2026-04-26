# Oneweave PubSub Idempotency Agent Guide

Treat [README.md](README.md) as the primary usage and package overview.

## Scope

- This repository is a Go library, not an application service.
- Keep APIs small and focused on idempotent message processing guards.
- Preserve backwards compatibility for exported symbols.

## Package Boundaries

- `idempotency.go`: guard execution flow that coordinates `Claim -> processor -> Complete/Release`.
- `memory_store.go`: in-memory processed-state store for local/single-process usage.
- `idempotency_test.go`: behavior and concurrency tests for dedupe/retry guarantees.

## Idempotency Conventions

- Claim an ID in the Store on arrival before running handler logic.
- Mark a message ID as processed only after successful handler completion.
- Release in-progress state on handler failure so retries are possible.
- If completion persistence fails, release in-progress state and return an error.
- Treat concurrency and deduplication guarantees as Store semantics, not guard-local memory.
- Keep persistence concerns behind the `Store` interface.

## Go Conventions

- Propagate caller context; avoid introducing `context.Background()` in library paths.
- Handle and return errors explicitly.
- Wrap propagated errors with `fmt.Errorf(... %w ...)`.

## Validation

Run before handoff:

```bash
./test.sh
```

```
./lint.sh
```