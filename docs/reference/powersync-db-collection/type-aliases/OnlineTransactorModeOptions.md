---
id: OnlineTransactorModeOptions
title: OnlineTransactorModeOptions
---

# Type Alias: OnlineTransactorModeOptions

```ts
type OnlineTransactorModeOptions = object;
```

Backend-confirmed transactor mode options.

> Experimental: Online transaction completion depends on PowerSync checkpoint
> internals and may change as the PowerSync SDK exposes more direct
> backend-acknowledgement hooks.

## Properties

### mode

```ts
mode: TransactorMode.ONLINE;
```

Resolve after the local write has been uploaded to the backend, synced back down,
and observed by TanStack DB.

> Experimental: This mode depends on PowerSync checkpoint internals and may
> change as the PowerSync SDK exposes more direct backend-acknowledgement hooks.

***

### timeoutMs?

```ts
optional timeoutMs: number;
```

Maximum total time to wait for the backend checkpoint and synced-down diff
records.

If the timeout is reached before the write has completed the upload and sync-down
cycle, the mutation transaction rejects.

> Experimental: This option only applies to the experimental online transaction
> mode.

***

### abortSignal?

```ts
optional abortSignal: AbortSignal;
```

Optional signal for cancelling the online wait.

> Experimental: This option only applies to the experimental online transaction
> mode.
