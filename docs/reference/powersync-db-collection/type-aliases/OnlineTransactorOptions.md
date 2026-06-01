---
id: OnlineTransactorOptions
title: OnlineTransactorOptions
---

# Type Alias: OnlineTransactorOptions

```ts
type OnlineTransactorOptions = BaseTransactorOptions & object;
```

Options for backend-confirmed transaction handling.

> Experimental: Online transaction completion depends on PowerSync checkpoint
> internals and may change as the PowerSync SDK exposes more direct
> backend-acknowledgement hooks.

## Properties

### database

```ts
database: AbstractPowerSyncDatabase;
```

The PowerSync database that mutations will be written to.

***

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
