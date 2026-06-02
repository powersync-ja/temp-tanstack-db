---
id: TransactorMode
title: TransactorMode
---

# Enumeration: TransactorMode

Controls when a [`PowerSyncTransactor`](../classes/PowerSyncTransactor.md)
considers a TanStack DB mutation transaction complete.

## Enumeration Members

### OFFLINE

```ts
OFFLINE = "offline"
```

Resolve mutation transactions once the local PowerSync SQLite write has been
observed by TanStack DB.

This is the default mode. It gives fast local-first behavior: mutations are
persisted locally, PowerSync can upload them later, and TanStack DB state is
already consistent with the local database when the transaction resolves.

***

### ONLINE

```ts
ONLINE = "online"
```

> Experimental: This mode depends on PowerSync checkpoint internals and may
> change as the PowerSync SDK exposes more direct backend-acknowledgement hooks.

Resolve mutation transactions only after PowerSync has uploaded the local write
to the backend and the resulting change has been synced back down and observed
by TanStack DB.

Use this mode when callers need backend confirmation before treating a mutation
as complete, such as when showing committed server state, navigating away from a
critical workflow, or coordinating with systems that only react after the backend
has accepted the write.

Because this waits for a full upload and sync-down cycle, transactions can take
longer to resolve and may remain pending while the client is offline or the
PowerSync connection is unable to complete a checkpoint.
