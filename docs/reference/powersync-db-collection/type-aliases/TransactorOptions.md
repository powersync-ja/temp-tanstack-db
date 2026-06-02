---
id: TransactorOptions
title: TransactorOptions
---

# Type Alias: TransactorOptions

```ts
type TransactorOptions = OfflineTransactorOptions | OnlineTransactorOptions;
```

Configuration for a [`PowerSyncTransactor`](../classes/PowerSyncTransactor.md).

`mode` is the discriminator:

- omit `mode` or use [`TransactorMode.OFFLINE`](../enumerations/TransactorMode.md#offline) for fast local-first writes
- use [`TransactorMode.ONLINE`](../enumerations/TransactorMode.md#online) to unlock backend-confirmed wait options

## Example

```typescript
new PowerSyncTransactor({
  database: db,
})
```

## Example

```typescript
new PowerSyncTransactor({
  database: db,
  mode: TransactorMode.ONLINE,
  timeoutMs: 30_000,
})
```
