---
id: TransactorModeOptions
title: TransactorModeOptions
---

# Type Alias: TransactorModeOptions

```ts
type TransactorModeOptions =
  | OfflineTransactorModeOptions
  | OnlineTransactorModeOptions;
```

Shared transactor mode options.

This lower-level discriminated union is used both by
[`PowerSyncTransactor`](../classes/PowerSyncTransactor.md) and by collection
options that configure the default transactor.
