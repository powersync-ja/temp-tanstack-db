---
id: OfflineTransactorModeOptions
title: OfflineTransactorModeOptions
---

# Type Alias: OfflineTransactorModeOptions

```ts
type OfflineTransactorModeOptions = object;
```

Local-first transactor mode options.

## Properties

### mode?

```ts
optional mode: TransactorMode.OFFLINE;
```

Resolve after the local write has been observed by TanStack DB.

This is the default when `mode` is omitted.
