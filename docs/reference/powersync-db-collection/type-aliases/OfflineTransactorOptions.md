---
id: OfflineTransactorOptions
title: OfflineTransactorOptions
---

# Type Alias: OfflineTransactorOptions

```ts
type OfflineTransactorOptions = BaseTransactorOptions & object;
```

Options for local-first transaction handling.

## Properties

### database

```ts
database: AbstractPowerSyncDatabase;
```

The PowerSync database that mutations will be written to.

***

### mode?

```ts
optional mode: TransactorMode.OFFLINE;
```

Resolve after the local write has been observed by TanStack DB.

This is the default when `mode` is omitted.
