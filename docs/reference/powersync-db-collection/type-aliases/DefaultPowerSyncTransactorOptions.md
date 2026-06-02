---
id: DefaultPowerSyncTransactorOptions
title: DefaultPowerSyncTransactorOptions
---

# Type Alias: DefaultPowerSyncTransactorOptions

```ts
type DefaultPowerSyncTransactorOptions = TransactorModeOptions;
```

Options used by the default [`PowerSyncTransactor`](../classes/PowerSyncTransactor.md)
created by [`powerSyncCollectionOptions`](../functions/powerSyncCollectionOptions.md).

The collection already provides the PowerSync database, so this only accepts the
mode-specific options.

## Example

```typescript
const todos = createCollection(
  powerSyncCollectionOptions({
    database: db,
    table: APP_SCHEMA.props.todos,
    transactor: {
      mode: TransactorMode.ONLINE,
      timeoutMs: 30_000,
    },
  })
)
```
