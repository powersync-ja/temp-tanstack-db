---
id: BasePowerSyncCollectionConfig
title: BasePowerSyncCollectionConfig
---

# Type Alias: BasePowerSyncCollectionConfig\<TTable, TSchema\>

```ts
type BasePowerSyncCollectionConfig<TTable, TSchema> = Omit<BaseCollectionConfig<ExtractedTable<TTable>, string, TSchema>, "onInsert" | "onUpdate" | "onDelete" | "getKey"> & object;
```

Defined in: [definitions.ts:165](https://github.com/TanStack/db/blob/main/packages/powersync-db-collection/src/definitions.ts#L165)

## Type Declaration

### database

```ts
database: AbstractPowerSyncDatabase;
```

The PowerSync database instance

### syncBatchSize?

```ts
optional syncBatchSize: number;
```

The maximum number of documents to read from the SQLite table
in a single batch during the initial sync between PowerSync and the
in-memory TanStack DB collection.

#### Remarks

- Defaults to [DEFAULT\_BATCH\_SIZE](../variables/DEFAULT_BATCH_SIZE.md) if not specified.
- Larger values reduce the number of round trips to the storage
  engine but increase memory usage per batch.
- Smaller values may lower memory usage and allow earlier
  streaming of initial results, at the cost of more query calls.

***

### transactor?

```ts
optional transactor: DefaultPowerSyncTransactorOptions;
```

Defaults for the built-in [`PowerSyncTransactor`](../classes/PowerSyncTransactor.md)
used by collection insert, update, and delete handlers.

Use this to opt the collection's default mutation handlers into a particular
transaction mode. For example, pass
`{ mode: TransactorMode.ONLINE, timeoutMs: 30_000 }` to wait for backend upload
and sync-down confirmation before collection mutations resolve.

This does not affect manually created [`PowerSyncTransactor`](../classes/PowerSyncTransactor.md)
instances.

### table

```ts
table: TTable;
```

The PowerSync schema Table definition

## Type Parameters

### TTable

`TTable` *extends* `Table` = `Table`

### TSchema

`TSchema` *extends* `StandardSchemaV1` = `never`
