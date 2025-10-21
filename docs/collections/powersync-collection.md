---
title: PowerSync Collection
---

# PowerSync Collection

PowerSync collections provide seamless integration between TanStack DB and [PowerSync](https://powersync.com), enabling automatic synchronization between your in-memory TanStack DB collections and PowerSync's SQLite database. This gives you offline-ready persistence, real-time sync capabilities, and powerful conflict resolution.

## Overview

The `@tanstack/powersync-db-collection` package allows you to create collections that:

- Automatically mirror the state of an underlying PowerSync SQLite database
- Reactively update when PowerSync records change
- Support optimistic mutations with rollback on error
- Provide persistence handlers to keep PowerSync in sync with TanStack DB transactions
- Use PowerSync's efficient SQLite-based storage engine
- Work with PowerSync's real-time sync features for offline-first scenarios
- Leverage PowerSync's built-in conflict resolution and data consistency guarantees
- Enable real-time synchronization with PostgreSQL, MongoDB and MySQL backends

## 1. Installation

Install the PowerSync collection package along with your preferred framework integration.
PowerSync currently works with Web, React Native and Node.js. The examples below use the Web SDK.
See the PowerSync quickstart [docs](https://docs.powersync.com/installation/quickstart-guide) for more details.

```bash
npm install @tanstack/powersync-db-collection @powersync/web @journeyapps/wa-sqlite
```

### 2. Create a PowerSync Database and Schema

```ts
import { Schema, Table, column } from "@powersync/web"

// Define your schema
const APP_SCHEMA = new Schema({
  documents: new Table({
    name: column.text,
    content: column.text,
    created_at: column.text,
    updated_at: column.text,
  }),
})

type Document = (typeof APP_SCHEMA)["types"]["documents"]

// Initialize PowerSync database
const db = new PowerSyncDatabase({
  database: {
    dbFilename: "app.sqlite",
  },
  schema: APP_SCHEMA,
})
```

### 3. (optional) Configure Sync with a Backend

```ts
import {
  AbstractPowerSyncDatabase,
  PowerSyncBackendConnector,
  PowerSyncCredentials,
} from "@powersync/web"

// TODO implement your logic here
class Connector implements PowerSyncBackendConnector {
  fetchCredentials: () => Promise<PowerSyncCredentials | null>

  /** Upload local changes to the app backend.
   *
   * Use {@link AbstractPowerSyncDatabase.getCrudBatch} to get a batch of changes to upload.
   *
   * Any thrown errors will result in a retry after the configured wait period (default: 5 seconds).
   */
  uploadData: (database: AbstractPowerSyncDatabase) => Promise<void>
}

// Configure the client to connect to a PowerSync service and your backend
db.connect(new Connector())
```

### 4. Create a TanStack DB Collection

There are two ways to create a collection: using type inference or using schema validation.

#### Option 1: Using Table Type Inference

The collection types are automatically inferred from the PowerSync Schema Table definition. The table is used to construct a default StandardSchema validator which is used internally to validate collection data and operations.

```ts
import { createCollection } from "@tanstack/react-db"
import { powerSyncCollectionOptions } from "@tanstack/powersync-db-collection"

const documentsCollection = createCollection(
  powerSyncCollectionOptions({
    database: db,
    table: APP_SCHEMA.props.documents,
  })
)
```

#### Option 2: Using Schema Validation

TODO

```ts
import { createCollection } from "@tanstack/react-db"
import {
  powerSyncCollectionOptions,
  convertPowerSyncSchemaToSpecs,
} from "@tanstack/powersync-db-collection"

// Convert PowerSync schema to TanStack DB schema
const schemas = convertPowerSyncSchemaToSpecs(APP_SCHEMA)

const documentsCollection = createCollection(
  powerSyncCollectionOptions({
    database: db,
    tableName: "documents",
    schema: schemas.documents, // Use schema for runtime type validation
  })
)
```

With schema validation, the collection will validate all inputs at runtime to ensure they match the PowerSync schema types. This provides an extra layer of type safety beyond TypeScript's compile-time checks.

## Features

### Offline-First

PowerSync collections are offline-first by default. All data is stored locally in a SQLite database, allowing your app to work without an internet connection. Changes are automatically synced when connectivity is restored.

### Real-Time Sync

When connected to a PowerSync backend, changes are automatically synchronized in real-time across all connected clients. The sync process handles:

- Bi-directional sync with the server
- Conflict resolution
- Queue management for offline changes
- Automatic retries on connection loss

### Optimistic Updates

Updates to the collection are applied optimistically to the local state first, then synchronized with PowerSync and the backend. If an error occurs during sync, the changes are automatically rolled back.

## Configuration Options

The `powerSyncCollectionOptions` function accepts the following options:

```ts
interface PowerSyncCollectionConfig<T> {
  database: PowerSyncDatabase // PowerSync database instance
  tableName: string // Name of the table in PowerSync
  schema?: Schema // Optional schema for validation
}
```

## Advanced Transactions

When you need more control over transaction handling, such as batching multiple operations or handling complex transaction scenarios, you can use PowerSync's transaction system directly with TanStack DB transactions.

```ts
import { createTransaction } from "@tanstack/react-db"
import { PowerSyncTransactor } from "@tanstack/powersync-db-collection"

// Create a transaction that won't auto-commit
const batchTx = createTransaction({
  autoCommit: false,
  mutationFn: async ({ transaction }) => {
    // Use PowerSyncTransactor to apply the transaction to PowerSync
    await new PowerSyncTransactor({ database: db }).applyTransaction(
      transaction
    )
  },
})

// Perform multiple operations in the transaction
batchTx.mutate(() => {
  // Add multiple documents in a single transaction
  for (let i = 0; i < 5; i++) {
    documentsCollection.insert({
      id: crypto.randomUUID(),
      name: `Document ${i}`,
      content: `Content ${i}`,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
  }
})

// Commit the transaction
await batchTx.commit()

// Wait for the changes to be persisted
await batchTx.isPersisted.promise
```

This approach allows you to:

- Batch multiple operations into a single transaction
- Control when the transaction is committed
- Ensure all operations are atomic
- Wait for persistence confirmation
- Handle complex transaction scenarios
