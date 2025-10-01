import { DiffTriggerOperation } from "@powersync/common"
import { PendingOperationStore } from "./PendingOperationStore"
import { PowerSyncTransactor } from "./PowerSyncTransactor"
import { mapOperation } from "./helpers"
import type { PendingOperation } from "./PendingOperationStore"
import type {
  BaseCollectionConfig,
  CollectionConfig,
  InferSchemaOutput,
  SyncConfig,
  Transaction,
} from "@tanstack/db"
import type {
  AbstractPowerSyncDatabase,
  TriggerDiffRecord,
} from "@powersync/common"
import type { StandardSchemaV1 } from "@standard-schema/spec"

/**
 * Configuration interface for PowerSync collection options
 * @template T - The type of items in the collection
 * @template TSchema - The schema type for validation
 */
/**
 * Configuration options for creating a PowerSync collection.
 *
 * @example
 * ```typescript
 * const APP_SCHEMA = new Schema({
 *   documents: new Table({
 *     name: column.text,
 *   }),
 * })
 *
 * type Document = (typeof APP_SCHEMA)["types"]["documents"]
 *
 * const db = new PowerSyncDatabase({
 *   database: {
 *     dbFilename: "test.sqlite",
 *   },
 *   schema: APP_SCHEMA,
 * })
 *
 * const collection = createCollection(
 *   powerSyncCollectionOptions<Document>({
 *     database: db,
 *     tableName: "documents",
 *   })
 * )
 * ```
 */
export type PowerSyncCollectionConfig<
  T extends object = Record<string, unknown>,
  TSchema extends StandardSchemaV1 = never,
> = Omit<
  BaseCollectionConfig<T, string, TSchema>,
  `onInsert` | `onUpdate` | `onDelete` | `getKey`
> & {
  /** The name of the table in PowerSync database */
  tableName: string
  /** The PowerSync database instance */
  database: AbstractPowerSyncDatabase
}

export type PowerSyncCollectionUtils = {
  /**
   * Applies mutations to the PowerSync database. This method is called automatically by the collection's
   * insert, update, and delete operations. You typically don't need to call this directly unless you
   * have special transaction requirements.
   *
   * @example
   * ```typescript
   * // Create a collection
   * const collection = createCollection(
   *   powerSyncCollectionOptions<Document>({
   *     database: db,
   *     tableName: "documents",
   *   })
   * )
   *
   * const addTx = createTransaction({
   *     autoCommit: false,
   *     mutationFn: async ({ transaction }) => {
   *         await collection.utils.mutateTransaction(transaction)
   *     },
   * })
   *
   * addTx.mutate(() => {
   *     for (let i = 0; i < 5; i++) {
   *        collection.insert({ id: randomUUID(), name: `tx-${i}` })
   *     }
   * })
   *
   * await addTx.commit()
   * await addTx.isPersisted.promise
   * ```
   *
   * @param transaction - The transaction containing mutations to apply
   * @returns A promise that resolves when the mutations have been persisted to PowerSync
   */
  mutateTransaction: (transaction: Transaction) => Promise<void>
}

/**
 * Creates PowerSync collection options for use with a standard Collection
 *
 * @template TExplicit - The explicit type of items in the collection (highest priority)
 * @template TSchema - The schema type for validation and type inference (second priority)
 * @param config - Configuration options for the PowerSync collection
 * @returns Collection options with utilities
 */

// Overload for when schema is provided
/**
 * Creates a PowerSync collection configuration with schema validation.
 *
 * @example
 * ```typescript
 * // With schema validation
 * const APP_SCHEMA = new Schema({
 *   documents: new Table({
 *     name: column.text,
 *   }),
 * })
 *
 * const collection = createCollection(
 *   powerSyncCollectionOptions({
 *     database: db,
 *     tableName: "documents",
 *     schema: APP_SCHEMA,
 *   })
 * )
 * ```
 */
export function powerSyncCollectionOptions<T extends StandardSchemaV1>(
  config: PowerSyncCollectionConfig<InferSchemaOutput<T>, T>
): CollectionConfig<InferSchemaOutput<T>, string, T> & {
  schema: T
  utils: PowerSyncCollectionUtils
}

/**
 * Creates a PowerSync collection configuration without schema validation.
 *
 * @example
 * ```typescript
 * const APP_SCHEMA = new Schema({
 *   documents: new Table({
 *     name: column.text,
 *   }),
 * })
 *
 * type Document = (typeof APP_SCHEMA)["types"]["documents"]
 *
 * const db = new PowerSyncDatabase({
 *   database: {
 *     dbFilename: "test.sqlite",
 *   },
 *   schema: APP_SCHEMA,
 * })
 *
 * const collection = createCollection(
 *   powerSyncCollectionOptions<Document>({
 *     database: db,
 *     tableName: "documents",
 *   })
 * )
 * ```
 */
export function powerSyncCollectionOptions<T extends object>(
  config: PowerSyncCollectionConfig<T> & {
    schema?: never
  }
): CollectionConfig<T, string> & {
  schema?: never
  utils: PowerSyncCollectionUtils
}

/**
 * Implementation of powerSyncCollectionOptions that handles both schema and non-schema configurations.
 */
export function powerSyncCollectionOptions<
  T extends object = Record<string, unknown>,
  TSchema extends StandardSchemaV1 = never,
>(
  config: PowerSyncCollectionConfig<T, TSchema>
): CollectionConfig<T, string, TSchema> & {
  id?: string
  utils: PowerSyncCollectionUtils
  schema?: TSchema
} {
  type Row = Record<string, unknown>
  type Key = string // we always use uuids for keys

  const { database, tableName, ...restConfig } = config

  /**
   * The onInsert, onUpdate, onDelete handlers should only return
   * after we have written the changes to Tanstack DB.
   * We currently only write to Tanstack DB from a diff trigger.
   * We wait for the diff trigger to observe the change,
   * and only then return from the on[X] handlers.
   * This ensures that when the transaction is reported as
   * complete to the caller, the in-memory state is already
   * consistent with the database.
   */
  const pendingOperationStore = new PendingOperationStore()
  const trackedTableName = `__${tableName}_tracking`

  const transactor = new PowerSyncTransactor<T>({
    database,
    pendingOperationStore,
    tableName,
    trackedTableName,
  })

  /**
   * "sync"
   * Notice that this describes the Sync between the local SQLite table
   * and the in-memory tanstack-db collection.
   * It is not about sync between a client and a server!
   */
  type SyncParams = Parameters<SyncConfig<Row, string>[`sync`]>[0]
  const sync: SyncConfig<Row, Key> = {
    sync: async (params: SyncParams) => {
      const { begin, write, commit, markReady } = params

      // Manually create a tracking operation for optimization purposes
      const abortController = new AbortController()

      database.onChangeWithCallback(
        {
          onChange: async () => {
            await database.writeTransaction(async (context) => {
              begin()
              const operations = await context.getAll<TriggerDiffRecord>(
                `SELECT * FROM ${trackedTableName} ORDER BY timestamp ASC`
              )
              const pendingOperations: Array<PendingOperation> = []

              for (const op of operations) {
                const { id, operation, timestamp, value } = op
                const parsedValue = {
                  id,
                  ...JSON.parse(value),
                }
                const parsedPreviousValue =
                  op.operation == DiffTriggerOperation.UPDATE
                    ? { id, ...JSON.parse(op.previous_value) }
                    : null
                write({
                  type: mapOperation(operation),
                  value: parsedValue,
                  previousValue: parsedPreviousValue,
                })
                pendingOperations.push({
                  id,
                  operation,
                  timestamp,
                })
              }

              // clear the current operations
              await context.execute(`DELETE FROM ${trackedTableName}`)

              commit()
              pendingOperationStore.resolvePendingFor(pendingOperations)
            })
          },
        },
        {
          signal: abortController.signal,
          triggerImmediate: false,
          tables: [trackedTableName],
        }
      )

      const disposeTracking = await database.triggers.createDiffTrigger({
        source: tableName,
        destination: trackedTableName,
        when: {
          [DiffTriggerOperation.INSERT]: `TRUE`,
          [DiffTriggerOperation.UPDATE]: `TRUE`,
          [DiffTriggerOperation.DELETE]: `TRUE`,
        },
        hooks: {
          beforeCreate: async (context) => {
            begin()
            for (const row of await context.getAll<Record<string, unknown>>(
              `SELECT * FROM ${tableName}`
            )) {
              write({
                type: `insert`,
                value: row,
              })
            }
            commit()
            markReady()
          },
        },
      })

      return () => {
        abortController.abort()
        disposeTracking()
      }
    },
    // Expose the getSyncMetadata function
    getSyncMetadata: undefined,
  }

  const getKey = (record: Record<string, unknown>) => record.id as string

  return {
    ...restConfig,
    getKey,
    sync,
    onInsert: async (params) => {
      // The transaction here should only ever contain a single insert mutation
      return await transactor.applyTransaction(params.transaction)
    },
    onUpdate: async (params) => {
      // The transaction here should only ever contain a single update mutation
      return await transactor.applyTransaction(params.transaction)
    },
    onDelete: async (params) => {
      // The transaction here should only ever contain a single delete mutation
      return await transactor.applyTransaction(params.transaction)
    },
    utils: {
      mutateTransaction: async (transaction: Transaction<T>) => {
        return await transactor.applyTransaction(transaction)
      },
    },
  } as CollectionConfig<T, string, TSchema> & {
    id?: string
    utils: PowerSyncCollectionUtils
    schema?: TSchema
  }
}
