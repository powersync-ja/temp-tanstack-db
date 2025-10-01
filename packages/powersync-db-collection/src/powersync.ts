import { DiffTriggerOperation } from "@powersync/common"
import { asPowerSyncRecord, mapOperation } from "./helpers"
import { PendingOperationStore } from "./PendingOperationStore"
import { PowerSyncTransactor } from "./PowerSyncTransactor"
import type {
  EnhancedPowerSyncCollectionConfig,
  PowerSyncCollectionConfig,
  PowerSyncCollectionUtils,
} from "./definitions"
import type { PendingOperation } from "./PendingOperationStore"
import type {
  CollectionConfig,
  InferSchemaOutput,
  SyncConfig,
} from "@tanstack/db"
import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { TriggerDiffRecord } from "@powersync/common"

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
): EnhancedPowerSyncCollectionConfig<T, TSchema> {
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
  const pendingOperationStore = PendingOperationStore.GLOBAL
  const trackedTableName = `__${tableName}_tracking`

  const transactor = new PowerSyncTransactor<T>({
    database,
  })

  /**
   * "sync"
   * Notice that this describes the Sync between the local SQLite table
   * and the in-memory tanstack-db collection.
   * It is not about sync between a client and a server!
   */
  const sync: SyncConfig<T, string> = {
    sync: async (params) => {
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
                  tableName,
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
            for (const row of await context.getAll<T>(
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

  const getKey = (record: T) => asPowerSyncRecord(record).id

  const outputConfig: EnhancedPowerSyncCollectionConfig<T, TSchema> = {
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
      getMeta: () => ({
        tableName,
        trackedTableName,
      }),
    },
  }
  return outputConfig
}
