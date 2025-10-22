import { DiffTriggerOperation, sanitizeSQL } from "@powersync/common"
import { DEFAULT_BATCH_SIZE } from "./definitions"
import { asPowerSyncRecord, mapOperation } from "./helpers"
import { PendingOperationStore } from "./PendingOperationStore"
import { PowerSyncTransactor } from "./PowerSyncTransactor"
import { convertTableToSchema } from "./schema"
import type { Table, TriggerDiffRecord } from "@powersync/common"
import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { CollectionConfig, SyncConfig } from "@tanstack/db"
import type {
  EnhancedPowerSyncCollectionConfig,
  PowerSyncCollectionConfig,
  PowerSyncCollectionUtils,
} from "./definitions"
import type { ExtractedTable } from "./helpers"
import type { PendingOperation } from "./PendingOperationStore"

/**
 * Creates PowerSync collection options for use with a standard Collection.
 *
 * @template TTable - The SQLite-based typing
 * @template TSchema - The validation schema type (optionally supports a custom input type)
 * @param config - Configuration options for the PowerSync collection
 * @returns Collection options with utilities
 */

/**
 * Creates a PowerSync collection configuration with basic default validation.
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
 *   powerSyncCollectionOptions({
 *     database: db,
 *     table: APP_SCHEMA.props.documents
 *   })
 * )
 * ```
 */
export function powerSyncCollectionOptions<TTable extends Table = Table>(
  config: PowerSyncCollectionConfig<TTable, never>
): CollectionConfig<ExtractedTable<TTable>, string, never> & {
  utils: PowerSyncCollectionUtils
}

// Overload for when schema is provided
/**
 * Creates a PowerSync collection configuration with schema validation.
 *
 * @example
 * ```typescript
 * import { z } from "zod"
 *
 * // The PowerSync SQLite schema
 * const APP_SCHEMA = new Schema({
 *   documents: new Table({
 *     name: column.text,
 *   }),
 * })
 *
 * // Advanced Zod validations. The output type of this schema
 * // is constrained to the SQLite schema of APP_SCHEMA
 * const schema = z.object({
 *   id: z.string(),
 *   name: z.string().min(3, { message: "Should be at least 3 characters" }).nullable(),
 * })
 *
 * const collection = createCollection(
 *   powerSyncCollectionOptions({
 *     database: db,
 *     table: APP_SCHEMA.props.documents,
 *     schema
 *   })
 * )
 * ```
 */
export function powerSyncCollectionOptions<
  TTable extends Table,
  TSchema extends StandardSchemaV1<
    ExtractedTable<TTable>,
    ExtractedTable<TTable>
  >,
>(
  config: PowerSyncCollectionConfig<TTable, TSchema>
): CollectionConfig<ExtractedTable<TTable>, string, TSchema> & {
  utils: PowerSyncCollectionUtils
  schema: TSchema
}

/**
 * Implementation of powerSyncCollectionOptions that handles both schema and non-schema configurations.
 */
export function powerSyncCollectionOptions<
  TTable extends Table = Table,
  TSchema extends StandardSchemaV1 = never,
>(
  config: PowerSyncCollectionConfig<TTable, TSchema>
): EnhancedPowerSyncCollectionConfig<TTable, TSchema> {
  const {
    database,
    table,
    schema: inputSchema,
    syncBatchSize = DEFAULT_BATCH_SIZE,
    ...restConfig
  } = config

  type RecordType = ExtractedTable<TTable>
  const { viewName } = table

  // We can do basic runtime validations for columns if not explicit schema has been provided
  const schema = inputSchema ?? (convertTableToSchema(table) as TSchema)
  /**
   * The onInsert, onUpdate, and onDelete handlers should only return
   * after we have written the changes to TanStack DB.
   * We currently only write to TanStack DB from a diff trigger.
   * We wait for the diff trigger to observe the change,
   * and only then return from the on[X] handlers.
   * This ensures that when the transaction is reported as
   * complete to the caller, the in-memory state is already
   * consistent with the database.
   */
  const pendingOperationStore = PendingOperationStore.GLOBAL
  // Keep the tracked table unique in case of multiple tabs.
  const trackedTableName = `__${viewName}_tracking_${Math.floor(
    Math.random() * 0xffffffff
  )
    .toString(16)
    .padStart(8, `0`)}`

  const transactor = new PowerSyncTransactor<RecordType>({
    database,
  })

  /**
   * "sync"
   * Notice that this describes the Sync between the local SQLite table
   * and the in-memory tanstack-db collection.
   */
  const sync: SyncConfig<RecordType, string> = {
    sync: (params) => {
      const { begin, write, commit, markReady } = params
      const abortController = new AbortController()

      // The sync function needs to be synchronous
      async function start() {
        database.logger.info(
          `Sync is starting for ${viewName} into ${trackedTableName}`
        )
        database.onChangeWithCallback(
          {
            onChange: async () => {
              await database
                .writeTransaction(async (context) => {
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
                      tableName: viewName,
                    })
                  }

                  // clear the current operations
                  await context.execute(`DELETE FROM ${trackedTableName}`)

                  commit()
                  pendingOperationStore.resolvePendingFor(pendingOperations)
                })
                .catch((error) => {
                  database.logger.error(
                    `An error has been detected in the sync handler`,
                    error
                  )
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
          source: viewName,
          destination: trackedTableName,
          when: {
            [DiffTriggerOperation.INSERT]: `TRUE`,
            [DiffTriggerOperation.UPDATE]: `TRUE`,
            [DiffTriggerOperation.DELETE]: `TRUE`,
          },
          hooks: {
            beforeCreate: async (context) => {
              let currentBatchCount = syncBatchSize
              let cursor = 0
              while (currentBatchCount == syncBatchSize) {
                begin()
                const batchItems = await context.getAll<RecordType>(
                  sanitizeSQL`SELECT * FROM ${viewName} LIMIT ? OFFSET ?`,
                  [syncBatchSize, cursor]
                )
                currentBatchCount = batchItems.length
                cursor += currentBatchCount
                for (const row of batchItems) {
                  write({
                    type: `insert`,
                    value: row,
                  })
                }
                commit()
              }
              markReady()
              database.logger.info(
                `Sync is ready for ${viewName} into ${trackedTableName}`
              )
            },
          },
        })

        // If the abort controller was aborted while processing the request above
        if (abortController.signal.aborted) {
          await disposeTracking()
        } else {
          abortController.signal.addEventListener(
            `abort`,
            () => {
              disposeTracking()
            },
            { once: true }
          )
        }
      }

      start().catch((error) =>
        database.logger.error(
          `Could not start syncing process for ${viewName} into ${trackedTableName}`,
          error
        )
      )

      return () => {
        database.logger.info(
          `Sync has been stopped for ${viewName} into ${trackedTableName}`
        )
        abortController.abort()
      }
    },
    // Expose the getSyncMetadata function
    getSyncMetadata: undefined,
  }

  const getKey = (record: RecordType) => asPowerSyncRecord(record).id

  const outputConfig: EnhancedPowerSyncCollectionConfig<TTable, TSchema> = {
    ...restConfig,
    schema,
    getKey,
    // Syncing should start immediately since we need to monitor the changes for mutations
    startSync: true,
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
        tableName: viewName,
        trackedTableName,
      }),
    },
  }
  return outputConfig
}
