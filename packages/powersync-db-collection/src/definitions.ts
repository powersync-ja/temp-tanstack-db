import type { AbstractPowerSyncDatabase, Table } from "@powersync/common"
import type { StandardSchemaV1 } from "@standard-schema/spec"
import type { BaseCollectionConfig, CollectionConfig } from "@tanstack/db"
import type { ExtractedTable } from "./helpers"

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
  TTable extends Table = Table,
  TSchema extends StandardSchemaV1 = never,
> = Omit<
  BaseCollectionConfig<ExtractedTable<TTable>, string, TSchema>,
  `onInsert` | `onUpdate` | `onDelete` | `getKey`
> & {
  /** The PowerSync Schema Table definition */
  table: TTable
  /** The PowerSync database instance */
  database: AbstractPowerSyncDatabase
  /**
   * The maximum number of documents to read from the SQLite table
   * in a single batch during the initial sync between PowerSync and the
   * in-memory TanStack DB collection.
   *
   * @remarks
   * - Defaults to {@link DEFAULT_BATCH_SIZE} if not specified.
   * - Larger values reduce the number of round trips to the storage
   *   engine but increase memory usage per batch.
   * - Smaller values may lower memory usage and allow earlier
   *   streaming of initial results, at the cost of more query calls.
   */
  syncBatchSize?: number
}

export type PowerSyncCollectionMeta = {
  /**
   * The SQLite table representing the collection.
   */
  tableName: string
  /**
   * The internal table used to track diff for the collection.
   */
  trackedTableName: string
}

export type EnhancedPowerSyncCollectionConfig<
  TTable extends Table = Table,
  TSchema extends StandardSchemaV1 = never,
> = CollectionConfig<ExtractedTable<TTable>, string, TSchema> & {
  id?: string
  utils: PowerSyncCollectionUtils
  schema?: TSchema
}

export type PowerSyncCollectionUtils = {
  getMeta: () => PowerSyncCollectionMeta
}

/**
 * Default value for {@link PowerSyncCollectionConfig#syncBatchSize}
 */
export const DEFAULT_BATCH_SIZE = 1000
