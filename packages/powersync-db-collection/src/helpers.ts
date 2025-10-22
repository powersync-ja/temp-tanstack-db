import { DiffTriggerOperation } from "@powersync/common"
import type { ExtractColumnValueType, Table } from "@powersync/common"

/**
 * All PowerSync table records have a uuid `id` column.
 */
export type PowerSyncRecord = {
  id: string
  [key: string]: unknown
}

/**
 * Utility type: If T includes null, add undefined.
 * PowerSync records typically are typed as `string | null`, where insert
 * and update operations also allow not specifying a value at all (optional)
 *  */
type WithUndefinedIfNull<T> = null extends T ? T | undefined : T
type OptionalIfUndefined<T> = {
  [K in keyof T as undefined extends T[K] ? K : never]?: T[K]
} & {
  [K in keyof T as undefined extends T[K] ? never : K]: T[K]
}

/**
 * Utility type that extracts the typed structure of a table based on its column definitions.
 * Maps each column to its corresponding TypeScript type using ExtractColumnValueType.
 *
 * @template TTable - The PowerSync table definition
 * @example
 * ```typescript
 * const table = new Table({
 *   name: column.text,
 *   age: column.integer
 * })
 * type TableType = ExtractedTable<typeof table>
 * // Results in: { name: string | null, age: number | null }
 * ```
 */
export type ExtractedTable<TTable extends Table> = OptionalIfUndefined<{
  [K in keyof TTable[`columnMap`]]: WithUndefinedIfNull<
    ExtractColumnValueType<TTable[`columnMap`][K]>
  >
}> & {
  id: string
}

export function asPowerSyncRecord(record: any): PowerSyncRecord {
  if (typeof record.id !== `string`) {
    throw new Error(`Record must have a string id field`)
  }
  return record as PowerSyncRecord
}

/**
 * Maps {@link DiffTriggerOperation} to TanstackDB operations
 */
export function mapOperation(operation: DiffTriggerOperation) {
  switch (operation) {
    case DiffTriggerOperation.INSERT:
      return `insert`
    case DiffTriggerOperation.UPDATE:
      return `update`
    case DiffTriggerOperation.DELETE:
      return `delete`
  }
}

/**
 * Maps TanstackDB operations to  {@link DiffTriggerOperation}
 */
export function mapOperationToPowerSync(operation: string) {
  switch (operation) {
    case `insert`:
      return DiffTriggerOperation.INSERT
    case `update`:
      return DiffTriggerOperation.UPDATE
    case `delete`:
      return DiffTriggerOperation.DELETE
    default:
      throw new Error(`Unknown operation ${operation} received`)
  }
}
