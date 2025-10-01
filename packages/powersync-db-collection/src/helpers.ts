import { DiffTriggerOperation } from "@powersync/common"

/**
 * All PowerSync table records have a uuid `id` column.
 */
export type PowerSyncRecord = {
  id: string
  [key: string]: unknown
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
