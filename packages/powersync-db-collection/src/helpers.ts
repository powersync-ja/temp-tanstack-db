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
 * Maps Tanstack DB operations to {@link DiffTriggerOperation}
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
