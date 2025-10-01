import { sanitizeSQL } from "@powersync/common"
import DebugModule from "debug"
import { asPowerSyncRecord } from "./helpers"
import type { AbstractPowerSyncDatabase, LockContext } from "@powersync/common"
import type { Transaction } from "@tanstack/db"
import type {
  PendingOperation,
  PendingOperationStore,
} from "./PendingOperationStore"
import type { PowerSyncRecord } from "./helpers"

const debug = DebugModule.debug(`ts/db:powersync`)

export type TransactorOptions = {
  database: AbstractPowerSyncDatabase
  tableName: string
  pendingOperationStore: PendingOperationStore
  trackedTableName: string
}

/**
 * Handles persisting Tanstack DB transactions to the PowerSync SQLite DB.
 */
export class PowerSyncTransactor<T extends object = Record<string, unknown>> {
  database: AbstractPowerSyncDatabase
  pendingOperationStore: PendingOperationStore
  tableName: string
  trackedTableName: string

  constructor(options: TransactorOptions) {
    this.database = options.database
    this.pendingOperationStore = options.pendingOperationStore
    this.tableName = sanitizeSQL`${options.tableName}`
    this.trackedTableName = sanitizeSQL`${options.trackedTableName}`
  }

  /**
   * Persists a {@link Transaction} to PowerSync's SQLite DB.
   */
  async applyTransaction(transaction: Transaction<T>) {
    const { mutations } = transaction

    // Persist to PowerSync
    const { whenComplete } = await this.database.writeTransaction(
      async (tx) => {
        for (const mutation of mutations) {
          switch (mutation.type) {
            case `insert`:
              await this.handleInsert(asPowerSyncRecord(mutation.modified), tx)
              break
            case `update`:
              await this.handleUpdate(asPowerSyncRecord(mutation.modified), tx)
              break
            case `delete`:
              await this.handleDelete(asPowerSyncRecord(mutation.original), tx)
              break
          }
        }

        /**
         * Fetch the last diff operation in the queue.
         * We need to wait for this operation to be seen by the
         * sync handler before returning from the application call.
         */
        const lastDiffOp = await tx.getOptional<PendingOperation>(`
          SELECT 
            id, operation, timestamp 
          FROM 
            ${this.trackedTableName}
          ORDER BY 
            timestamp DESC
          LIMIT 1
          `)

        /**
         * Return a promise from the writeTransaction, without awaiting it.
         * This promise will resolve once the entire transaction has been
         * observed via the diff triggers.
         * We return without awaiting in order to free the writeLock.
         */
        return {
          whenComplete: lastDiffOp
            ? this.pendingOperationStore.waitFor(lastDiffOp)
            : Promise.resolve(),
        }
      }
    )

    // Wait for the change to be observed via the diff trigger
    await whenComplete
  }

  protected async handleInsert(
    mutation: PowerSyncRecord,
    context: LockContext
  ) {
    debug(`insert`, mutation)
    const keys = Object.keys(mutation).map((key) => sanitizeSQL`${key}`)
    await context.execute(
      `
        INSERT into ${this.tableName} 
            (${keys.join(`, `)}) 
        VALUES 
            (${keys.map((_) => `?`).join(`, `)})
        `,
      Object.values(mutation)
    )
  }

  protected async handleUpdate(
    mutation: PowerSyncRecord,
    context: LockContext
  ) {
    debug(`update`, mutation)

    const keys = Object.keys(mutation).map((key) => sanitizeSQL`${key}`)
    await context.execute(
      `
        UPDATE ${this.tableName} 
        SET ${keys.map((key) => `${key} = ?`).join(`, `)}
        WHERE id = ?
        `,
      [...Object.values(mutation), mutation.id]
    )
  }

  protected async handleDelete(
    mutation: PowerSyncRecord,
    context: LockContext
  ) {
    debug(`delete`, mutation)
    await context.execute(
      `
        DELETE FROM ${this.tableName} WHERE id = ?
        `,
      [mutation.id]
    )
  }
}
