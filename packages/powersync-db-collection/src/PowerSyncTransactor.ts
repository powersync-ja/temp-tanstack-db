import { sanitizeSQL } from "@powersync/common"
import DebugModule from "debug"
import { PendingOperationStore } from "./PendingOperationStore"
import { asPowerSyncRecord, mapOperationToPowerSync } from "./helpers"
import type { AbstractPowerSyncDatabase, LockContext } from "@powersync/common"
import type { PendingMutation, Transaction } from "@tanstack/db"
import type { PendingOperation } from "./PendingOperationStore"
import type { EnhancedPowerSyncCollectionConfig } from "./definitions"

const debug = DebugModule.debug(`ts/db:powersync`)

export type TransactorOptions = {
  database: AbstractPowerSyncDatabase
}

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
 *         await new PowerSyncTransactor({database: db}).applyTransaction(transaction)
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
export class PowerSyncTransactor<T extends object = Record<string, unknown>> {
  database: AbstractPowerSyncDatabase
  pendingOperationStore: PendingOperationStore

  constructor(options: TransactorOptions) {
    this.database = options.database
    this.pendingOperationStore = PendingOperationStore.GLOBAL
  }

  /**
   * Persists a {@link Transaction} to PowerSync's SQLite DB.
   */
  async applyTransaction(transaction: Transaction<T>) {
    const { mutations } = transaction

    if (mutations.length == 0) {
      return
    }
    /**
     * The transaction might contain ops for different collections.
     * We can do some optimizations for single collection transactions.
     */
    const mutationsCollectionIds = mutations.map(
      (mutation) => mutation.collection.id
    )
    const collectionIds = Array.from(new Set(mutationsCollectionIds))
    const lastCollectionMutationIndexes = new Map<string, number>()
    const allCollections = collectionIds
      .map((id) => mutations.find((mutation) => mutation.collection.id == id)!)
      .map((mutation) => mutation.collection)
    for (const collectionId of collectionIds) {
      lastCollectionMutationIndexes.set(
        collectionId,
        mutationsCollectionIds.lastIndexOf(collectionId)
      )
    }

    // Check all the observers are ready before taking a lock
    await Promise.all(
      allCollections.map(async (collection) => {
        if (collection.isReady()) {
          return
        }
        await new Promise<void>((resolve) => collection.onFirstReady(resolve))
      })
    )

    // Persist to PowerSync
    const { whenComplete } = await this.database.writeTransaction(
      async (tx) => {
        const pendingOperations: Array<PendingOperation | null> = []

        for (const [index, mutation] of mutations.entries()) {
          /**
           * Each collection processes events independently. We need to make sure the
           * last operation for each collection has been seen.
           */
          const shouldWait =
            index == lastCollectionMutationIndexes.get(mutation.collection.id)
          switch (mutation.type) {
            case `insert`:
              pendingOperations.push(
                await this.handleInsert(mutation, tx, shouldWait)
              )
              break
            case `update`:
              pendingOperations.push(
                await this.handleUpdate(mutation, tx, shouldWait)
              )
              break
            case `delete`:
              pendingOperations.push(
                await this.handleDelete(mutation, tx, shouldWait)
              )
              break
          }
        }

        /**
         * Return a promise from the writeTransaction, without awaiting it.
         * This promise will resolve once the entire transaction has been
         * observed via the diff triggers.
         * We return without awaiting in order to free the writeLock.
         */
        return {
          whenComplete: Promise.all(
            pendingOperations
              .filter((op) => !!op)
              .map((op) => this.pendingOperationStore.waitFor(op))
          ),
        }
      }
    )

    // Wait for the change to be observed via the diff trigger
    await whenComplete
  }

  protected async handleInsert(
    mutation: PendingMutation<T>,
    context: LockContext,
    waitForCompletion: boolean = false
  ): Promise<PendingOperation | null> {
    debug(`insert`, mutation)

    return this.handleOperationWithCompletion(
      mutation,
      context,
      waitForCompletion,
      async (tableName, mutation) => {
        const keys = Object.keys(mutation.modified).map(
          (key) => sanitizeSQL`${key}`
        )

        await context.execute(
          `
        INSERT into ${tableName} 
            (${keys.join(`, `)}) 
        VALUES 
            (${keys.map((_) => `?`).join(`, `)})
        `,
          Object.values(mutation.modified)
        )
      }
    )
  }

  protected async handleUpdate(
    mutation: PendingMutation<T>,
    context: LockContext,
    waitForCompletion: boolean = false
  ): Promise<PendingOperation | null> {
    debug(`update`, mutation)

    return this.handleOperationWithCompletion(
      mutation,
      context,
      waitForCompletion,
      async (tableName, mutation) => {
        const keys = Object.keys(mutation.modified).map(
          (key) => sanitizeSQL`${key}`
        )
        await context.execute(
          `
        UPDATE ${tableName} 
        SET ${keys.map((key) => `${key} = ?`).join(`, `)}
        WHERE id = ?
        `,
          [
            ...Object.values(mutation.modified),
            asPowerSyncRecord(mutation.modified).id,
          ]
        )
      }
    )
  }

  protected async handleDelete(
    mutation: PendingMutation<T>,
    context: LockContext,
    waitForCompletion: boolean = false
  ): Promise<PendingOperation | null> {
    debug(`update`, mutation)

    return this.handleOperationWithCompletion(
      mutation,
      context,
      waitForCompletion,
      async (tableName, mutation) => {
        await context.execute(
          `
        DELETE FROM ${tableName} WHERE id = ?
        `,
          [asPowerSyncRecord(mutation.original).id]
        )
      }
    )
  }

  /**
   * Helper function which wraps a persistence operation by:
   * - Fetching the mutation's collection's SQLite table details
   * - Executing the mutation
   * - Returning the last pending diff op if required
   */
  protected async handleOperationWithCompletion(
    mutation: PendingMutation<T>,
    context: LockContext,
    waitForCompletion: boolean,
    handler: (tableName: string, mutation: PendingMutation<T>) => Promise<void>
  ): Promise<PendingOperation | null> {
    const { tableName, trackedTableName } = (
      mutation.collection.config as EnhancedPowerSyncCollectionConfig
    ).utils.getMeta()

    if (!tableName) {
      throw new Error(`Could not get tableName from mutation's collection config.
        The provided mutation might not have originated from PowerSync.`)
    }

    await handler(sanitizeSQL`${tableName}`, mutation)

    if (!waitForCompletion) {
      return null
    }

    // Need to get the operation in order to wait for it
    const diffOperation = await context.get<{ id: string; timestamp: string }>(
      sanitizeSQL`SELECT id, timestamp FROM ${trackedTableName} ORDER BY timestamp DESC LIMIT 1`
    )
    return {
      tableName,
      id: diffOperation.id,
      operation: mapOperationToPowerSync(mutation.type),
      timestamp: diffOperation.timestamp,
    }
  }
}
