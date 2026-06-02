import { sanitizeSQL } from '@powersync/common'
import DebugModule from 'debug'
import { PendingOperationStore } from './PendingOperationStore'
import { asPowerSyncRecord, mapOperationToPowerSync } from './helpers'
import type { AbstractPowerSyncDatabase, LockContext } from '@powersync/common'
import type { PendingMutation, Transaction } from '@tanstack/db'
import type { PendingOperation } from './PendingOperationStore'
import type {
  EnhancedPowerSyncCollectionConfig,
  PowerSyncCollectionMeta,
} from './definitions'

const debug = DebugModule.debug(`ts/db:powersync`)

/**
 * Controls when a {@link PowerSyncTransactor} considers a mutation transaction
 * complete.
 */
export enum TransactorMode {
  /**
   * Resolve mutation transactions once the local PowerSync SQLite write has
   * been observed by TanStack DB.
   *
   * This is the default mode. It gives fast local-first behavior: mutations are
   * persisted locally, PowerSync can upload them later, and TanStack DB state is
   * already consistent with the local database when the transaction resolves.
   */
  OFFLINE = 'offline',
  /**
   * Resolve mutation transactions only after PowerSync has uploaded the local
   * write to the backend and the resulting change has been synced back down and
   * observed by TanStack DB.
   *
   * Use this mode when callers need backend confirmation before treating a
   * mutation as complete, such as when showing committed server state,
   * navigating away from a critical workflow, or coordinating with systems that
   * only react after the backend has accepted the write.
   *
   * Because this waits for a full upload and sync-down cycle, transactions can
   * take longer to resolve and may remain pending while the client is offline or
   * the PowerSync connection is unable to complete a checkpoint.
   *
   * @experimental This mode depends on PowerSync checkpoint internals and may
   * change as the PowerSync SDK exposes more direct backend-acknowledgement
   * hooks.
   */
  ONLINE = 'online'
}

type BaseTransactorOptions = {
  /**
   * The PowerSync database that mutations will be written to.
   */
  database: AbstractPowerSyncDatabase
}

/**
 * Local-first transactor mode options.
 */
export type OfflineTransactorModeOptions = {
  /**
   * Resolve after the local write has been observed by TanStack DB.
   * This is the default when `mode` is omitted.
   */
  mode?: TransactorMode.OFFLINE
}

/**
 * Backend-confirmed transactor mode options.
 *
 * @experimental Online transaction completion depends on PowerSync checkpoint
 * internals and may change as the PowerSync SDK exposes more direct
 * backend-acknowledgement hooks.
 */
export type OnlineTransactorModeOptions = {
  /**
   * Resolve after the local write has been uploaded to the backend, synced back
   * down, and observed by TanStack DB.
   *
   * @experimental This mode depends on PowerSync checkpoint internals and may
   * change as the PowerSync SDK exposes more direct backend-acknowledgement
   * hooks.
   */
  mode: TransactorMode.ONLINE
  /**
   * Maximum total time to wait for the backend checkpoint and synced-down diff
   * records.
   *
   * If the timeout is reached before the write has completed the upload and
   * sync-down cycle, the mutation transaction rejects.
   *
   * @experimental This option only applies to the experimental online
   * transaction mode.
   */
  timeoutMs?: number
  /**
   * Optional signal for cancelling the online wait.
   *
   * @experimental This option only applies to the experimental online
   * transaction mode.
   */
  abortSignal?: AbortSignal
}

/**
 * Shared transactor mode options.
 *
 * This lower-level discriminated union is used both by
 * {@link PowerSyncTransactor} and by collection options that configure the
 * default transactor.
 */
export type TransactorModeOptions =
  | OfflineTransactorModeOptions
  | OnlineTransactorModeOptions

/**
 * Options for local-first transaction handling.
 */
export type OfflineTransactorOptions = BaseTransactorOptions &
  OfflineTransactorModeOptions

/**
 * Options for backend-confirmed transaction handling.
 *
 * @experimental Online transaction completion depends on PowerSync checkpoint
 * internals and may change as the PowerSync SDK exposes more direct
 * backend-acknowledgement hooks.
 */
export type OnlineTransactorOptions = BaseTransactorOptions &
  OnlineTransactorModeOptions

/**
 * Configuration for {@link PowerSyncTransactor}.
 *
 * `mode` is the discriminator:
 * - omit `mode` or use `TransactorMode.OFFLINE` for fast local-first writes
 * - use `TransactorMode.ONLINE` to unlock backend-confirmed wait options
 */
export type TransactorOptions = OfflineTransactorOptions | OnlineTransactorOptions

/**
 * Applies mutations to the PowerSync database. This method is called automatically by the collection's
 * insert, update, and delete operations. You typically don't need to call this directly unless you
 * have special transaction requirements.
 *
 * By default, transactions resolve in {@link TransactorMode.OFFLINE} mode after
 * the local SQLite write has been observed by TanStack DB. For workflows that
 * need server acknowledgement, the experimental
 * {@link TransactorMode.ONLINE} mode waits for PowerSync to upload the mutation
 * to the backend and sync the accepted change back down before resolving.
 *
 * @example
 * Local-first transaction handling.
 *
 * ```typescript
 * // Create a collection
 * const collection = createCollection(
 *   powerSyncCollectionOptions<Document>({
 *     database: db,
 *     table: APP_SCHEMA.props.documents,
 *   })
 * )
 *
 * const addTx = createTransaction({
 *   autoCommit: false,
 *   mutationFn: async ({ transaction }) => {
 *     await new PowerSyncTransactor({ database: db }).applyTransaction(transaction)
 *   },
 * })
 *
 * addTx.mutate(() => {
 *   for (let i = 0; i < 5; i++) {
 *     collection.insert({ id: randomUUID(), name: `tx-${i}` })
 *   }
 * })
 *
 * await addTx.commit()
 * await addTx.isPersisted.promise
 * ```
 *
 * @example
 * Experimental: wait for backend acknowledgement before resolving the
 * transaction.
 *
 * ```typescript
 * const onlineTransactor = new PowerSyncTransactor({
 *   database: db,
 *   mode: TransactorMode.ONLINE,
 *   timeoutMs: 30_000,
 * })
 *
 * const confirmedTx = createTransaction({
 *   autoCommit: false,
 *   mutationFn: async ({ transaction }) => {
 *     await onlineTransactor.applyTransaction(transaction)
 *   },
 * })
 *
 * confirmedTx.mutate(() => {
 *   collection.insert({ id: randomUUID(), name: `confirmed-write` })
 * })
 *
 * await confirmedTx.commit()
 * await confirmedTx.isPersisted.promise
 * // At this point the mutation has been uploaded and synced back down.
 * ```
 *
 * @param transaction - The transaction containing mutations to apply
 * @returns A promise that resolves according to the configured {@link TransactorMode}.
 */
export class PowerSyncTransactor {
  database: AbstractPowerSyncDatabase
  pendingOperationStore: PendingOperationStore
  readonly mode: TransactorMode
  protected readonly onlineOptions: Pick<
    OnlineTransactorOptions,
    'abortSignal' | 'timeoutMs'
  > | null

  constructor(options: TransactorOptions) {
    this.database = options.database
    this.pendingOperationStore = PendingOperationStore.GLOBAL
    this.mode = options.mode ?? TransactorMode.OFFLINE
    this.onlineOptions =
      options.mode === TransactorMode.ONLINE
        ? {
            abortSignal: options.abortSignal,
            timeoutMs: options.timeoutMs,
          }
        : null
  }

  /**
   * Persists a {@link Transaction} to the PowerSync SQLite database.
   */
  async applyTransaction(transaction: Transaction<any>) {
    const { mutations } = transaction

    if (mutations.length == 0) {
      return
    }
    /**
     * The transaction might contain operations for different collections.
     * We can do some optimizations for single-collection transactions.
     */
    const mutationsCollectionIds = mutations.map(
      (mutation) => mutation.collection.id,
    )
    const collectionIds = Array.from(new Set(mutationsCollectionIds))
    const lastCollectionMutationIndexes = new Map<string, number>()
    const allCollections = collectionIds
      .map((id) => mutations.find((mutation) => mutation.collection.id == id)!)
      .map((mutation) => mutation.collection)
    for (const collectionId of collectionIds) {
      lastCollectionMutationIndexes.set(
        collectionId,
        mutationsCollectionIds.lastIndexOf(collectionId),
      )
    }

    // Check all the observers are ready before taking a lock
    await Promise.all(
      allCollections.map(async (collection) => {
        if (collection.isReady()) {
          return
        }
        await new Promise<void>((resolve) => collection.onFirstReady(resolve))
      }),
    )

    // Persist to PowerSync
    const { whenComplete } = await this.database.writeTransaction(
      async (tx) => {
        const pendingOperations: Array<PendingOperation | null> = []

        for (const [index, mutation] of mutations.entries()) {
          /**
           * Each collection processes events independently. We need to make sure the
           * last operation for each collection has been observed.
           */
          const shouldWait =
            index == lastCollectionMutationIndexes.get(mutation.collection.id)
          switch (mutation.type) {
            case `insert`:
              pendingOperations.push(
                await this.handleInsert(mutation, tx, shouldWait),
              )
              break
            case `update`:
              pendingOperations.push(
                await this.handleUpdate(mutation, tx, shouldWait),
              )
              break
            case `delete`:
              pendingOperations.push(
                await this.handleDelete(mutation, tx, shouldWait),
              )
              break
          }
        }

        if (this.mode == TransactorMode.OFFLINE) {
          /**
           * Return a promise from the writeTransaction, without awaiting it.
           * This promise will resolve once the entire transaction has been
           * observed via the diff triggers.
           * We return without awaiting in order to free the write lock.
           */
          return {
            whenComplete: Promise.all(
              pendingOperations
                .filter((op) => !!op)
                .map((op) => this.pendingOperationStore.waitFor(op)),
            ),
          }
        } else {
          // TODO, this should wait for all unique collections to have been flushed
          // after the write checkpoint has been synced.
          const meta = this.getMutationCollectionMeta(mutations[0]!);
          /**
           * Resolve after the backend has accepted the write checkpoint and
           * TanStack DB has processed the resulting synced-down diff records.
           */
          return {
            whenComplete: this.waitForOnlineCompletion(meta),
          }
        }

  
      },
    )

    // Wait for the change to be observed via the diff trigger
    await whenComplete
  }

  protected async handleInsert(
    mutation: PendingMutation<any>,
    context: LockContext,
    waitForCompletion: boolean = false,
  ): Promise<PendingOperation | null> {
    debug(`insert`, mutation)

    return this.handleOperationWithCompletion(
      mutation,
      context,
      waitForCompletion,
      async (tableName, mutation, serializeValue) => {
        const values = serializeValue(mutation.modified)
        const keys = Object.keys(values).map((key) => sanitizeSQL`${key}`)
        const queryParameters = Object.values(values)

        const metadataValue = this.processMutationMetadata(mutation)
        if (metadataValue != null) {
          keys.push(`_metadata`)
          queryParameters.push(metadataValue)
        }

        await context.execute(
          `
        INSERT into ${tableName} 
            (${keys.join(`, `)}) 
        VALUES 
            (${keys.map((_) => `?`).join(`, `)})
        `,
          queryParameters,
        )
      },
    )
  }

  protected async handleUpdate(
    mutation: PendingMutation<any>,
    context: LockContext,
    waitForCompletion: boolean = false,
  ): Promise<PendingOperation | null> {
    debug(`update`, mutation)

    return this.handleOperationWithCompletion(
      mutation,
      context,
      waitForCompletion,
      async (tableName, mutation, serializeValue) => {
        const values = serializeValue(mutation.modified)
        const keys = Object.keys(values).map((key) => sanitizeSQL`${key}`)
        const queryParameters = Object.values(values)

        const metadataValue = this.processMutationMetadata(mutation)
        if (metadataValue != null) {
          keys.push(`_metadata`)
          queryParameters.push(metadataValue)
        }

        await context.execute(
          `
        UPDATE ${tableName} 
        SET ${keys.map((key) => `${key} = ?`).join(`, `)}
        WHERE id = ?
        `,
          [...queryParameters, asPowerSyncRecord(mutation.modified).id],
        )
      },
    )
  }

  protected async handleDelete(
    mutation: PendingMutation<any>,
    context: LockContext,
    waitForCompletion: boolean = false,
  ): Promise<PendingOperation | null> {
    debug(`update`, mutation)

    return this.handleOperationWithCompletion(
      mutation,
      context,
      waitForCompletion,
      async (tableName, mutation) => {
        const metadataValue = this.processMutationMetadata(mutation)
        if (metadataValue != null) {
          /**
           * Delete operations with metadata require a different approach to handle metadata.
           * This will delete the record.
           */
          await context.execute(
            `
            UPDATE ${tableName} SET _deleted = TRUE, _metadata = ? WHERE id = ?
            `,
            [metadataValue, asPowerSyncRecord(mutation.original).id],
          )
        } else {
          await context.execute(
            `
            DELETE FROM ${tableName} WHERE id = ?
            `,
            [asPowerSyncRecord(mutation.original).id],
          )
        }
      },
    )
  }

  /**
   * Helper function which wraps a persistence operation by:
   * - Fetching the mutation's collection's SQLite table details
   * - Executing the mutation
   * - Returning the last pending diff operation if required
   */
  protected async handleOperationWithCompletion(
    mutation: PendingMutation<any>,
    context: LockContext,
    waitForCompletion: boolean,
    handler: (
      tableName: string,
      mutation: PendingMutation<any>,
      serializeValue: (value: any) => Record<string, unknown>,
    ) => Promise<void>,
  ): Promise<PendingOperation | null> {
    const { tableName, trackedTableName, serializeValue } =
      this.getMutationCollectionMeta(mutation)

    await handler(sanitizeSQL`${tableName}`, mutation, serializeValue)

    if (!waitForCompletion) {
      return null
    }

    // Need to get the operation in order to wait for it
    const diffOperation = await context.get<{ id: string; timestamp: string }>(
      sanitizeSQL`SELECT id, timestamp FROM ${trackedTableName} ORDER BY timestamp DESC LIMIT 1`,
    )
    return {
      tableName,
      id: diffOperation.id,
      operation: mapOperationToPowerSync(mutation.type),
      timestamp: diffOperation.timestamp,
    }
  }

  protected getMutationCollectionMeta(
    mutation: PendingMutation<any>,
  ): PowerSyncCollectionMeta<any> {
    if (
      typeof (mutation.collection.config as any).utils?.getMeta != `function`
    ) {
      throw new Error(`Collection is not a PowerSync collection.`)
    }
    return (
      mutation.collection
        .config as unknown as EnhancedPowerSyncCollectionConfig<any>
    ).utils.getMeta()
  }

  /**
   * Waits for PowerSync to upload the local write, receive the accepted change
   * back from the backend, and drain the resulting TanStack DB diff records.
   */
  protected async waitForOnlineCompletion(
    meta: PowerSyncCollectionMeta<any>,
  ): Promise<void> {
    const { abortSignal, timeoutMs } = this.onlineOptions ?? {}

    if (timeoutMs == null) {
      const options = { abortSignal }
      await meta.internal.checkpointObserver.waitForCheckpoint(options)
      await meta.internal.diffObserver.waitForEmpty(options)
      return
    }

    const deadlineController = new AbortController()
    const timeout = setTimeout(() => {
      deadlineController.abort()
    }, timeoutMs)

    const onAbort = () => {
      deadlineController.abort()
    }

    abortSignal?.addEventListener('abort', onAbort)

    try {
      if (abortSignal?.aborted) {
        deadlineController.abort()
      }

      const options = { abortSignal: deadlineController.signal }
      await meta.internal.checkpointObserver.waitForCheckpoint(options)
      await meta.internal.diffObserver.waitForEmpty(options)
    } finally {
      clearTimeout(timeout)
      abortSignal?.removeEventListener('abort', onAbort)
    }
  }

  /**
   * Processes collection mutation metadata for persistence to the database.
   * We only support storing string metadata.
   * @returns null if no metadata should be stored.
   */
  protected processMutationMetadata(
    mutation: PendingMutation<any>,
  ): string | null {
    const { metadataIsTracked } = this.getMutationCollectionMeta(mutation)
    if (!metadataIsTracked) {
      // If it's not supported, we don't store metadata.
      if (typeof mutation.metadata != `undefined`) {
        // Log a warning if metadata is provided but not tracked.
        this.database.logger.warn(
          `Metadata provided for collection ${mutation.collection.id} but the PowerSync table does not track metadata. The PowerSync table should be configured with trackMetadata: true.`,
          mutation.metadata,
        )
      }
      return null
    } else if (typeof mutation.metadata == `undefined`) {
      return null
    } else if (typeof mutation.metadata == `string`) {
      return mutation.metadata
    } else {
      return JSON.stringify(mutation.metadata)
    }
  }
}
