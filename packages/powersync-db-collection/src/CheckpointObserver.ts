import type { AbstractPowerSyncDatabase } from '@powersync/common';

const CHECKPOINT_WATCH_TABLE = 'ps_local_checkpoint_watch';

type CheckpointValue = bigint | null;
type SerializedCheckpointValue = string | null;

type CheckpointRecord = {
  id: number;
  recorded_at: string;
  event: string;
  old_target_op: CheckpointValue;
  new_target_op: CheckpointValue;
  old_last_op: CheckpointValue;
  new_last_op: CheckpointValue;
  old_last_applied_op: CheckpointValue;
  new_last_applied_op: CheckpointValue;
};

type SerializedCheckpointRecord = Omit<
  CheckpointRecord,
  | 'old_target_op'
  | 'new_target_op'
  | 'old_last_op'
  | 'new_last_op'
  | 'old_last_applied_op'
  | 'new_last_applied_op'
> & {
  old_target_op: SerializedCheckpointValue;
  new_target_op: SerializedCheckpointValue;
  old_last_op: SerializedCheckpointValue;
  new_last_op: SerializedCheckpointValue;
  old_last_applied_op: SerializedCheckpointValue;
  new_last_applied_op: SerializedCheckpointValue;
};

type PendingCheckpointTracker = {
  target_op: CheckpointValue;
  resolver: () => void;
  rejector: (error: Error) => void;
};

const MAX_OP_ID = BigInt('9223372036854775807');

const isConcreteCheckpoint = (value: CheckpointValue): value is bigint => {
  return value != null && value != MAX_OP_ID;
};

const checkpointApplied = (target: CheckpointValue, lastApplied: CheckpointValue): boolean => {
  return target != null && lastApplied != null && target <= lastApplied;
};

const parseCheckpointValue = (value: SerializedCheckpointValue): CheckpointValue => {
  return value == null ? null : BigInt(value);
};

const deserializeCheckpointRecord = (
  record: SerializedCheckpointRecord
): CheckpointRecord => ({
  ...record,
  old_target_op: parseCheckpointValue(record.old_target_op),
  new_target_op: parseCheckpointValue(record.new_target_op),
  old_last_op: parseCheckpointValue(record.old_last_op),
  new_last_op: parseCheckpointValue(record.new_last_op),
  old_last_applied_op: parseCheckpointValue(record.old_last_applied_op),
  new_last_applied_op: parseCheckpointValue(record.new_last_applied_op),
});

/**
 * Observes and monitors the PowerSync write checkpoint state.
 * Note: this uses internals which might be subject to change.
 * Note: This is a sneaky and lazy method for obtaining write checkpoint updates
 * without actually implementing the necessary logic/hooks in the PowerSync SDK.
 */
export class CheckpointObserver {
  readonly db: AbstractPowerSyncDatabase;

  protected disposeWatch: (() => void) | null;

  protected disposeDatabaseListener: (() => void) | null;

  protected pendingCheckpoints: Array<PendingCheckpointTracker>;

  protected disposed: boolean;

  constructor(params: { db: AbstractPowerSyncDatabase }) {
    this.db = params.db;
    this.disposeWatch = null;
    this.disposeDatabaseListener = this.db.registerListener({
      closing: () => this.dispose(),
      closed: () => this.dispose()
    });
    this.pendingCheckpoints = [];
    this.disposed = false;
  }

  /**
   * Wait for a write checkpoint to be synced and applied locally.
   * Uses the next set target write checkpoint if no `target` is provided.
   */
  async waitForCheckpoint(
    options: {
      target?: bigint;
      timeoutMs?: number;
      abortSignal?: AbortSignal;
    } = {}
  ): Promise<void> {
    const { target = null, timeoutMs, abortSignal } = options;
    // TODO, handle if target is provided and it's lower than the last applied_op

    let tracker!: PendingCheckpointTracker;

    const promise = new Promise<void>((resolve, reject) => {
      let complete = false;

      let timeout = timeoutMs
        ? setTimeout(() => {
            tracker.rejector(new Error(`Timeout reached`));
          }, timeoutMs)
        : null;

      const onAbort = () => {
        tracker.rejector(new Error('Aborted'));
      };

      if (abortSignal) {
        abortSignal.addEventListener('abort', onAbort);
      }

      const cleanup = () => {
        const index = this.pendingCheckpoints.indexOf(tracker);
        if (index != -1) {
          this.pendingCheckpoints.splice(index, 1);
        }
        if (timeout) {
          clearTimeout(timeout);
          timeout = null;
        }
        if (abortSignal) {
          abortSignal.removeEventListener('abort', onAbort);
        }
      };

      tracker = {
        target_op: target,
        resolver: () => {
          if (complete) {
            return;
          }
          complete = true;
          cleanup();
          resolve();
        },
        rejector: (error: Error) => {
          if (complete) {
            return;
          }
          complete = true;
          cleanup();
          reject(error);
        }
      };
    });

    this.pendingCheckpoints.push(tracker);

    if (this.disposed) {
      tracker.rejector(new Error('Checkpoint observer disposed'));
    } else if (abortSignal?.aborted) {
      tracker.rejector(new Error('Aborted'));
    } else {
      try {
        await this.processCurrentState();
      } catch (error) {
        tracker.rejector(error instanceof Error ? error : new Error('Failed to read checkpoint state'));
      }
    }

    return promise;
  }

  /**
   * Initializes watches over the internal PowerSync checkpoint state.
   * We don't have super great observability for changes made here, so,
   * we work around that by using SQLite triggers to capture value changes to
   * the `$local` bucket in `ps_buckets`.
   *
   * TODO: This trigger-based implementation might be overkill. It treats the SDK
   * as a black box and records every relevant internal state change so we don't
   * miss anything. We could probably simplify this if the SDK exposed a direct
   * checkpoint hook or helper.
   */
  async init() {
    if (this.isDisposedOrClosed()) {
      return;
    }

    this.disposed = false;

    await this.db.writeLock(async (ctx) => {
      if (this.disposed) {
        return;
      }

      /**
       * A table to buffer updates made to the local write checkpoint
       * state. The old values are not required for the completion logic, but
       * keeping them makes the buffered checkpoint transitions easier to inspect.
       */
      await ctx.execute(/* sql */ `
        CREATE TEMP TABLE ${CHECKPOINT_WATCH_TABLE} (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          recorded_at TEXT NOT NULL DEFAULT (strftime ('%Y-%m-%dT%H:%M:%fZ', 'now')),
          event TEXT NOT NULL,
          old_target_op INTEGER,
          new_target_op INTEGER,
          old_last_op INTEGER,
          new_last_op INTEGER,
          old_last_applied_op INTEGER,
          new_last_applied_op INTEGER
        );
      `);

      await ctx.execute(/* sql */ `
        CREATE TEMP TRIGGER ps_watch_local_bucket_insert AFTER INSERT ON main.ps_buckets WHEN NEW.name = '$local' BEGIN
        INSERT INTO
          ${CHECKPOINT_WATCH_TABLE} (
            event,
            old_target_op,
            new_target_op,
            old_last_op,
            new_last_op,
            old_last_applied_op,
            new_last_applied_op
          )
        VALUES
          (
            'insert',
            NULL,
            NEW.target_op,
            NULL,
            NEW.last_op,
            NULL,
            NEW.last_applied_op
          );

        END;
      `);

      await ctx.execute(/* sql */ `
        CREATE TEMP TRIGGER ps_watch_local_bucket_update AFTER
        UPDATE ON main.ps_buckets WHEN NEW.name = '$local'
        AND (
          OLD.target_op IS NOT NEW.target_op
          OR OLD.last_op IS NOT NEW.last_op
          OR OLD.last_applied_op IS NOT NEW.last_applied_op
        ) BEGIN
        INSERT INTO
          ps_local_checkpoint_watch (
            event,
            old_target_op,
            new_target_op,
            old_last_op,
            new_last_op,
            old_last_applied_op,
            new_last_applied_op
          )
        VALUES
          (
            'update',
            OLD.target_op,
            NEW.target_op,
            OLD.last_op,
            NEW.last_op,
            OLD.last_applied_op,
            NEW.last_applied_op
          );

        END;
      `);

      await ctx.execute(/* sql */ `
        CREATE TEMP TRIGGER ps_watch_local_bucket_delete AFTER DELETE ON main.ps_buckets WHEN OLD.name = '$local' BEGIN
        INSERT INTO
          ${CHECKPOINT_WATCH_TABLE} (
            event,
            old_target_op,
            new_target_op,
            old_last_op,
            new_last_op,
            old_last_applied_op,
            new_last_applied_op
          )
        VALUES
          (
            'delete',
            OLD.target_op,
            NULL,
            OLD.last_op,
            NULL,
            OLD.last_applied_op,
            NULL
          );

        END;
      `);
    });

    if (this.isDisposedOrClosed()) {
      return;
    }

    this.disposeWatch = this.db.onChangeWithCallback(
      {
        onChange: async () => {
          if (this.disposed) {
            return;
          }

          // Needs to be a write lock in order to use temp tables (scoped to connection)
          await this.db.writeLock(async (ctx) => {
            if (this.disposed) {
              return;
            }

            // get the current tracked items
            // node:sqlite does not reliably preserve large SQLite integers, so
            // read checkpoint values as text and convert them to BigInt in JS.
            const records = await ctx.getAll<SerializedCheckpointRecord>(/* sql */ `
              SELECT
                id,
                recorded_at,
                event,
                CAST(old_target_op AS TEXT) AS old_target_op,
                CAST(new_target_op AS TEXT) AS new_target_op,
                CAST(old_last_op AS TEXT) AS old_last_op,
                CAST(new_last_op AS TEXT) AS new_last_op,
                CAST(old_last_applied_op AS TEXT) AS old_last_applied_op,
                CAST(new_last_applied_op AS TEXT) AS new_last_applied_op
              FROM
                ${CHECKPOINT_WATCH_TABLE}
              ORDER BY
                id ASC
            `);

            this.processRecords(records.map(deserializeCheckpointRecord));

            // delete the records, we've seen it all
            await ctx.execute(`DELETE FROM ${CHECKPOINT_WATCH_TABLE}`);
          });
        }
      },
      { tables: [CHECKPOINT_WATCH_TABLE] }
    );
  }

  async dispose() {
    if (this.disposed) {
      return;
    }

    this.disposed = true;
    this.disposeDatabaseListener?.();
    this.disposeDatabaseListener = null;
    this.disposeWatch?.();
    this.disposeWatch = null;

    if (!this.db.closed) {
      await this.db.writeLock(async (ctx) => {
        await ctx.execute(/* sql */ `DROP TRIGGER IF EXISTS ps_watch_local_bucket_insert`);
        await ctx.execute(/* sql */ `DROP TRIGGER IF EXISTS ps_watch_local_bucket_update`);
        await ctx.execute(/* sql */ `DROP TRIGGER IF EXISTS ps_watch_local_bucket_delete`);
        await ctx.execute(/* sql */ `DROP TABLE IF EXISTS ${CHECKPOINT_WATCH_TABLE}`);
      });
    }

    // It's too late for any pending ops
    for (const op of [...this.pendingCheckpoints]) {
      op.rejector(new Error('Disposing watcher'));
    }
    this.pendingCheckpoints = [];
  }

  protected async processCurrentState() {
    await this.db.writeLock(async (ctx) => {
      const row = await ctx.getOptional<{
        target_op: SerializedCheckpointValue;
        last_applied_op: SerializedCheckpointValue;
      }>(/* sql */ `
        SELECT
          -- node:sqlite does not reliably preserve large SQLite integers, so
          -- read checkpoint values as text and convert them to BigInt in JS.
          CAST(target_op AS TEXT) AS target_op,
          CAST(last_applied_op AS TEXT) AS last_applied_op
        FROM
          main.ps_buckets
        WHERE
          name = '$local'
      `);

      if (!row) {
        return;
      }

      const targetOp = parseCheckpointValue(row.target_op);
      const lastAppliedOp = parseCheckpointValue(row.last_applied_op);

      if (isConcreteCheckpoint(targetOp) && !checkpointApplied(targetOp, lastAppliedOp)) {
        for (const pendingOp of this.pendingCheckpoints) {
          if (pendingOp.target_op == null) {
            pendingOp.target_op = targetOp;
          }
        }
      }

      for (const pendingOp of [...this.pendingCheckpoints]) {
        if (checkpointApplied(pendingOp.target_op, lastAppliedOp)) {
          pendingOp.resolver();
        }
      }
    });
  }

  protected isDisposedOrClosed(): boolean {
    return this.disposed || this.db.closed;
  }

  protected processRecords(records: Array<CheckpointRecord>) {
    for (const op of records) {
      // If we have a concrete target op (write checkpoint) and pending requests which don't have a target yet
      if (op.old_target_op != op.new_target_op && isConcreteCheckpoint(op.new_target_op)) {
        // The target op just changed from a MAX to a concrete op, this value should be targeted
        for (const pendingOp of this.pendingCheckpoints) {
          if (pendingOp.target_op == null) {
            // These wanted to wait for the next write checkpoint, we know this now.
            pendingOp.target_op = op.new_target_op;
          }
        }
      }

      for (const pendingOp of [...this.pendingCheckpoints]) {
        if (checkpointApplied(pendingOp.target_op, op.new_last_applied_op)) {
          pendingOp.resolver();
        }
      }
    }
  }
}
