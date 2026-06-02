import { BaseObserver } from '@powersync/common';
import type { AbstractPowerSyncDatabase } from '@powersync/common';

// TODO cleanup exports
export interface DiffObserver {
    waitForEmpty: (options?: {timeoutMs?: number, abortSignal?: AbortSignal}) => Promise<void>
}

type DiffObserverImplListener = {
    empty: () => void;
    closed: () => void;
}

export class DiffObserverImpl extends BaseObserver<DiffObserverImplListener> implements DiffObserver {
    readonly db: AbstractPowerSyncDatabase;
    readonly diffTableName: string;

    constructor(options: {db: AbstractPowerSyncDatabase, diffTableName: string}) {
        super();
        this.db = options.db;
        this.diffTableName = options.diffTableName;
    }    

    dispose(): void {
        this.iterateListeners(l => l.closed?.());
    }

    markEmpty() {
        this.iterateListeners(l => l.empty?.());
    }

     async waitForEmpty(options: {timeoutMs?: number, abortSignal?: AbortSignal} = {}): Promise<void> {
        // get a write lock to check the table state
        let finalPromise: Promise<void>;
        await this.db.writeLock(async (ctx) => {
            const {count} = await ctx.get<{count: number}>(`SELECT COUNT(*) as count FROM ${this.diffTableName}`)
            if (count == 0) {
                // It's already empty
                return;
            }

            // configure listeners inside a write lock
            finalPromise = new Promise<void>((resolve, reject) => {
                const {abortSignal, timeoutMs} = options;
                let completed = false;

                const timeout = timeoutMs ? setTimeout(() => {
                    cleanup();
                    reject(new Error('Timeout while waiting for diff table to clear'));
                }, timeoutMs) : null;

                const onAbort = () => {
                    cleanup();
                    reject(new Error('Aborted while waiting for diff table to clear'));
                }

                abortSignal?.addEventListener('abort', onAbort);

                const cleanup = () => {
                    completed = true;
                    if (timeout) {
                        clearTimeout(timeout);
                    }
                    abortSignal?.removeEventListener('abort', onAbort);
                    dispose();
                }
                // register a listener for when it's empty
                const dispose = this.registerListener({
                    closed: () => {
                        cleanup();
                        reject(new Error('Diff Observer closed'));
                    },
                    empty: () => {
                        if (completed) {
                            return;
                        }
                        cleanup();
                        resolve()
                    }
                });

                if (abortSignal?.aborted) {
                    onAbort();
                }
            })
        })

        // await the listeners outside of the previous write lock
        await finalPromise!;
     }
}
