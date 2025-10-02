import { randomUUID } from "node:crypto"
import { tmpdir } from "node:os"
import {
  CrudEntry,
  PowerSyncDatabase,
  Schema,
  Table,
  column,
} from "@powersync/node"
import { createCollection, createTransaction } from "@tanstack/db"
import { describe, expect, it, onTestFinished, vi } from "vitest"
import { powerSyncCollectionOptions } from "../src"
import { PowerSyncTransactor } from "../src/PowerSyncTransactor"
import type { AbstractPowerSyncDatabase } from "@powersync/node"

const APP_SCHEMA = new Schema({
  users: new Table({
    name: column.text,
  }),
  documents: new Table({
    name: column.text,
  }),
})

type Document = (typeof APP_SCHEMA)[`types`][`documents`]
type User = (typeof APP_SCHEMA)[`types`][`users`]

describe(`PowerSync Integration`, () => {
  async function createDatabase() {
    const db = new PowerSyncDatabase({
      database: {
        dbFilename: `test.sqlite`,
        dbLocation: tmpdir(),
      },
      schema: APP_SCHEMA,
    })
    onTestFinished(async () => {
      await db.disconnectAndClear()
      await db.close()
    })
    return db
  }

  async function createTestData(db: AbstractPowerSyncDatabase) {
    await db.execute(`
        INSERT into documents (id, name)
        VALUES 
            (uuid(), 'one'),
            (uuid(), 'two'),
            (uuid(), 'three')
        `)
  }

  describe(`sync`, () => {
    it(`should initialize and fetch initial data`, async () => {
      const db = await createDatabase()
      await createTestData(db)

      const collection = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => collection.cleanup())

      await collection.stateWhenReady()

      // Verify the collection state contains our items
      expect(collection.size).toBe(3)
      expect(collection.toArray.map((entry) => entry.name)).deep.equals([
        `one`,
        `two`,
        `three`,
      ])
    })

    it(`should update when data syncs`, async () => {
      const db = await createDatabase()
      await createTestData(db)

      const collection = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => collection.cleanup())

      await collection.stateWhenReady()

      // Verify the collection state contains our items
      expect(collection.size).toBe(3)

      // Make an update, simulates a sync from another client
      await db.execute(`
        INSERT into documents (id, name)
        VALUES 
            (uuid(), 'four')
        `)

      // The collection should update
      await vi.waitFor(
        () => {
          expect(collection.size).toBe(4)
          expect(collection.toArray.map((entry) => entry.name)).deep.equals([
            `one`,
            `two`,
            `three`,
            `four`,
          ])
        },
        { timeout: 1000 }
      )

      await db.execute(`
        DELETE from documents
        WHERE name = 'two'
        `)

      // The collection should update
      await vi.waitFor(
        () => {
          expect(collection.size).toBe(3)
          expect(collection.toArray.map((entry) => entry.name)).deep.equals([
            `one`,
            `three`,
            `four`,
          ])
        },
        { timeout: 1000 }
      )

      await db.execute(`
        UPDATE documents
        SET name = 'updated'
        WHERE name = 'one'
        `)

      // The collection should update
      await vi.waitFor(
        () => {
          expect(collection.size).toBe(3)
          expect(collection.toArray.map((entry) => entry.name)).deep.equals([
            `updated`,
            `three`,
            `four`,
          ])
        },
        { timeout: 1000 }
      )
    })

    it(`should propagate collection mutations to PowerSync`, async () => {
      const db = await createDatabase()
      await createTestData(db)

      const collection = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => collection.cleanup())

      await collection.stateWhenReady()

      // Verify the collection state contains our items
      expect(collection.size).toBe(3)

      const id = randomUUID()
      const tx = collection.insert({
        id,
        name: `new`,
      })

      // The insert should optimistically update the collection
      const newDoc = collection.get(id)
      expect(newDoc).toBeDefined()
      expect(newDoc!.name).toBe(`new`)

      await tx.isPersisted.promise
      // The item should now be present in PowerSync
      // We should also have patched it back in to Tanstack DB (removing the optimistic state)

      // Now do an update
      await collection.update(id, (d) => (d.name = `updatedNew`)).isPersisted
        .promise

      const updatedDoc = collection.get(id)
      expect(updatedDoc).toBeDefined()
      expect(updatedDoc!.name).toBe(`updatedNew`)

      await collection.delete(id).isPersisted.promise

      // There should be a crud entries for this
      const _crudEntries = await db.getAll(`
        SELECT * FROM ps_crud ORDER BY id`)

      const crudEntries = _crudEntries.map((r) => CrudEntry.fromRow(r as any))

      expect(crudEntries.length).toBe(6)
      // We can only group transactions for similar operations
      expect(crudEntries.map((e) => e.op)).toEqual([
        `PUT`,
        `PUT`,
        `PUT`,
        `PUT`,
        `PATCH`,
        `DELETE`,
      ])
    })

    it(`should handle transactions`, async () => {
      const db = await createDatabase()
      await createTestData(db)

      const collection = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => collection.cleanup())

      await collection.stateWhenReady()

      expect(collection.size).toBe(3)

      const addTx = createTransaction({
        autoCommit: false,
        mutationFn: async ({ transaction }) => {
          await new PowerSyncTransactor({ database: db }).applyTransaction(
            transaction
          )
        },
      })

      addTx.mutate(() => {
        for (let i = 0; i < 5; i++) {
          collection.insert({ id: randomUUID(), name: `tx-${i}` })
        }
      })

      await addTx.commit()
      await addTx.isPersisted.promise

      expect(collection.size).toBe(8)

      // fetch the ps_crud items
      // There should be a crud entries for this
      const _crudEntries = await db.getAll(`
        SELECT * FROM ps_crud ORDER BY id`)
      const crudEntries = _crudEntries.map((r) => CrudEntry.fromRow(r as any))

      const lastTransactionId =
        crudEntries[crudEntries.length - 1]?.transactionId
      /**
       * The last items, created in the same transaction, should be in the same
       * PowerSync transaction.
       */
      expect(
        crudEntries
          .reverse()
          .slice(0, 5)
          .every((crudEntry) => crudEntry.transactionId == lastTransactionId)
      ).true
    })

    it(`should handle transactions with multiple collections`, async () => {
      const db = await createDatabase()
      await createTestData(db)

      const documentsCollection = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => documentsCollection.cleanup())

      const usersCollection = createCollection(
        powerSyncCollectionOptions<User>({
          database: db,
          tableName: `users`,
        })
      )
      onTestFinished(() => usersCollection.cleanup())

      await documentsCollection.stateWhenReady()
      await usersCollection.stateWhenReady()

      expect(documentsCollection.size).toBe(3)
      expect(usersCollection.size).toBe(0)

      const addTx = createTransaction({
        autoCommit: false,
        mutationFn: async ({ transaction }) => {
          await new PowerSyncTransactor({ database: db }).applyTransaction(
            transaction
          )
        },
      })

      addTx.mutate(() => {
        for (let i = 0; i < 5; i++) {
          documentsCollection.insert({ id: randomUUID(), name: `tx-${i}` })
          usersCollection.insert({ id: randomUUID(), name: `user` })
        }
      })

      await addTx.commit()
      await addTx.isPersisted.promise

      expect(documentsCollection.size).toBe(8)
      expect(usersCollection.size).toBe(5)

      // fetch the ps_crud items
      // There should be a crud entries for this
      const _crudEntries = await db.getAll(`
        SELECT * FROM ps_crud ORDER BY id`)
      const crudEntries = _crudEntries.map((r) => CrudEntry.fromRow(r as any))

      const lastTransactionId =
        crudEntries[crudEntries.length - 1]?.transactionId
      /**
       * The last items, created in the same transaction, should be in the same
       * PowerSync transaction.
       */
      expect(
        crudEntries
          .reverse()
          .slice(0, 10)
          .every((crudEntry) => crudEntry.transactionId == lastTransactionId)
      ).true
    })
  })

  describe(`General use`, async () => {
    it(`should rollback transactions on error`, async () => {
      const db = await createDatabase()

      // Create two collections for the same table
      const collection = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => collection.cleanup())

      const addTx = createTransaction({
        autoCommit: false,
        mutationFn: async ({ transaction }) => {
          await new PowerSyncTransactor({ database: db }).applyTransaction(
            transaction
          )
        },
      })

      expect(collection.size).eq(0)
      const id = randomUUID()
      // Attempt to insert invalid data
      // We can only do this since we aren't using schema validation here
      addTx.mutate(() => {
        collection.insert({
          id,
          name: new Error() as unknown as string, // This will cause a SQL error eventually
        })
      })

      // This should be present in the optimisic state, but should be reverted when attempting to persist
      expect(collection.size).eq(1)
      expect((collection.get(id)?.name as any) instanceof Error).true

      try {
        await addTx.commit()
        await addTx.isPersisted.promise
        expect.fail(`Should have thrown an error`)
      } catch (error) {
        expect(error).toBeDefined()
        // The collection should be in a clean state
        expect(collection.size).toBe(0)
      }
    })
  })

  describe(`Multiple Clients`, async () => {
    it(`should sync updates between multiple clients`, async () => {
      const db = await createDatabase()

      // Create two collections for the same table
      const collectionA = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => collectionA.cleanup())
      await collectionA.stateWhenReady()

      const collectionB = createCollection(
        powerSyncCollectionOptions<Document>({
          database: db,
          tableName: `documents`,
        })
      )
      onTestFinished(() => collectionB.cleanup())
      await collectionB.stateWhenReady()

      await createTestData(db)

      // Both collections should have the data present after insertion
      await vi.waitFor(
        () => {
          expect(collectionA.size).eq(3)
          expect(collectionB.size).eq(3)
        },
        { timeout: 1000 }
      )
    })
  })

  describe(`Lifecycle`, async () => {
    it(`should cleanup resources`, async () => {
      const db = await createDatabase()
      const collectionOptions = powerSyncCollectionOptions<Document>({
        database: db,
        tableName: `documents`,
      })

      const meta = collectionOptions.utils.getMeta()

      const tableExists = async (): Promise<boolean> => {
        const result = await db.writeLock(async (tx) => {
          return tx.get<{ count: number }>(
            `
              SELECT COUNT(*) as count 
              FROM sqlite_temp_master 
              WHERE type='table' AND name = ?
            `,
            [meta.trackedTableName]
          )
        })
        return result.count > 0
      }

      const collection = createCollection(collectionOptions)
      await collection.stateWhenReady()
      expect(await tableExists()).true

      await collection.cleanup()

      // It seems that even though `cleanup` is async, the sync disposer cannot be async
      // We wait for the table to be deleted
      await vi.waitFor(
        async () => {
          expect(await tableExists()).false
        },
        { timeout: 1000 }
      )
    })
  })
})
