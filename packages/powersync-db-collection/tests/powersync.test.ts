import { randomUUID } from "node:crypto"
import { tmpdir } from "node:os"
import {
  CrudEntry,
  PowerSyncDatabase,
  Schema,
  Table,
  column,
} from "@powersync/node"
import {
  SchemaValidationError,
  createCollection,
  createTransaction,
  eq,
  liveQueryCollectionOptions,
} from "@tanstack/db"
import { describe, expect, it, onTestFinished, vi } from "vitest"
import { z } from "zod"
import { powerSyncCollectionOptions } from "../src"
import { PowerSyncTransactor } from "../src/PowerSyncTransactor"
import type { AbstractPowerSyncDatabase } from "@powersync/node"

const APP_SCHEMA = new Schema({
  users: new Table({
    name: column.text,
  }),
  documents: new Table(
    {
      name: column.text,
    },
    { viewName: `documents` }
  ),
})

describe(`PowerSync Integration`, () => {
  async function createDatabase() {
    const db = new PowerSyncDatabase({
      database: {
        dbFilename: `test.sqlite`,
        dbLocation: tmpdir(),
        implementation: { type: `node:sqlite` },
      },
      schema: APP_SCHEMA,
    })
    onTestFinished(async () => {
      await db.disconnectAndClear()
      await db.close()
    })
    // Initial clear in case a test might have failed
    await db.disconnectAndClear()
    return db
  }

  function createDocumentsCollection(db: PowerSyncDatabase) {
    const collection = createCollection(
      powerSyncCollectionOptions({
        database: db,
        // We get typing and a default validator from this
        table: APP_SCHEMA.props.documents,
      })
    )
    onTestFinished(() => collection.cleanup())
    return collection
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

  describe(`schema`, () => {
    it(`should use basic runtime validations from automatic schema`, async () => {
      const db = await createDatabase()

      // the collection should infer types and validate with the schema
      const collection = createDocumentsCollection(db)

      collection.insert({
        id: randomUUID(),
        name: `aname`,
      })

      collection.insert({
        id: randomUUID(),
        name: null,
      })

      expect(collection.size).eq(2)

      // should validate inputs
      try {
        collection.insert({} as any)
        console.log(`failed`)
      } catch (ex) {
        expect(ex instanceof SchemaValidationError).true
        if (ex instanceof SchemaValidationError) {
          expect(ex.message).contains(`id field must be a string`)
        }
      }
    })

    it(`should allow for advanced validations`, async () => {
      const db = await createDatabase()

      const errorMessage = `Name must be at least 3 characters`
      const schema = z.object({
        id: z.string(),
        name: z.string().min(3, { message: errorMessage }).nullable(),
      })

      const collection = createCollection(
        powerSyncCollectionOptions({
          database: db,
          table: APP_SCHEMA.props.documents,
          schema,
        })
      )
      onTestFinished(() => collection.cleanup())

      try {
        collection.insert({
          id: randomUUID(),
          name: `2`,
        })
        expect.fail(`Should throw a validation error`)
      } catch (ex) {
        expect(ex instanceof SchemaValidationError).true
        if (ex instanceof SchemaValidationError) {
          console.log(ex)
          expect(ex.message).contains(errorMessage)
        }
      }

      collection.insert({
        id: randomUUID(),
        name: null,
      })

      expect(collection.size).eq(1)

      // should validate inputs
      try {
        collection.insert({} as any)
        console.log(`failed`)
      } catch (ex) {
        expect(ex instanceof SchemaValidationError).true
        if (ex instanceof SchemaValidationError) {
          console.log(ex)
          expect(ex.message).contains(`Required - path: id`)
        }
      }
    })
  })

  describe(`sync`, () => {
    it(`should initialize and fetch initial data`, async () => {
      const db = await createDatabase()
      await createTestData(db)
      const collection = createDocumentsCollection(db)

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

      const collection = createDocumentsCollection(db)

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

      const collection = createDocumentsCollection(db)
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

      const collection = createDocumentsCollection(db)
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

      const documentsCollection = createDocumentsCollection(db)

      const usersCollection = createCollection(
        powerSyncCollectionOptions({
          database: db,
          table: APP_SCHEMA.props.users,
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

  describe(`General use`, () => {
    it(`should rollback transactions on error`, async () => {
      const db = await createDatabase()

      const options = powerSyncCollectionOptions({
        database: db,
        table: APP_SCHEMA.props.documents,
      })

      // This will cause the transactor to fail when writing to SQLite
      vi.spyOn(options.utils, `getMeta`).mockImplementation(() => ({
        tableName: `fakeTable`,
        trackedTableName: `error`,
      }))
      // Create two collections for the same table
      const collection = createCollection(options)

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
      await collection.stateWhenReady()

      const id = randomUUID()
      addTx.mutate(() => {
        collection.insert({
          id,
          name: `aname`,
        })
      })

      // This should be present in the optimistic state, but should be reverted when attempting to persist
      expect(collection.size).eq(1)

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

    it(`should work with live queries`, async () => {
      const db = await createDatabase()

      // Create two collections for the same table
      const collection = createDocumentsCollection(db)

      await collection.stateWhenReady()

      const liveDocuments = createCollection(
        liveQueryCollectionOptions({
          query: (q) =>
            q
              .from({ document: collection })
              .where(({ document }) => eq(document.name, `book`))
              .select(({ document }) => ({
                id: document.id,
                name: document.name,
              })),
        })
      )

      expect(liveDocuments.size).eq(0)

      const bookNames = new Set<string>()

      liveDocuments.subscribeChanges((changes) => {
        changes
          .map((change) => change.value.name)
          .forEach((change) => bookNames.add(change!))
      })

      await collection.insert({
        id: randomUUID(),
        name: `notabook`,
      }).isPersisted.promise
      await collection.insert({
        id: randomUUID(),
        name: `book`,
      }).isPersisted.promise

      expect(collection.size).eq(2)
      await vi.waitFor(
        () => {
          expect(Array.from(bookNames)).deep.equals([`book`])
        },
        { timeout: 1000 }
      )
    })
  })

  describe(`Multiple Clients`, async () => {
    it(`should sync updates between multiple clients`, async () => {
      const db = await createDatabase()

      // Create two collections for the same table
      const collectionA = createDocumentsCollection(db)
      await collectionA.stateWhenReady()

      const collectionB = createDocumentsCollection(db)
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
      const collectionOptions = powerSyncCollectionOptions({
        database: db,
        table: APP_SCHEMA.props.documents,
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
