import { Schema, Table, column } from "@powersync/common"
import { describe, expect, it } from "vitest"
import {
  convertPowerSyncSchemaToSpecs,
  convertTableToSchema,
} from "../src/schema"
import type { StandardSchemaV1 } from "@standard-schema/spec"

describe(`Schema Conversion`, () => {
  describe(`convertTableToSchema`, () => {
    it(`should convert a simple table with text and integer columns`, () => {
      const table = new Table({
        name: column.text,
        age: column.integer,
      })

      const schema = convertTableToSchema(table)

      // Test schema structure
      expect(schema).toHaveProperty(`~standard`)
      expect(schema[`~standard`].version).toBe(1)
      expect(schema[`~standard`].vendor).toBe(`powersync`)
      expect(schema[`~standard`].validate).toBeTypeOf(`function`)

      // Test validation with valid data
      const validResult = schema[`~standard`].validate({
        id: `123`,
        name: `John`,
        age: 25,
      }) as StandardSchemaV1.SuccessResult<any>

      expect(validResult.issues).toBeUndefined()
      expect(validResult.value).toEqual({
        id: `123`,
        name: `John`,
        age: 25,
      })

      // Test validation with invalid data
      const invalidResult = schema[`~standard`].validate({
        id: `123`,
        name: 123, // wrong type
        age: `25`, // wrong type
      }) as StandardSchemaV1.FailureResult

      expect(invalidResult.issues).toHaveLength(2)
      expect(invalidResult.issues[0]?.message).toContain(`must be a string`)
      expect(invalidResult.issues[1]?.message).toContain(`must be a number`)
    })

    it(`should handle null values correctly`, () => {
      const table = new Table({
        name: column.text,
        age: column.integer,
      })

      const schema = convertTableToSchema(table)

      // Test validation with null values
      const result = schema[`~standard`].validate({
        id: `123`,
        name: null,
        age: null,
      }) as StandardSchemaV1.SuccessResult<any>

      expect(result.issues).toBeUndefined()
      expect(result.value).toEqual({
        id: `123`,
        name: null,
        age: null,
      })
    })

    it(`should require id field`, () => {
      const table = new Table({
        name: column.text,
      })

      const schema = convertTableToSchema(table)

      // Test validation without id
      const result = schema[`~standard`].validate({
        name: `John`,
      }) as StandardSchemaV1.FailureResult

      expect(result.issues).toHaveLength(1)
      expect(result.issues[0]?.message).toContain(`id field must be a string`)
    })

    it(`should handle all column types`, () => {
      const table = new Table({
        text_col: column.text,
        int_col: column.integer,
        real_col: column.real,
      })

      const schema = convertTableToSchema(table)

      // Test validation with all types
      const result = schema[`~standard`].validate({
        id: `123`,
        text_col: `text`,
        int_col: 42,
        real_col: 3.14,
      }) as StandardSchemaV1.SuccessResult<any>

      expect(result.issues).toBeUndefined()
      expect(result.value).toEqual({
        id: `123`,
        text_col: `text`,
        int_col: 42,
        real_col: 3.14,
      })
    })
  })

  describe(`convertPowerSyncSchemaToSpecs`, () => {
    it(`should convert multiple tables in a schema`, () => {
      const schema = new Schema({
        users: new Table({
          name: column.text,
          age: column.integer,
        }),
        posts: new Table({
          title: column.text,
          views: column.integer,
        }),
      })

      const result = convertPowerSyncSchemaToSpecs(schema)

      // Test structure
      expect(result).toHaveProperty(`users`)
      expect(result).toHaveProperty(`posts`)

      // Test users table schema
      const userValidResult = result.users[`~standard`].validate({
        id: `123`,
        name: `John`,
        age: 25,
      }) as StandardSchemaV1.SuccessResult<any>

      expect(userValidResult.issues).toBeUndefined()
      expect(userValidResult.value).toEqual({
        id: `123`,
        name: `John`,
        age: 25,
      })

      // Test posts table schema
      const postValidResult = result.posts[`~standard`].validate({
        id: `456`,
        title: `Hello`,
        views: 100,
      }) as StandardSchemaV1.SuccessResult<any>

      expect(postValidResult.issues).toBeUndefined()
      expect(postValidResult.value).toEqual({
        id: `456`,
        title: `Hello`,
        views: 100,
      })
    })

    it(`should handle empty schema`, () => {
      const schema = new Schema({})
      const result = convertPowerSyncSchemaToSpecs(schema)
      expect(result).toEqual({})
    })

    it(`should validate each table independently`, () => {
      const schema = new Schema({
        users: new Table({
          name: column.text,
        }),
        posts: new Table({
          views: column.integer,
        }),
      })

      const result = convertPowerSyncSchemaToSpecs(schema)

      // Test that invalid data in one table doesn't affect the other
      const userInvalidResult = result.users[`~standard`].validate({
        id: `123`,
        name: 42, // wrong type
      }) as StandardSchemaV1.FailureResult

      const postValidResult = result.posts[`~standard`].validate({
        id: `456`,
        views: 100,
      }) as StandardSchemaV1.SuccessResult<any>

      expect(userInvalidResult.issues).toHaveLength(1)
      expect(postValidResult.issues).toBeUndefined()
    })
  })
})
