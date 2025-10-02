import { ColumnType } from "@powersync/common"
import type {
  ColumnsType,
  ExtractColumnValueType,
  Schema,
  Table,
} from "@powersync/common"
import type { StandardSchemaV1 } from "@standard-schema/spec"

/**
 * Utility type that extracts the typed structure of a table based on its column definitions.
 * Maps each column to its corresponding TypeScript type using ExtractColumnValueType.
 *
 * @template Columns - The ColumnsType definition containing column configurations
 * @example
 * ```typescript
 * const table = new Table({
 *   name: column.text,
 *   age: column.integer
 * })
 * type TableType = ExtractedTable<typeof table.columnMap>
 * // Results in: { name: string | null, age: number | null }
 * ```
 */
type ExtractedTable<Columns extends ColumnsType> = {
  [K in keyof Columns]: ExtractColumnValueType<Columns[K]>
} & {
  id: string
}

/**
 * Converts a PowerSync Table instance to a StandardSchemaV1 schema.
 * Creates a schema that validates the structure and types of table records
 * according to the PowerSync table definition.
 *
 * @template Columns - The ColumnsType definition containing column configurations
 * @param table - The PowerSync Table instance to convert
 * @returns A StandardSchemaV1 compatible schema with proper type validation
 *
 * @example
 * ```typescript
 * const usersTable = new Table({
 *   name: column.text,
 *   age: column.integer
 * })
 *
 * const schema = convertTableToSchema(usersTable)
 * // Now you can use this schema with powerSyncCollectionOptions
 * const collection = createCollection(
 *   powerSyncCollectionOptions({
 *     database: db,
 *     tableName: "users",
 *     schema: schema
 *   })
 * )
 * ```
 */
export function convertTableToSchema<Columns extends ColumnsType>(
  table: Table<Columns>
): StandardSchemaV1<ExtractedTable<Columns>> {
  // Create validate function that checks types according to column definitions
  const validate = (
    value: unknown
  ):
    | StandardSchemaV1.SuccessResult<ExtractedTable<Columns>>
    | StandardSchemaV1.FailureResult => {
    if (typeof value != `object` || value == null) {
      return {
        issues: [
          {
            message: `Value must be an object`,
          },
        ],
      }
    }

    const issues: Array<StandardSchemaV1.Issue> = []

    // Check id field
    if (!(`id` in value) || typeof (value as any).id != `string`) {
      issues.push({
        message: `id field must be a string`,
        path: [`id`],
      })
    }

    // Check each column
    for (const column of table.columns) {
      const val = (value as ExtractedTable<Columns>)[column.name]

      if (val == null) {
        continue
      }

      switch (column.type) {
        case ColumnType.TEXT:
          if (typeof val != `string`) {
            issues.push({
              message: `${column.name} must be a string or null`,
              path: [column.name],
            })
          }
          break
        case ColumnType.INTEGER:
        case ColumnType.REAL:
          if (typeof val != `number`) {
            issues.push({
              message: `${column.name} must be a number or null`,
              path: [column.name],
            })
          }
          break
      }
    }

    if (issues.length > 0) {
      return { issues }
    }

    return { value: { ...value } as ExtractedTable<Columns> }
  }

  return {
    "~standard": {
      version: 1,
      vendor: `powersync`,
      validate,
      types: {
        input: {} as ExtractedTable<Columns>,
        output: {} as ExtractedTable<Columns>,
      },
    },
  }
}

/**
 * Converts an entire PowerSync Schema (containing multiple tables) into a collection of StandardSchemaV1 schemas.
 * Each table in the schema is converted to its own StandardSchemaV1 schema while preserving all type information.
 *
 * @template Tables - A record type mapping table names to their Table definitions
 * @param schema - The PowerSync Schema containing multiple table definitions
 * @returns An object where each key is a table name and each value is that table's StandardSchemaV1 schema
 *
 * @example
 * ```typescript
 * const mySchema = new Schema({
 *   users: new Table({
 *     name: column.text,
 *     age: column.integer
 *   }),
 *   posts: new Table({
 *     title: column.text,
 *     views: column.integer
 *   })
 * })
 *
 * const standardizedSchemas = convertSchemaToSpecs(mySchema)
 * // Result has type:
 * // {
 * //   users: StandardSchemaV1<{ name: string | null, age: number | null }>,
 * //   posts: StandardSchemaV1<{ title: string | null, views: number | null }>
 * // }
 *
 * // Can be used with collections:
 * const usersCollection = createCollection(
 *   powerSyncCollectionOptions({
 *     database: db,
 *     tableName: "users",
 *     schema: standardizedSchemas.users
 *   })
 * )
 * ```
 */
export function convertPowerSyncSchemaToSpecs<
  Tables extends Record<string, Table<ColumnsType>>,
>(
  schema: Schema<Tables>
): {
  [TableName in keyof Tables]: StandardSchemaV1<
    ExtractedTable<Tables[TableName][`columnMap`]>
  >
} {
  // Create a map to store the standardized schemas
  const standardizedSchemas = {} as {
    [TableName in keyof Tables]: StandardSchemaV1<
      ExtractedTable<Tables[TableName][`columnMap`]>
    >
  }

  // Iterate through each table in the schema
  schema.tables.forEach((table) => {
    // Convert each table to a StandardSchemaV1 and store it in the result map
    ;(standardizedSchemas as any)[table.name] = convertTableToSchema(table)
  })

  return standardizedSchemas
}
