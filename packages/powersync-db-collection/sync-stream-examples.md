## Incorporating Sync Streams

Ideally we would be able to map TanstackDB queries to sync streams automatically, if we can optimise the amount of data sync to
the sqlite database from the service we have smaller set of data that needs to be considered when syncing from the sqlite database to TanstackDB collections.

As a stepping stone towards that, we now expose data loading hooks for both eager and on-demand sync modes that allow a user to call sync streams when a collection is defined (eager mode) or when a collection's data boundary changes based on the live queries predicates (on-demand).
For the these examples we are assuming the follow sync stream exists:

```
config:
  edition: 3

streams:
  lists:
    query: SELECT * FROM lists WHERE owner_id = auth.user_id()
    auto_subscribe: true
  todos:
    query: SELECT * FROM todos WHERE list_id = subscription.parameter('list') AND list_id IN (SELECT id FROM lists WHERE owner_id = auth.user_id())
```

### Example 1: Eager mode basic usage

If you want an eager collection to subscribe to a sync stream when a collection loads, you can use the `onLoad` hook.
The hook may return a cleanup function.

Consider the diagram as an example.
We start with 4 todos in the PS service, only 2 todos get synced via the sync stream to the SQLite database. Because it's eager mode, both get synced from the SQLite database to the collection. Finally the TanstackDB query only returns the single todo that matches the live query predicate.

```typescript
const collection = createCollection(
  powerSyncCollectionOptions({
    database: db,
    table: AppSchema.props.todos,
    syncMode: 'eager',
    onLoad: async () => {
      console.log('onLoad')
      const subscription = await db
        .syncStream('todos', { list: 'list_1' })
        .subscribe({ ttl: 0 })

      await subscription.waitForFirstSync()

      return () => {
        console.log('onUnload')
        subscription.unsubscribe()
      }
    },
  }),
)
```

A live query that filters by completed.

```typescript
const liveQuery = createLiveQueryCollection({
  query: (q) =>
    q
      .from({ todo: collection })
      .where(({ todo }) => eq(todo.completed, 1))
      .select(({ todo }) => ({
        id: todo.id,
        completed: todo.completed,
      })),
})
```

### Example 2: On-demand basic usage

If you want to on-demand collection to subscribe to a sync stream whenever a subset of data is loaded (when the list of live queries against the collection change), you can use the `onLoadSubset` hook.
The hook may return a cleanup function.

Consider the diagram as an example.
We start with 4 todos in the PS service, only 2 todos get synced via the sync stream to the SQLite database. Because it's on-demand mode, only 1 todo matches gets synced from the SQLite database to the collection. Finally the TanstackDB query only returns the single todo that matches the live query predicate.

```typescript
const collection = createCollection(
  powerSyncCollectionOptions({
    database: db,
    table: AppSchema.props.todos,
    syncMode: 'on-demand',
    onLoadSubset: async (options) => {
      console.log('onLoadSubset')
      const subscription = await db
        .syncStream('todos', { list: 'list_1' })
        .subscribe({ ttl: 0 })

      await subscription.waitForFirstSync()

      return () => {
        console.log('onUnloadSubset')
        subscription.unsubscribe()
      }
    },
  }),
)
```

A live query that filters by completed.

```typescript
const liveQuery = createLiveQueryCollection({
  query: (q) =>
    q
      .from({ todo: collection })
      .where(({ todo }) => eq(todo.completed, 1))
      .select(({ todo }) => ({
        id: todo.id,
        completed: todo.completed,
      })),
})
```

### Example 3: Extract a single filter value using `extractSimpleComparisons`

Given a live query like:

```
.where(({ todo }) => eq(todo.list_id, selectedListId))
```

`onLoadSubset` receives options.where as an expression tree `for eq(list_id, '<uuid>')`.
We parse it to get the `list_id` value and pass it to `syncStream`.

Consider the diagram as an example. Note it differs from example 1 and 2 as it aims to illustrate `extractSimpleComparisons`.
We start with 4 todos in the PS service, the sync stream subscription criteria (`list_id = "list_1"`) is derived from the live query registered against the collection. Only 2 todos get synced via the sync stream to the SQLite database. Two todos get synced from the SQLite database to the collection. Finally the TanstackDB query returns both todos as they both match `eq(todo.list_id, 'list_id')`.

#### Collection

```typescript
const collection = createCollection(
  powerSyncCollectionOptions({
    database: db,
    table: AppSchema.props.todos,
    syncMode: 'on-demand',
    onLoadSubset: async (options) => {
      // Extract simple comparisons from the where expression
      const comparisons = extractSimpleComparisons(options.where)
      // comparisons = [{ field: ['todo', 'list_id'], operator: 'eq', value: '<uuid>' }]

      // Find the list_id filter
      const listIdFilter = comparisons.find(
        (c) => c.field.includes('list_id') && c.operator === 'eq',
      )

      if (!listIdFilter) {
        console.warn('No list_id filter found, skipping sync stream')
        return
      }

      console.log(`Subscribing to todos for list: ${listIdFilter.value}`)

      const subscription = await db
        .syncStream('todos', { list: listIdFilter.value })
        .subscribe({ ttl: 0 })

      await subscription.waitForFirstSync()

      return () => {
        console.log(`Unsubscribing from todos for list: ${listIdFilter.value}`)
        subscription.unsubscribe()
      }
    },
  }),
)
```

#### Live query

Simple filter -> triggers `onLoadSubset` with `eq(list_id, '...')`

```typescript
const liveQuery = createLiveQueryCollection({
  query: (q) =>
    q
      .from({ todo: collection })
      .where(
        ({ todo }) => eq(todo.list_id, 'list_id'), // or some listId variable
      )
      .select(({ todo }) => ({
        id: todo.id,
        completed: todo.completed,
      })),
})
```

### Example 4: Use `parseWhereExpression` with custom handlers

`parseWhereExpression` gives you full control over how each operator is handled.
Here we build a params object for syncStream from the expression tree.

Assume a small adjustment to the sync stream definition of todos (adding the `completed` subscription parameter)

```
todos:
    query: SELECT * FROM todos WHERE list_id = subscription.parameter('list') AND completed = subscription.parameter("completed") AND list_id IN (SELECT id FROM lists WHERE owner_id = auth.user_id())
```

Note: We keep the `list` parameter name as is (consistent with most of our examples), but to correctly work with the following example we need to map it to `list_id`. You may opt to name it as `list_id` in the sync stream definition and skip the mapping process.

Consider the diagram as an example.
We start with 4 todos in the PS service, the sync stream subscription criteria (`list_id = "list_1" and completed = 1`) is derived from the live query registered against the collection. Only 1 todo gets synced via the sync stream to the SQLite database. One todos gets synced from the SQLite database to the collection. Finally the TanstackDB query returns 1 todo that matches `eq(todo.list_id, 'list_id') and eq(todo.completed, 1)`.

#### Collection

```typescript
const collection = createCollection(
  powerSyncCollectionOptions({
    database: db,
    table: AppSchema.props.todos,
    syncMode: 'on-demand',
    onLoadSubset: async (options) => {
      // Parse the where into a flat params record using custom handlers
      const streamParams = parseWhereExpression(options.where, {
        handlers: {
          eq: (field: Array<string>, value: unknown) => {
            const mappedField = mapFields(field[field.length - 1]!)

            return {
              [mappedField]: value,
            }
          },
          and: (...filters: Array<Record<string, unknown>>) =>
            Object.assign({}, ...filters),
        },
        onUnknownOperator: (op, _args) => {
          console.warn(`Ignoring unsupported operator in stream params: ${op}`)
          return {}
        },
      })
      // For a query like: where(({ todo }) => and(eq(todo.list_id, 'abc'), eq(todo.completed, 0)))
      // streamParams = { list: 'abc', completed: 0 }

      if (!streamParams || Object.keys(streamParams).length === 0) {
        console.warn('No stream params extracted, skipping sync stream')
        return
      }

      console.log(
        `Subscribing to todos with params: ${JSON.stringify(streamParams)}`,
      )

      const subscription = await db
        .syncStream('todos', streamParams)
        .subscribe({ ttl: 0 })

      await subscription.waitForFirstSync()

      return () => subscription.unsubscribe()
    },
  }),
)
```

#### Live Query

Compound filter -> triggers `onLoadSubset` with `and(eq(list_id, '...'), eq(completed, 1))`

```typescript
const liveQuery = createLiveQueryCollection({
  query: (q) =>
    q
      .from({ todo: collection })
      .where(({ todo }) =>
        and(eq(todo.list_id, 'list_1'), eq(todo.completed, 1)),
      )
      .select(({ todo }) => ({
        id: todo.id,
        completed: todo.completed,
      })),
})
```
