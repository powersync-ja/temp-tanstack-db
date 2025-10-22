# @tanstack/electric-db-collection

## 0.1.35

### Patch Changes

- Updated dependencies [[`5566b26`](https://github.com/TanStack/db/commit/5566b26100abdae9b4a041f048aeda1dd726e904)]:
  - @tanstack/db@0.4.11

## 0.1.34

### Patch Changes

- Updated dependencies [[`63aa8ef`](https://github.com/TanStack/db/commit/63aa8ef8b09960ce0f93e068d41b37fb0503a21a), [`b0687ab`](https://github.com/TanStack/db/commit/b0687ab4c1476362d7a25e3c1704ab0fb0385455)]:
  - @tanstack/db@0.4.10

## 0.1.33

### Patch Changes

- Updated dependencies [[`e52be92`](https://github.com/TanStack/db/commit/e52be92ce16b09a095b4b9baf7ac2cf708146f47), [`4a7c44a`](https://github.com/TanStack/db/commit/4a7c44a723223ade4e226745eadffead671fff13), [`ee61bb6`](https://github.com/TanStack/db/commit/ee61bb61f76ca510f113e96baa090940719aac40)]:
  - @tanstack/db@0.4.9

## 0.1.32

### Patch Changes

- Updated dependencies [[`d9ae7b7`](https://github.com/TanStack/db/commit/d9ae7b76b8ab30fd55fe835531974eee333dd450), [`44555b7`](https://github.com/TanStack/db/commit/44555b733a1a4d38d8126bf8da51d4b44f898298)]:
  - @tanstack/db@0.4.8

## 0.1.31

### Patch Changes

- feat: Add awaitMatch utility and reduce default timeout (#402) ([#499](https://github.com/TanStack/db/pull/499))

  Adds a new `awaitMatch` utility function to support custom synchronization matching logic when transaction IDs (txids) are not available. Also reduces the default timeout for `awaitTxId` from 30 seconds to 5 seconds for faster feedback.

  **New Features:**
  - New utility method: `collection.utils.awaitMatch(matchFn, timeout?)` - Wait for custom match logic
  - Export `isChangeMessage` and `isControlMessage` helper functions for custom match functions
  - Type: `MatchFunction<T>` for custom match functions

  **Changes:**
  - Default timeout for `awaitTxId` reduced from 30 seconds to 5 seconds

  **Example Usage:**

  ```typescript
  import { isChangeMessage } from "@tanstack/electric-db-collection"

  const todosCollection = createCollection(
    electricCollectionOptions({
      onInsert: async ({ transaction, collection }) => {
        const newItem = transaction.mutations[0].modified
        await api.todos.create(newItem)

        // Wait for sync using custom match logic
        await collection.utils.awaitMatch(
          (message) =>
            isChangeMessage(message) &&
            message.headers.operation === "insert" &&
            message.value.text === newItem.text,
          5000 // timeout in ms (optional, defaults to 5000)
        )
      },
    })
  )
  ```

  **Benefits:**
  - Supports backends that can't provide transaction IDs
  - Flexible heuristic-based matching
  - Faster feedback on sync issues with reduced timeout

- Updated dependencies [[`6692aad`](https://github.com/TanStack/db/commit/6692aad4267e127b71ce595529080d6fc0aa2066)]:
  - @tanstack/db@0.4.7

## 0.1.30

### Patch Changes

- prefix logs and errors with collection id, when available ([#655](https://github.com/TanStack/db/pull/655))

- Updated dependencies [[`dd6cdf7`](https://github.com/TanStack/db/commit/dd6cdf7ea62d91bfb12ea8d25bdd25549259c113), [`c30a20b`](https://github.com/TanStack/db/commit/c30a20b1df39b34f18d0aa7c7b901a27fb963f36)]:
  - @tanstack/db@0.4.6

## 0.1.29

### Patch Changes

- The awaitTxId utility now resolves transaction IDs based on snapshot-end message metadata (xmin, xmax, xip_list) in addition to explicit txid arrays, enabling matching on the initial snapshot at the start of a new shape. ([#648](https://github.com/TanStack/db/pull/648))

- Updated dependencies [[`7556fb6`](https://github.com/TanStack/db/commit/7556fb6f888b5bdc830fe6448eb3368efeb61988)]:
  - @tanstack/db@0.4.5

## 0.1.28

### Patch Changes

- Updated dependencies [[`56b870b`](https://github.com/TanStack/db/commit/56b870b3e63f8010b6eeebea87893b10c75a5888), [`f623990`](https://github.com/TanStack/db/commit/f62399062e4db61426ddfbbbe324c48cab2513dd), [`5f43d5f`](https://github.com/TanStack/db/commit/5f43d5f7f47614be8e71856ceb0f91733d9be627), [`05776f5`](https://github.com/TanStack/db/commit/05776f52a8ce4fe41b34fc8cace2046afc42835c), [`d27d32a`](https://github.com/TanStack/db/commit/d27d32aceb7f8fcabc07dcf1b55a84a605d2f23f)]:
  - @tanstack/db@0.4.4

## 0.1.27

### Patch Changes

- Updated dependencies [[`32f2212`](https://github.com/TanStack/db/commit/32f221278e2a684f3f4e1e2ace1ca98f5ecc858a)]:
  - @tanstack/db@0.4.3

## 0.1.26

### Patch Changes

- Fix repeated renders when markReady called when the collection was already ready. This would occur after each long poll on an Electric collection. ([#604](https://github.com/TanStack/db/pull/604))

- Updated dependencies [[`51c6bc5`](https://github.com/TanStack/db/commit/51c6bc58244ed6a3ac853e7e6af7775b33d6b65a), [`248e2c6`](https://github.com/TanStack/db/commit/248e2c6db8e9df8cf2cb225100e4ba9cb67cd534), [`ce7e2b2`](https://github.com/TanStack/db/commit/ce7e2b209ed882baa29ec86f89f1b527d6580e0b), [`1b832ff`](https://github.com/TanStack/db/commit/1b832ff9ec236e7dbe9256803e2ba12b4c9b9a30)]:
  - @tanstack/db@0.4.2

## 0.1.25

### Patch Changes

- Updated dependencies [[`8cd0876`](https://github.com/TanStack/db/commit/8cd0876b50bc7c1a614365318d5e74c2f32a0f80)]:
  - @tanstack/db@0.4.1

## 0.1.24

### Patch Changes

- Refactor the main Collection class into smaller classes to make it easier to maintain. ([#560](https://github.com/TanStack/db/pull/560))

- Updated dependencies [[`2f87216`](https://github.com/TanStack/db/commit/2f8721630e06331ca8bb2f962fbb283341103a58), [`ac6250a`](https://github.com/TanStack/db/commit/ac6250a879e95718e8d911732c10fb3388569f0f), [`2f87216`](https://github.com/TanStack/db/commit/2f8721630e06331ca8bb2f962fbb283341103a58)]:
  - @tanstack/db@0.4.0

## 0.1.23

### Patch Changes

- Pull in the latest version of @electric-sql/client instead of pinning it ([#572](https://github.com/TanStack/db/pull/572))

- Updated dependencies [[`cacfca2`](https://github.com/TanStack/db/commit/cacfca2d1b430c34a05202128fd3affa4bff54d6)]:
  - @tanstack/db@0.3.2

## 0.1.22

### Patch Changes

- Updated dependencies [[`5f51f35`](https://github.com/TanStack/db/commit/5f51f35d2c9543766a00cc5eea1374c62798b34e)]:
  - @tanstack/db@0.3.1

## 0.1.21

### Patch Changes

- Updated dependencies [[`c557a14`](https://github.com/TanStack/db/commit/c557a1488650ea9081b671a4ac278d55c59ac9cc), [`b5c87f7`](https://github.com/TanStack/db/commit/b5c87f71dbb534e4f1c660cf010e2cb6c0446ec5)]:
  - @tanstack/db@0.3.0

## 0.1.20

### Patch Changes

- Refactor of the types of collection config factories for better type inference. ([#530](https://github.com/TanStack/db/pull/530))

- Define BaseCollectionConfig interface and let all collections extend it. ([#531](https://github.com/TanStack/db/pull/531))

- Updated dependencies [[`b03894d`](https://github.com/TanStack/db/commit/b03894db05e063629a3660e03b31a80a48558dd5), [`3968087`](https://github.com/TanStack/db/commit/39680877fdc1993733933d2def13217bd18fa254)]:
  - @tanstack/db@0.2.5

## 0.1.19

### Patch Changes

- Updated dependencies [[`92febbf`](https://github.com/TanStack/db/commit/92febbf1feaa1d46f8cc4d7a4ea0d44cd5f85256), [`b487430`](https://github.com/TanStack/db/commit/b4874308813f95232f3361de539cec104ed55170)]:
  - @tanstack/db@0.2.4

## 0.1.18

### Patch Changes

- Fixed a bug where a live query could get stuck in "loading" state, or show incomplete data, when an electric "must-refetch" message arrived before the first "up-to-date". ([#532](https://github.com/TanStack/db/pull/532))

- Updated dependencies [[`b162556`](https://github.com/TanStack/db/commit/b1625565df44b0824501297f7ef14ae1cd450b49)]:
  - @tanstack/db@0.2.3

## 0.1.17

### Patch Changes

- fix AbortController re-initialization after a collection is garbage collected ([#523](https://github.com/TanStack/db/pull/523))

## 0.1.16

### Patch Changes

- Updated dependencies [[`33515c6`](https://github.com/TanStack/db/commit/33515c69befc557add2cf828354ee378100f3977)]:
  - @tanstack/db@0.2.2

## 0.1.15

### Patch Changes

- Updated dependencies [[`620ebea`](https://github.com/TanStack/db/commit/620ebea96eb3fbeec66701b949de9920c4084c17)]:
  - @tanstack/db@0.2.1

## 0.1.14

### Patch Changes

- Updated dependencies [[`08303e6`](https://github.com/TanStack/db/commit/08303e645974db97e10b2aca0031abcbce027dd6), [`bafeaa1`](https://github.com/TanStack/db/commit/bafeaa1e9f161ac2200ce86537e442b2aa8e2a5b), [`1814f8c`](https://github.com/TanStack/db/commit/1814f8cc3c0e831c82f8053b86fbbbd737e4f34b), [`31acdf2`](https://github.com/TanStack/db/commit/31acdf2a96411da327f93f0d30fa78d884422969), [`e41ed7e`](https://github.com/TanStack/db/commit/e41ed7e1ff1d94dd3ce0c48b6321f66b8ea044fd), [`51954d8`](https://github.com/TanStack/db/commit/51954d8c5d64291d136159bce293e0ad00a19f88)]:
  - @tanstack/db@0.2.0

## 0.1.13

### Patch Changes

- Updated dependencies [[`cc4c34a`](https://github.com/TanStack/db/commit/cc4c34a6b40c81c83aa10c8d00dfc0a3d33c56db)]:
  - @tanstack/db@0.1.12

## 0.1.12

### Patch Changes

- Updated dependencies [[`b869f68`](https://github.com/TanStack/db/commit/b869f68f0109b3126509f202a38855cee38b4276)]:
  - @tanstack/db@0.1.11

## 0.1.11

### Patch Changes

- Updated dependencies [[`eb8fd18`](https://github.com/TanStack/db/commit/eb8fd18c50ee03b72cb06e4d7ef25f214367950b), [`e59a355`](https://github.com/TanStack/db/commit/e59a3551e75bac9dd166e14c911d9491e3a67b9a), [`074aab0`](https://github.com/TanStack/db/commit/074aab0477a7c55e9e0f19a705b96ed2619e2afb), [`d469c39`](https://github.com/TanStack/db/commit/d469c39a7bdc034fa4fbc533010573b3515f239f)]:
  - @tanstack/db@0.1.10

## 0.1.10

### Patch Changes

- Updated dependencies [[`d64b4a8`](https://github.com/TanStack/db/commit/d64b4a8b692a213c7ad58faaf66f5f5fd50bef66)]:
  - @tanstack/db@0.1.9

## 0.1.9

### Patch Changes

- fix the handling of an electric must-refetch message so that the truncate is handled in the same transaction as the next up-to-date, ensuring you don't get a momentary empty collection. ([#460](https://github.com/TanStack/db/pull/460))

- fix disabling of gc by setting `gcTime: 0` on the collection options ([#463](https://github.com/TanStack/db/pull/463))

- Updated dependencies [[`1c5e206`](https://github.com/TanStack/db/commit/1c5e206d00d0a99f8419f0d00429b5a3c6cdc76e), [`4d20004`](https://github.com/TanStack/db/commit/4d2000488b9b5abf85c05801633297528af0eff6), [`968602e`](https://github.com/TanStack/db/commit/968602e4ffc597eaa559219daf22d6ef6321162a)]:
  - @tanstack/db@0.1.8

## 0.1.8

### Patch Changes

- bump electric version ([#454](https://github.com/TanStack/db/pull/454))

## 0.1.7

### Patch Changes

- Updated dependencies [[`48d0889`](https://github.com/TanStack/db/commit/48d088996a3f18df026aa7d2d1e7f27d1151345b), [`aecbcc3`](https://github.com/TanStack/db/commit/aecbcc32012561f1645df0bdf89a6c259058d888), [`a937f4c`](https://github.com/TanStack/db/commit/a937f4c7a5f4fc20c255e86692c5e2e80d5ebbec), [`3d60fad`](https://github.com/TanStack/db/commit/3d60fadbb9e8a1b62a9bcde947e282d653a2a270), [`79c95a3`](https://github.com/TanStack/db/commit/79c95a36f60087ffc3f9a02b76975c8bdf40acc7)]:
  - @tanstack/db@0.1.7

## 0.1.6

### Patch Changes

- Updated dependencies [[`ad33e9e`](https://github.com/TanStack/db/commit/ad33e9e535ca6197c2e00e2dbb59bf8e8f9bb51e)]:
  - @tanstack/db@0.1.6

## 0.1.5

### Patch Changes

- Updated dependencies [[`9a5a20c`](https://github.com/TanStack/db/commit/9a5a20c21fbf8286ab90e1db6d6f3315f8344a4e)]:
  - @tanstack/db@0.1.5

## 0.1.4

### Patch Changes

- Add must-refetch message handling to clear synced data and re-sync collection data from server. ([#412](https://github.com/TanStack/db/pull/412))

- Export the `AwaitTxIdFn` type to ensure that building with types works correctly. ([#398](https://github.com/TanStack/db/pull/398))

- Updated dependencies [[`c90b4d8`](https://github.com/TanStack/db/commit/c90b4d85822f94f7fe72286d5c7ee07b087d0e20), [`6c1c19c`](https://github.com/TanStack/db/commit/6c1c19cedbc1d9d98396948e8e43fa0515bb8919), [`69a6d2d`](https://github.com/TanStack/db/commit/69a6d2d94c7a5510568c8b652356c62bd2b3cc76), [`6250a92`](https://github.com/TanStack/db/commit/6250a92c8045ef2fd69c107a94e05179471681d7), [`68538b4`](https://github.com/TanStack/db/commit/68538b4c446abeb992e24964f811c8900749f141)]:
  - @tanstack/db@0.1.4

## 0.1.3

### Patch Changes

- Updated dependencies [[`0cb7699`](https://github.com/TanStack/db/commit/0cb76999e5d6df5916694a5afeb31b928eab68e4)]:
  - @tanstack/db@0.1.3

## 0.1.2

### Patch Changes

- Ensure that you can use optional properties in the `select` and `join` clauses of a query, and fix an issue where standard schemas were not properly carried through to live queries. ([#377](https://github.com/TanStack/db/pull/377))

- Updated dependencies [[`bb5d50e`](https://github.com/TanStack/db/commit/bb5d50e255d9114ef32b8f52eef6b15399255327), [`97b595e`](https://github.com/TanStack/db/commit/97b595e9617b1abb05c14489e3d608b314da08e8)]:
  - @tanstack/db@0.1.2

## 0.1.1

### Patch Changes

- Updated dependencies [[`bc2f204`](https://github.com/TanStack/db/commit/bc2f204b8cb8a4870ade00757d10f846524e2090), [`bda3f24`](https://github.com/TanStack/db/commit/bda3f24cc41504f60be0c5e071698b7735f75e28)]:
  - @tanstack/db@0.1.1

## 0.1.0

### Minor Changes

- 0.1 release - first beta 🎉 ([#332](https://github.com/TanStack/db/pull/332))

### Patch Changes

- Updated dependencies [[`7d2f4be`](https://github.com/TanStack/db/commit/7d2f4be95c43aad29fb61e80e5a04c58c859322b), [`f0eda36`](https://github.com/TanStack/db/commit/f0eda36cb36350399bc8835686a6c4b6ad297e45)]:
  - @tanstack/db@0.1.0

## 0.0.15

### Patch Changes

- Updated dependencies [[`6e8d7f6`](https://github.com/TanStack/db/commit/6e8d7f660050118e050d575913733e469e3daa8c)]:
  - @tanstack/db@0.0.33

## 0.0.14

### Patch Changes

- Updated dependencies [[`e04bd12`](https://github.com/TanStack/db/commit/e04bd1252f612d4638104368d17cb644cc85295b)]:
  - @tanstack/db@0.0.32

## 0.0.13

### Patch Changes

- Updated dependencies [[`3e9a36d`](https://github.com/TanStack/db/commit/3e9a36d2600c4f700ca7bc4f720c189a5a29387a)]:
  - @tanstack/db@0.0.31

## 0.0.12

### Patch Changes

- Updated dependencies [[`6bdde55`](https://github.com/TanStack/db/commit/6bdde554f36f54c0c4f4dacb74bef5da45811855)]:
  - @tanstack/db@0.0.30

## 0.0.11

### Patch Changes

- feat: Replace string-based errors with named error classes for better error handling ([#297](https://github.com/TanStack/db/pull/297))

  This comprehensive update replaces all string-based error throws throughout the TanStack DB codebase with named error classes, providing better type safety and developer experience.

  ## New Features
  - **Root `TanStackDBError` class** - all errors inherit from a common base for unified error handling
  - **Named error classes** organized by package and functional area
  - **Type-safe error handling** using `instanceof` checks instead of string matching
  - **Package-specific error definitions** - each adapter has its own error classes
  - **Better IDE support** with autocomplete for error types

  ## Package Structure

  ### Core Package (`@tanstack/db`)

  Contains generic errors used across the ecosystem:
  - Collection configuration, state, and operation errors
  - Transaction lifecycle and mutation errors
  - Query building, compilation, and execution errors
  - Storage and serialization errors

  ### Adapter Packages

  Each adapter now exports its own specific error classes:
  - **`@tanstack/electric-db-collection`**: Electric-specific errors
  - **`@tanstack/trailbase-db-collection`**: TrailBase-specific errors
  - **`@tanstack/query-db-collection`**: Query collection specific errors

  ## Breaking Changes
  - Error handling code using string matching will need to be updated to use `instanceof` checks
  - Some error messages may have slight formatting changes
  - Adapter-specific errors now need to be imported from their respective packages

  ## Migration Guide

  ### Core DB Errors

  **Before:**

  ```ts
  try {
    collection.insert(data)
  } catch (error) {
    if (error.message.includes("already exists")) {
      // Handle duplicate key error
    }
  }
  ```

  **After:**

  ```ts
  import { DuplicateKeyError } from "@tanstack/db"

  try {
    collection.insert(data)
  } catch (error) {
    if (error instanceof DuplicateKeyError) {
      // Type-safe error handling
    }
  }
  ```

  ### Adapter-Specific Errors

  **Before:**

  ```ts
  // Electric collection errors were imported from @tanstack/db
  import { ElectricInsertHandlerMustReturnTxIdError } from "@tanstack/db"
  ```

  **After:**

  ```ts
  // Now import from the specific adapter package
  import { ElectricInsertHandlerMustReturnTxIdError } from "@tanstack/electric-db-collection"
  ```

  ### Unified Error Handling

  **New:**

  ```ts
  import { TanStackDBError } from "@tanstack/db"

  try {
    // Any TanStack DB operation
  } catch (error) {
    if (error instanceof TanStackDBError) {
      // Handle all TanStack DB errors uniformly
      console.log("TanStack DB error:", error.message)
    }
  }
  ```

  ## Benefits
  - **Type Safety**: All errors now have specific types that can be caught with `instanceof`
  - **Unified Error Handling**: Root `TanStackDBError` class allows catching all library errors with a single check
  - **Better Package Separation**: Each adapter manages its own error types
  - **Developer Experience**: Better IDE support with autocomplete for error types
  - **Maintainability**: Error definitions are co-located with their usage
  - **Consistency**: Uniform error handling patterns across the entire codebase

  All error classes maintain the same error messages and behavior while providing better structure and package separation.

- Updated dependencies [[`ced0657`](https://github.com/TanStack/db/commit/ced0657e72646e35343dfea8389d96e213710cdf), [`dcfef51`](https://github.com/TanStack/db/commit/dcfef51d4d94c756bf77e40e7015e47b7982c09a), [`360b0df`](https://github.com/TanStack/db/commit/360b0dfa411ba9f8a93f6b737aa1df8fb37dd036), [`608be0c`](https://github.com/TanStack/db/commit/608be0c14dc5ae9577deebf436557a1eace46733), [`5260ee3`](https://github.com/TanStack/db/commit/5260ee3098d12eccc58a5cf903ea479908681402)]:
  - @tanstack/db@0.0.29

## 0.0.10

### Patch Changes

- Updated dependencies [[`bb85522`](https://github.com/TanStack/db/commit/bb8552210a97dd05d3ca6fdd080a3fd25c1023a6), [`e9e8e5e`](https://github.com/TanStack/db/commit/e9e8e5e20c23fb7f98865d6b8aab05ad5322e5f7)]:
  - @tanstack/db@0.0.28

## 0.0.9

### Patch Changes

- Updated dependencies [[`bec8620`](https://github.com/TanStack/db/commit/bec862004deef5fdd560f70107ebd59f7c27656e)]:
  - @tanstack/db@0.0.27

## 0.0.8

### Patch Changes

- Add initial release of TrailBase collection for TanStack DB. TrailBase is a blazingly fast, open-source alternative to Firebase built on Rust, SQLite, and V8. It provides type-safe REST and realtime APIs with sub-millisecond latencies, integrated authentication, and flexible access control - all in a single executable. This collection type enables seamless integration with TrailBase backends for high-performance real-time applications. ([#228](https://github.com/TanStack/db/pull/228))

- Updated dependencies [[`09c6995`](https://github.com/TanStack/db/commit/09c6995ea9c8e6979d077ca63cbdd6215054ae78)]:
  - @tanstack/db@0.0.26

## 0.0.7

### Patch Changes

- Add explicit collection readiness detection with `isReady()` and `markReady()` ([#270](https://github.com/TanStack/db/pull/270))
  - Add `isReady()` method to check if a collection is ready for use
  - Add `onFirstReady()` method to register callbacks for when collection becomes ready
  - Add `markReady()` to SyncConfig interface for sync implementations to explicitly signal readiness
  - Replace `onFirstCommit()` with `onFirstReady()` for better semantics
  - Update status state machine to allow `loading` → `ready` transition for cases with no data to commit
  - Update all sync implementations (Electric, Query, Local-only, Local-storage) to use `markReady()`
  - Improve error handling by allowing collections to be marked ready even when sync errors occur

  This provides a more intuitive and ergonomic API for determining collection readiness, replacing the previous approach of using commits as a readiness signal.

- Updated dependencies [[`1758eda`](https://github.com/TanStack/db/commit/1758edab9608383d9d1470156021ee632f043e51), [`20f810e`](https://github.com/TanStack/db/commit/20f810e13a7d802bf56da6f0df89b34312ebb2fd)]:
  - @tanstack/db@0.0.25

## 0.0.6

### Patch Changes

- Updated dependencies [[`11215d9`](https://github.com/TanStack/db/commit/11215d9544d02e9dc6258c661ba4b5e439e479ed), [`fe42591`](https://github.com/TanStack/db/commit/fe42591bd7ea9955d67ecec4471b44cb7808e74b), [`665efe6`](https://github.com/TanStack/db/commit/665efe660c1aed68139326a2a33904968622a882)]:
  - @tanstack/db@0.0.24

## 0.0.5

### Patch Changes

- Updated dependencies [[`056609e`](https://github.com/TanStack/db/commit/056609ed2926e12df5ee08be5fad0a6333e787f3)]:
  - @tanstack/db@0.0.23

## 0.0.4

### Patch Changes

- Updated dependencies [[`aeee9a1`](https://github.com/TanStack/db/commit/aeee9a13411527bd0ebfc0a0c06989bdb904b650)]:
  - @tanstack/db@0.0.22

## 0.0.3

### Patch Changes

- Move Collections to their own packages ([#252](https://github.com/TanStack/db/pull/252))
  - Move local-only and local-storage collections to main `@tanstack/db` package
  - Create new `@tanstack/electric-db-collection` package for ElectricSQL integration
  - Create new `@tanstack/query-db-collection` package for TanStack Query integration
  - Delete `@tanstack/db-collections` package (removed from repo)
  - Update example app and documentation to use new package structure

  Why?
  - Better separation of concerns
  - Independent versioning for each collection type
  - Cleaner dependencies (electric collections don't need query deps, etc.)
  - Easier to add more collection types moving forward

- Updated dependencies [[`8e23322`](https://github.com/TanStack/db/commit/8e233229b25eabed07cdaf12948ba913786bf4f9)]:
  - @tanstack/db@0.0.21
