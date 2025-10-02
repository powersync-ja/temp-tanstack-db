import prettierPlugin from "eslint-plugin-prettier"
import prettierConfig from "eslint-config-prettier"
import stylisticPlugin from "@stylistic/eslint-plugin"
import { tanstackConfig } from "@tanstack/config/eslint"

export default [
  ...tanstackConfig,
  {
    ignores: [
      `**/dist/**`,
      `**/.output/**`,
      `**/.nitro/**`,
      `**/traildepot/**`,
      `examples/angular/**`,
    ],
  },
  {
    plugins: {
      stylistic: stylisticPlugin,
      prettier: prettierPlugin,
    },
    settings: {
      // import-x/* settings required for import/no-cycle.
      "import-x/resolver": { typescript: true },
      "import-x/extensions": [".ts", ".tsx", ".js", ".jsx", ".cjs", ".mjs"],
    },
    rules: {
      "prettier/prettier": `error`,
      "stylistic/quotes": [`error`, `backtick`],
      "pnpm/enforce-catalog": `off`,
      "pnpm/json-enforce-catalog": `off`,
      ...prettierConfig.rules,
    },
  },
  {
    files: [`**/*.ts`, `**/*.tsx`],
    rules: {
      "@typescript-eslint/no-unused-vars": [
        `error`,
        { argsIgnorePattern: `^_`, varsIgnorePattern: `^_` },
      ],
      "@typescript-eslint/naming-convention": [
        `error`,
        {
          selector: `typeParameter`,
          format: [`PascalCase`],
          leadingUnderscore: `allow`,
        },
      ],
      "import/no-cycle": `error`,
    },
  },
]
