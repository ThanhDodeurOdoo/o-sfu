import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import prettier from "eslint-plugin-prettier/recommended";
import { defineConfig } from "eslint/config";

export default defineConfig([
    js.configs.recommended,
    ...tseslint.configs.recommended,
    prettier,
    {
        languageOptions: {
            globals: {
                ...globals.browser,
            },
        },
        // Rules based on what is found in other odoo JS/TS codebases
        rules: {
            "prettier/prettier": [
                "error",
                {
                    tabWidth: 4,
                    semi: true,
                    singleQuote: false,
                    printWidth: 100,
                    endOfLine: "auto",
                    trailingComma: "none",
                },
            ],
            "node/no-unsupported-features/es-syntax": "off",
            "node/no-missing-import": "off",
            "comma-dangle": "off",
            "no-console": "error",
            "no-undef": "error",
            "no-restricted-globals": ["error", "event", "self"],
            "no-const-assign": ["error"],
            "no-debugger": ["error"],
            "no-dupe-class-members": ["error"],
            "no-dupe-keys": ["error"],
            "no-dupe-args": ["error"],
            "no-dupe-else-if": ["error"],
            "no-unsafe-negation": ["error"],
            "no-duplicate-imports": ["error"],
            "valid-typeof": ["error"],
            "@typescript-eslint/no-unused-vars": [
                "error",
                { vars: "all", args: "none", ignoreRestSiblings: false, caughtErrors: "all" },
            ],
            curly: ["error", "all"],
            "no-restricted-syntax": ["error", "PrivateIdentifier"],
            "prefer-const": [
                "error",
                {
                    destructuring: "all",
                    ignoreReadBeforeAssign: true,
                },
            ],
        },
    },
]);
