// ESLint v9 flat config — installed wave11-e.
//
// TypeScript-eslint recommended on top of eslint:recommended.
// Style rules relaxed to warnings so baseline passes clean.
// The vitest-ban rule is an error — this repo uses Jest.
//
// Note: some test files carry eslint-disable comments for rules that have been
// renamed in typescript-eslint v6+ (no-throw-literal → only-throw-error) or
// require plugins not loaded here (import/no-relative-packages). Those
// directives are reported as warnings via reportUnusedDisableDirectives rather
// than errors, so the baseline exits 0.

import js from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  {
    // Top-level linterOptions apply to all files.
    linterOptions: {
      reportUnusedDisableDirectives: 'warn',
    },
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['src/**/*.ts', 'test/**/*.ts'],
    rules: {
      // TypeScript compiler covers this; avoid false-positives on TS globals.
      'no-undef': 'off',
      // Relax style rules to warnings so pre-existing code doesn't block CI.
      '@typescript-eslint/no-explicit-any': 'warn',
      '@typescript-eslint/no-unused-vars': [
        'warn',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
          destructuredArrayIgnorePattern: '^_',
          caughtErrorsIgnorePattern: '^_',
        },
      ],
      '@typescript-eslint/no-require-imports': 'warn',
      '@typescript-eslint/no-unsafe-function-type': 'warn',
      '@typescript-eslint/no-empty-object-type': 'warn',
      '@typescript-eslint/no-unused-expressions': 'warn',
      // Pre-existing patterns — relax to warn for baseline.
      'no-case-declarations': 'warn',
      'prefer-const': 'warn',
      // Ban vitest — this repo uses Jest.
      'no-restricted-imports': [
        'error',
        {
          paths: [
            {
              name: 'vitest',
              message: "This repo uses Jest. Don't import from vitest.",
            },
          ],
        },
      ],
    },
  },
);
