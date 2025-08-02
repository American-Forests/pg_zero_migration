# copilot-instructions.md

This file provides guidance to Copilot when working with code in this repository.

## 📚 Onboarding

At the start of each session, read:
1. Any `**/README.md` docs across the project
2. Any `**/README.*.md` docs across the project

## Architecture

Use the following tools and libraries:
- yarn for package install
- vitest for testing

Generate code that is compatible with:
- eslint rules in eslintrc.json
- prettier rules in .prettierrc.json.
- the node engines and typescript version in package.json.

Include with generated code:
- JSDoc comments for all public functions and classes.
- Code comments for complex logic.

## ✅ Quality Gates

When writing code, Copilot must not finish until all of these succeed:

1. `yarn typecheck`
2. `yarn format`
3. `yarn lint`
4. All unit tests (`yarn test`) pass
5. All integration tests (`TEST_PGHOST=192.168.4.24 yarn test:integration`) pass