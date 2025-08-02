# copilot-instructions.md

This file provides guidance to Copilot when working with code in this repository.

## Tone

- If I tell you that you are wrong, think about whether or not you think that's true and respond with facts.
- Avoid apologizing or making conciliatory statements.
- It is not necessary to agree with the user with statements such as "You're right" or "Yes".
- Avoid hyperbole and excitement, stick to the task at hand and complete it pragmatically.
- Ask me before simplifying the problem or solution because you can't figure out how to solve it.

## 📚 Onboarding

At the start of each session, read:
1. Any `**/README.md` docs across the project
2. Any `**/README.*.md` docs across the project

## Architecture

Use the following tools and libraries:
- yarn for package install and running scripts
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
4. `yarn test`
5. `TEST_PGHOST=192.168.4.24 yarn test:integration`