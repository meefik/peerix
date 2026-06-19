# AGENTS.md

## Tech Stack
- TypeScript 5+
- Node.js 24+
- Vite
- TypeDoc
- Playwright
- CRITICAL: Zero runtime dependencies architecture.

## Coding Conventions
- NEVER introduce new runtime packages to `dependencies` in `package.json`. Only `devDependencies` are permitted.
- Prefer `async/await` over raw `.then()` promises.
- Use explicit error handling. Throw standard `Error` classes with descriptive, context-rich messages.
- Adhere strictly to the Single Responsibility Principle to keep files modular and concise.
- Always declare explicit return types for all methods, accessors, and functions, including arrow functions.
- Use consistent naming conventions: `camelCase` for variables and functions, `PascalCase` for classes and interfaces, and `UPPER_SNAKE_CASE` for constants.
- Write TypeDoc block comments (`/** ... */`) for all exported functions, classes, and interfaces. You MUST include `@param` and `@returns` tags where applicable for public methods and functions.
- Do not spend tokens formatting the code for style; leave style enforcement to Prettier/ESLint.
- Use named exports exclusively. Do not use `export default`.
- Fix grammar mistakes in comments and documentation to maintain professionalism and clarity.
- Use Conventional Commits format for all commit messages. Allowed types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`. Format: `<type>[optional scope]: <description>`.

## Commands
- Install dependencies: `npm install`
- Run unit tests: `npm run test:unit`
- Run end-to-end tests: `npm run test:e2e`
- Run both unit and end-to-end tests: `npm test`
- Run the development server: `npm run dev`
- Build the app: `npm run build`
- Generate TypeDoc documentation: `npm run docs`

## Testing Requirements
- Co-locate unit tests using the `[filename].test.ts` naming convention.
- Use the native `node:test` runner and `node:assert` for unit testing. DO NOT import Jest, Vitest, or Chai.
- Always use the `node:` prefix for built-in Node.js modules.
- Use the built-in `mock` module from `node:test` (e.g., `mock.method()`) for mocking external APIs and heavy I/O operations.
- Use Playwright strictly for end-to-end testing, located in a separate `/tests` directory. Assume the local application runs on `http://localhost:3000`.
- Strictly adhere to the Arrange-Act-Assert (AAA) pattern using comments (`// Arrange`, `// Act`, `// Assert`) to separate the blocks.
