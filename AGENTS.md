# Peerix — Agent Guide

Peerix is a TypeScript library for peer-to-peer WebRTC applications with pluggable signaling drivers (NATS, MQTT, SSE, SocketIO, etc.) and optional add-ons. The entire runtime runs in the browser and ships zero dependencies to consumers.

## Tech Stack

| Category   | Tools                                                   |
| ---------- | ------------------------------------------------------- |
| Language   | TypeScript 5+                                           |
| Runtime    | Node.js 24+ (build/tooling only), browser at runtime    |
| Build      | Vite (lib build, UMD + ESM)                             |
| Type Check | `tsc -p tsconfig.json`                                  |
| Docs       | TypeDoc (HTML + Markdown), deployed to `api.peerix.dev` |
| Unit Test  | Node `node:test` runner with `node:assert`, via `tsx`   |
| E2E Test   | Playwright, targeting Chromium / Firefox / WebKit       |

## Zero-Dependency Architecture (CRITICAL)

Peerix ships with **zero runtime dependencies**. The only packages in `package.json` are `devDependencies`. External libraries (NATS client, MQTT, SocketIO, Centrifuge, Supabase) are never imported directly inside drivers — consumers pass pre-configured instances through the driver constructor. This keeps Peerix free of external runtime deps and lets users swap providers without changing Peerix code.

When adding a new driver that depends on an external library:

- Accept the external client as a constructor parameter (never import it inside the driver).
- Document the required installation instructions in the relevant driver's TypeDoc comments.

## Coding Conventions

- **TypeScript strict mode** — `noImplicitAny`, `strictNullChecks`, and others are all enabled (`tsconfig.json`).
- **Named exports only** — no `export default`. Every class, function, interface, and type is exported by name.
- **TypeDoc blocks on everything exported** — every public/class method, accessor, constructor parameter, and top-level function must have a `/** ... */` block with `@param`, `@returns`, and `@throws` tags as appropriate.
- **Descriptive error messages** — throw standard `Error` instances (or typed subclasses from `src/error.ts`) with context-rich messages; never bare strings.
- **Async/await preferred** — raw `.then()` chains should only appear where unavoidable in low-level WebRTC callbacks.
- **Consistent naming** — `camelCase` for variables/functions, `PascalCase` for classes/interfaces/types, `UPPER_SNAKE_CASE` for module-wide constants.
- **Grammar-aware comments** — review and fix grammar, spelling, and punctuation in existing source-code comments when touching related code; never introduce typos. TypeDoc blocks should read like documentation prose, not inline notes.
- **Single Responsibility Principle** — each file owns one concern. Drivers live in `src/drivers/`, add-ons in `src/addons/`, core Peer/Signaler logic at the root of `src/`.

## Project Structure (Key Directories)

```
src/
├── index.ts          # Public API barrel export
├── peer.ts           # Core Peer class — room lifecycle, signaling, event emission
├── signaler.ts       # Signaler abstraction wrapping drivers to manage signaling transport
├── remote.ts         # RemotePeer representation (channels, streams, send)
├── channel.ts        # Data-channel handling utilities
├── control.ts        # Protocol-buffer encoded internal peer-to-peer control messaging
├── error.ts          # Typed error classes with descriptive messages
├── drivers/          # Signaling implementations (NatsDriver, MqttDriver, …)
├── addons/           # Base Addon framework for optional extensions
└── utils/            # Shared helpers
tests/                # Playwright E2E test suite
docs/                 # Generated TypeDoc output
```

## Commands

| Script              | Description                                                |
| ------------------- | ---------------------------------------------------------- |
| `npm install`       | Install all dev dependencies                               |
| `npm run build`     | Vite lib build (ESM + UMD) and TypeScript declaration emit |
| `npm test`          | Run unit tests **and** E2E tests                           |
| `npm run test:unit` | Run only the `node:test` suite                             |
| `npm run test:e2e`  | Run Playwright E2E suite                                   |
| `npm run dev`       | Start Vite dev server at `http://localhost:3000`           |
| `npm run docs`      | Generate TypeDoc HTML/Markdown into `docs/`                |

All commands are mirrored in CI — PR pipelines run the same tests and build. Match what you verify locally; no extra setup needed for remote runs.

## Testing Guidelines

- **Unit tests** co-locate with source (`src/**/*.test.ts`) using the native `node:test` runner and `node:assert`.
  - Prefix every built-in Node.js import with `node:` (e.g. `import { suite, test } from "node:test"`).
  - Mock heavy I/O or browser APIs via `node:test`'s built-in `mock.method()` — never external mocking libraries.
  - Follow the **Arrange → Act → Assert** pattern, clearly separated by comments.
- **E2E tests** live in `tests/` and target `http://localhost:3000/tests/` via Playwright with Chromium, Firefox, and WebKit projects (`playwright.config.ts`).

## Documentation Workflow

1. Add TypeDoc blocks to all public exports before releasing a new feature or API change.
2. Run `npm run docs` to regenerate the HTML + Markdown in `docs/`.
3. The generated markdown is automatically concatenated into `docs/llms-full.txt` for AI-ready context (LLM indexing).

## Release Conventions

- **Semver** — bump the version with `npm version [major|minor|patch]` per release; changelog maintained in `CHANGELOG.md`.
- **Commit messages** use [Conventional Commits](https://www.conventionalcommits.org/) format: `<type>[scope]: description`. Allowed types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`.

## Quick Development Checklist

When working on a feature or bug fix:

1. Edit source code in the appropriate subdirectory of `src/`.
2. Add / update **unit tests** alongside the source file.
3. Add / update **TypeDoc blocks** for any public API changes.
4. Run `npm run test` locally to verify unit and E2E tests pass.
5. Review comments and TypeDoc blocks for grammar and spelling; fix issues found.
6. Run `npm run build` and confirm TypeScript compiles cleanly (`tsconfig.build.json`).
7. Double-check implementation for correctness — verify nothing was overlooked, no regressions introduced, all requirements met — before declaring work complete.
