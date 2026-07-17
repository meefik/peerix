# Peerix — Agent Guide

TypeScript library for peer-to-peer WebRTC applications with pluggable signaling drivers (NATS, MQTT, SSE, SocketIO, Centrifuge, Supabase, BroadcastChannel, Memory) and optional add-ons. Runs in the browser, ships zero runtime dependencies.

## Stack

- TypeScript 6 (strict), ES2020 target
- Node.js LTS (build/tooling only)
- Vite (UMD + ESM lib build)
- `node:test` + `node:assert` (unit), Playwright (E2E: Chromium/Firefox/WebKit)
- TypeDoc (HTML + Markdown, deployed to `api.peerix.dev`)

## Zero-Dependency Rule (CRITICAL)

`package.json` contains only `devDependencies`. Drivers never import external libraries directly — consumers pass pre-configured client instances via the constructor. Document required installs in the driver's TypeDoc.

## Conventions

- Minimal code, no unnecessary abstractions
- TypeDoc on all exports (`@param`, `@returns`, `@throws`)
- `camelCase` (vars/functions), `PascalCase` (classes/types), `UPPER_SNAKE_CASE` (constants)
- One concern per file. Drivers in `src/drivers/`, add-ons in `src/addons/`, core in `src/` root, helpers in `src/utils/`
- Fix grammar and typos in existing comments when touching nearby code

## Commands

| Script              | Description                                  |
| ------------------- | -------------------------------------------- |
| `npm install`       | Install dev dependencies                     |
| `npm run dev`       | Vite dev server (`localhost:3000`)           |
| `npm run build`     | Vite build + TSC declarations                |
| `npm run typecheck` | Type check                                   |
| `npm run test:unit` | Unit tests (`node:test` via `tsx`)           |
| `npm run test:e2e`  | Playwright E2E suite                         |
| `npm run test`      | Runs unit + E2E tests                        |
| `npm run docs`      | Generate TypeDoc docs + LLM context files    |

CI runs Playwright tests on tag pushes.

## Testing

- **Unit tests** co-locate with source (`src/**/*.test.ts`). Prefix Node built-ins with `node:`. Use `mock.method()` for mocking — no external mock libraries. Follow Arrange → Act → Assert.
- **E2E tests** live in `tests/`, target `localhost:3000/tests/` via Playwright.

## Docs

1. Add TypeDoc to public exports before releasing changes.
2. `npm run docs` generates HTML/Markdown into `docs/` and LLM context files (`llms.txt`, `llms-full.txt`).

## Releases

- Semver via `npm version [major|minor|patch]`. Changelog in `CHANGELOG.md`.
- Conventional Commits: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`.

## Dev Checklist

1. Edit code in the appropriate `src/` location.
2. Add/update unit tests alongside source.
3. Run `npm run typecheck`, `npm run test`, and `npm run build` — confirm clean output.
4. Add/update TypeDoc for public API changes.
5. Review comments and TypeDoc for grammar/spelling.
6. Verify nothing was overlooked before declaring done.
