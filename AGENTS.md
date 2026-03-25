# Repository Guidelines

## Project Structure & Module Organization

This repository is a Rust workspace for a low-latency Solana execution bot. Workspace members live under `crates/`:

- `crates/bot`: runtime orchestration, bootstrap, and top-level config
- `crates/detection`: ShredStream ingestion and normalized market events
- `crates/state`: in-memory account state, decoders, dependency graph, warmup
- `crates/strategy`: route registry, quoting, guards, and opportunity selection
- `crates/builder`, `crates/signing`, `crates/submit`: transaction build/sign/submit pipeline
- `crates/reconciliation`, `crates/telemetry`: execution tracking and observability

Architecture notes belong in `docs/architecture/`. Tests are currently colocated with code via `#[cfg(test)]`.

## Build, Test, and Development Commands

- `cargo check`: fast workspace compile check
- `cargo build`: build all crates
- `cargo test`: run the full test suite
- `cargo test -p state`: run tests for a single crate
- `cargo fmt --all`: format the workspace

Run commands from the repository root. Prefer crate-scoped commands while iterating on one plane.

## Coding Style & Naming Conventions

Use standard Rust style with `rustfmt`; indentation is 4 spaces. Follow existing naming:

- `snake_case` for files, modules, functions, and test names
- `UpperCamelCase` for structs/enums/traits
- explicit enums for statuses and rejection reasons, not ad hoc strings

Keep responsibilities separated by plane. Hot-path logic should stay short, deterministic, and free of synchronous RPC calls. New venue-specific decoding belongs in `state`; routing logic belongs in `strategy`; submission channels belong in `submit`.

## Testing Guidelines

Add unit tests near the code they validate. Cover contract behavior, not just happy paths: stale updates, warmup gating, route selection, build outputs, submit results, and reconciliation transitions. Name tests descriptively, for example `rejects_stale_updates` or `minimal_pipeline_wiring_reaches_submit`.

## Commit & Pull Request Guidelines

This repository has no established commit history yet. Use concise, imperative commit messages with a scope prefix when helpful, for example:

- `feat(state): add pool snapshot invalidation`
- `fix(strategy): reject non-warm routes`

PRs should include a short summary, affected crates, test commands run, and any hot-path or config implications. Link related issues when available.

## Security & Configuration Tips

Do not commit secrets, private keys, auth tokens, or live Jito/ShredStream credentials. Keep environment-specific endpoints and sensitive config outside the repository.
