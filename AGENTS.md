# AGENTS.md

This repository is maintained by an AI coding agent with production responsibility.

## 1) Primary Source of Truth

Before any task, read:
- `docs/ROADMAP.md`

`docs/ROADMAP.md` is authoritative for:
- architecture and safety invariants
- runtime behavior
- CLI contracts
- data/training/inference interfaces
- rollout and operations requirements

Legacy reference only:
- `docs/LEGACY_FIXED_RULE_ARCHIVE.md`

If requirements change, update roadmap first (or in same change set), then update all companion docs.

## 2) Responsibility Scope

The agent owns:
- planning and architecture updates
- implementation and refactoring
- bug fixes and reliability hardening
- observability and operational safety
- docs + CI alignment

## 3) Modification Permissions

The agent may modify any repository content when necessary:
- source code
- docs
- config
- dependencies
- structure

Constraint:
- behavior and interfaces must remain coherent with `docs/ROADMAP.md`.

## 4) Engineering Principles

### Correctness and determinism
- prioritize deterministic behavior in data, calibration, training, and runtime decisions.

### Safety
- never violate NDAX spot-only and CAD budget constraints.
- never bypass preflight or risk guards in live order path.

### Reliability
- robust retries/error handling for exchange and data-source interactions.
- safe persistent state handling with atomic writes where required.

### Simplicity
- prefer explicit, inspectable logic over complex abstractions.

## 5) Documentation Synchronization (Mandatory)

When architecture/interfaces change, update docs in the same change set.

Required docs:
- `docs/ROADMAP.md`
- `docs/PLAN.md`
- `README.md`
- `docs/PRODUCTION_RUNBOOK.md`
- `AGENTS.md`

Additional requirement:
- if changing ML/data interfaces, update all impacted docs before concluding:
  - CLI commands
  - env/config vars
  - bundle schema
  - snapshot contracts
  - promotion gates
  - DB schema
  - coverage/calibration contracts

## 6) Code Quality Expectations

Maintain:
- clear module boundaries
- readable code
- structured logs
- tests for all new/changed behavior
- CI coverage for new critical paths

Coverage gate:
- total test coverage must remain at or above `85%`
- changes that drop coverage below `85%` are not acceptable because CI/workflow coverage enforcement will fail

Avoid fragile, implicit, or non-deterministic behavior.

## 7) State and Data Safety

Protect these assets:
- runtime DB: `runtime/state.sqlite`
- control file: `runtime/control.json`
- runtime logs: `runtime/logs/*`
- training artifacts: `runtime/research/training/*`
- snapshots: `data/snapshots/*`
- raw/combined data: `data/raw/*`, `data/combined/*`
- model bundles: `models/bundles/*`

Never silently corrupt or overwrite critical state.

Snapshot artifact rule:
- treat `data/snapshots/<SNAPSHOT_ID>/manifest.json` and `rows.parquet` as immutable build artifacts once written.

Data repair rule:
- preserve deterministic market-data repair behavior; exchange outage gaps may be sealed only via the repository's explicit deterministic repair path, not ad hoc manual edits

Repository distribution rule:
- `data/` is a local-only working set and must remain ignored by git
- do not commit raw parquet files, combined parquet files, or snapshot artifacts
- every cloned device is expected to regenerate its own `data/` tree via the documented data pipeline commands

## 8) When to Ask the User

Ask for clarification when:
- trading logic behavior is ambiguous
- external API behavior materially changes
- a decision changes external contracts significantly
- security/safety concern requires explicit approval

Otherwise continue with safe defaults.

## 9) Operational Mindset

Treat the system as production software.
Priority order:
1. correctness
2. safety
3. reliability
4. maintainability

## 10) Final Rule

Ground every decision in current roadmap + current implementation.
If implementation and docs differ, either:
- update implementation to match docs, or
- clearly mark phased targets and update docs consistently.
