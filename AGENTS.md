# AGENTS.md

This repository is developed and maintained by an AI coding agent.
The agent is responsible for planning, implementation, reliability hardening, and documentation fidelity.

## 1) Primary Source of Truth

Before any task, the agent must read:
- `docs/ROADMAP.md`

`docs/ROADMAP.md` is the canonical specification and defines:
- architecture
- safety constraints
- runtime behavior
- CLI interfaces
- data/training/inference contracts
- rollout and operations expectations

Legacy reference:
- `docs/LEGACY_FIXED_RULE_ARCHIVE.md` is historical context only and is not the active target architecture.

If user instructions conflict with the roadmap:
1. ask for clarification, or
2. update roadmap and companion docs when requirements are explicitly changed.

## 2) Scope of Responsibility

The agent owns end-to-end lifecycle quality, including:
- planning and architecture updates
- implementation and refactoring
- bug fixing and reliability improvements
- observability and operational safety
- documentation and CI alignment

## 3) Repository Modification Permissions

The agent may modify any repository content when needed, including:
- source code
- docs
- config
- dependencies
- project structure

Constraints:
- changes must remain consistent with `docs/ROADMAP.md`
- CLI behavior and safety guarantees must remain coherent with roadmap contracts

## 4) Engineering Principles

### Reliability
- preserve deterministic behavior
- use robust error handling and retries for NDAX interactions

### Simplicity
- prefer clear deterministic logic over unnecessary abstractions

### Safety
- never violate spot-only and CAD budget constraints
- never bypass critical preflight or risk guards in live order path

### Graceful operation
- preserve start/pause/resume/stop behavior
- ensure clean shutdown with persisted state and logs

## 5) Documentation Synchronization Rules

When architecture or interfaces change, the same change set must update documentation.

Required docs:
- `docs/ROADMAP.md`
- `docs/PLAN.md`
- `README.md`
- `docs/PRODUCTION_RUNBOOK.md`
- `AGENTS.md`

Additional rule:
- if changing or adding ML interfaces (commands, config vars, bundle schema, snapshot contracts, promotion gates, DB tables), update roadmap and all affected docs before concluding task.

## 6) Code Quality Expectations

The agent should maintain:
- clear module boundaries
- readable code
- structured logs
- deterministic training and inference paths
- safe persistent-state handling with atomic writes where required
- tests for all new/changed behavior
- CI updates so new tests run on push/pull request

Avoid fragile or implicit behavior.

## 7) State and Data Safety

The system must:
- preserve runtime-state integrity
- avoid silent corruption
- reconcile against NDAX state on restart
- maintain deterministic snapshot and model-bundle contracts

Critical files and stores:
- runtime DB (`runtime/state.sqlite`)
- control state (`runtime/control.json`)
- logs (`runtime/logs/*`)
- data snapshots (`data/snapshots/*`)
- model bundles (`models/bundles/*`)

## 8) When to Ask the User

Ask for clarification if:
- trading logic requirements are ambiguous
- NDAX API behavior changes materially
- a decision changes external behavior/contracts significantly
- security or safety concerns require explicit approval

Do not block progress when a safe and documented default exists.

## 9) Continuous Improvement

The agent is expected to continuously improve:
- error handling
- logging
- safety checks
- code structure
- performance where it does not compromise determinism or safety

## 10) Operational Mindset

Treat this as a production system.
Priority order:
1. correctness
2. safety
3. reliability
4. maintainability

## 11) Final Rule

Always ground decisions in `docs/ROADMAP.md` and current repository implementation.
Documentation must match implemented behavior or clearly mark phased targets.
