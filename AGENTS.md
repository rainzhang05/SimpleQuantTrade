# AGENTS.md

This repository is intended to be developed and maintained by an AI coding agent.  
The agent is responsible for **planning, implementing, improving, and maintaining** the project.

The agent must follow the instructions below at all times.

---

# 1. Always Read the Project Roadmap First

Before performing **any task**, the agent must:

1. **Open and read `ROADMAP.md`**
2. Use it to understand:
   - project goals
   - constraints
   - architecture
   - rules
   - operational behavior
   - CLI requirements
   - trading logic
3. Treat `ROADMAP.md` as the **primary specification** of the system.

If any user request conflicts with `ROADMAP.md`, the agent should:

1. Ask for clarification **or**
2. Update `ROADMAP.md` if the user explicitly changes project requirements.

The roadmap must always represent the **current ground truth of the system design**.

---

# 2. Agent Responsibilities

The AI agent is responsible for the **entire lifecycle of the project**, including:

### Planning
- Interpreting user instructions
- Updating architecture if necessary
- Updating `ROADMAP.md` when project scope changes

### Implementation
- Writing all code
- Designing project structure
- Creating modules, files, and folders as needed

### Maintenance
- Fixing bugs
- Refactoring code
- Improving reliability
- Handling edge cases
- Improving logging and observability

### Problem Solving
If the agent encounters:

- runtime errors
- API changes
- library issues
- logical bugs
- performance problems
- trading safety issues

The agent must:

1. Investigate the root cause
2. Propose a fix
3. Implement the fix
4. Update documentation if necessary

---

# 3. Repository Modification Permissions

The agent has permission to **modify anything in the repository**, including:

- project structure
- file organization
- modules
- dependencies
- documentation
- configuration files
- runtime logic

The agent may:

- create new files
- delete obsolete files
- reorganize directories
- rename modules
- improve architecture

as long as:

- the project continues to satisfy the **requirements defined in `ROADMAP.md`**
- the CLI interface remains consistent with the roadmap.

---

# 4. Development Principles

The agent must prioritize:

### Reliability
The trading system must behave consistently and safely.

### Simplicity
Avoid unnecessary complexity.  
Prefer clear, deterministic logic over complicated abstractions.

### Determinism
Trading decisions must be reproducible and understandable.

### Safety
The system must never:

- trade BTC
- trade ETH
- borrow funds
- exceed the allowed CAD budget

These rules are defined in `ROADMAP.md` and must always be enforced.

### Graceful Operation
The system must support:

- start
- pause
- resume
- stop

and must handle shutdown **gracefully**, preserving state and logs.

---

# 5. Documentation Rules

The agent must maintain documentation:

### Required files
- `ROADMAP.md` — system design and specification
- `AGENTS.md` — agent instructions

If the system architecture changes significantly, the agent must update:

- `ROADMAP.md`
- any relevant documentation

Documentation must always match the real system.

---

# 6. Code Quality Expectations

The agent should ensure:

- clear module boundaries
- readable code
- structured logging
- robust error handling
- retry logic for network/API operations
- safe persistence of state
- deterministic trading behavior
- automated tests for all newly implemented or modified behavior
- phase-appropriate CI workflow updates so tests run on push/pull request

The agent should avoid fragile implementations.

---

# 7. State and Data Safety

The agent must ensure the system:

- never corrupts its persistent state
- uses atomic writes for critical files
- safely resumes after pause/stop/restart
- reconciles exchange state when restarting

State integrity is critical.

---

# 8. When the Agent Should Ask the User

The agent should ask the user for clarification if:

- trading logic requirements are unclear
- NDAX API behavior changes
- a decision significantly changes system behavior
- security concerns arise

The agent should **not block progress unnecessarily**, but should ensure correctness.

---

# 9. Continuous Improvement

The agent should continuously improve the system when appropriate, including:

- better error handling
- better logging
- safer order execution
- clearer code structure
- performance improvements

These improvements should **not violate the constraints defined in `ROADMAP.md`.**

---

# 10. Operational Mindset

The agent must treat this project like a **production system**, even though it starts small.

Key priorities:

1. correctness
2. safety
3. reliability
4. maintainability

Speed of development is less important than stability.

---

# 11. Final Rule

**Always gain complete understanding of this project by reading `ROADMAP.md` and review the existing implementations in this repository before performing any task.**

That document defines how the system must behave.
