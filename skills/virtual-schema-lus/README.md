# Exasol Lua Virtual Schema Adapter – AI Implementation Skill

This repository contains a **machine-oriented skill specification** designed to guide
AI coding agents (e.g. Codex, SWE-agent, autonomous refactoring agents) to implement
**production-quality Exasol Virtual Schemas in Lua**.

The goal is not to teach Lua or SQL, but to encode **real-world engineering practice**
as a deterministic, repeatable skill that an AI can follow without guesswork.

---

## What This Is

- A **prescriptive skill document** for AI agents
- Grounded in **actual Exasol-maintained Lua Virtual Schema implementations**
- Optimized for:
  - low latency
  - correctness under the Exasol optimizer
  - maintainability
  - safe deployment

The skill captures not just *what is possible*, but *what is acceptable in production*.

---

## What This Is Not

- ❌ A beginner tutorial
- ❌ A generic Virtual Schema overview
- ❌ A human-oriented “how to” guide

Humans may read it, but it is explicitly written **for AI consumption**.

---

## Contents

- Canonical Lua Virtual Schema architecture
- Mandatory framework usage (`virtual-schema-common-lua`)
- Adapter lifecycle rules (`create`, `refresh`, `pushdown`, `setProperties`)
- Capability contracts and exclusion mechanics
- Property validation rules
- Low-latency design patterns
- Advanced patterns (policy engines, adapterNotes, hidden-column wrappers)
- Known limitations (TLS / certificates)
- Packaging & distribution via **Lua-amalg**
- Packaging/runtime compatibility caveats from live Exasol testing
- Companion `SQL_PREPROCESSOR_SCRIPT` patterns for helper syntax
- `EXPLAIN VIRTUAL`-driven AST validation guidance
- Dependency management via **LuaRocks**
- Authoritative reference sources

---

## Key Design Principles Encoded in the Skill

### 1. Lua Virtual Schemas are a control-plane tool
Lua is used to **plan, rewrite, and govern queries**, not to move data.

### 2. Statelessness is mandatory
Every adapter must behave correctly under concurrency and reentrancy.

### 3. Capabilities are a contract
If an adapter advertises a capability, it must be correct under all cases.

### 4. Packaging must be single-file
Adapters should be distributed as a **single Lua file** for:
- trivial installation
- reproducible deployments
- simple integration testing

Single-file packaging still has to be validated in Exasol itself. Runtime-compatible loading
behavior matters more than the specific bundler used.

### 5. Explicit tradeoffs are better than hidden ones
The skill documents known limitations (e.g. TLS truststores) so the AI can
make correct architectural decisions (Lua vs Java).

### 6. Real optimizer behavior beats assumptions
The skill now explicitly pushes the agent to validate rewrite assumptions with
`EXPLAIN VIRTUAL`, especially for helper syntax, `CASE` expressions, and hidden-column
semantic wrappers.

---

## Files

- `exasol_lua_virtual_schema_skill_final.md`  
  The full skill specification to be consumed by an AI agent.

- `README.md`  
  This file.

---

## Typical Usage (AI-Oriented)

1. Provide the skill markdown to the AI as a **system or context document**
2. Ask the AI to implement a new Lua Virtual Schema adapter
3. The AI should:
   - follow the architecture exactly
   - respect all constraints
   - generate modular code
   - package the result via Lua-amalg
   - document limitations and assumptions

---

## When to Use Lua vs Java Virtual Schemas

Use **Lua Virtual Schemas** when:
- low latency is critical
- endpoints use public CA–signed TLS
- logic is primarily metadata, rewrite, or policy-based

Use **Java Virtual Schemas** when:
- custom TLS truststores are required
- filesystem access is mandatory
- heavy native drivers are involved

This distinction is explicitly encoded in the skill.

---

## Status

This skill reflects **current best practice** in the Exasol ecosystem,
including known limitations acknowledged by Exasol engineering teams.

It is intended to evolve as the platform evolves.

---

## License / Usage

This document is intended for internal tooling, experimentation, and
AI-assisted development workflows.

It contains no proprietary code, only architectural and procedural guidance.
