# Exasol Lua Virtual Schema Skill

This folder contains the agent-facing skill for implementing production-quality
Exasol Virtual Schema adapters in Lua.

## Files

- `SKILL.md`
  The main skill document. This is the file an agent should load.

## What The Skill Covers

- canonical Lua virtual-schema structure
- required VSCL framework usage
- capability and property contracts
- metadata and pushdown rewrite patterns
- low-latency design rules
- `adapterNotes` usage for semantic wrappers
- packaging and runtime caveats discovered from live Exasol validation
- companion `SQL_PREPROCESSOR_SCRIPT` patterns
- `EXPLAIN VIRTUAL` validation workflow

## Intended Audience

This material is written for coding agents and advanced implementers, not as a
beginner tutorial.
