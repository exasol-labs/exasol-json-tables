# Exasol Lua Virtual Schema Skill

This folder contains the agent-facing skill for implementing production-quality
Exasol Virtual Schema adapters in Lua.

## Files

- `SKILL.md`
  The main skill document. This is the file an agent should load.

## What The Skill Covers

- canonical Lua virtual-schema structure
- required VSCL framework usage
- architecture choice between virtual schemas and wrapper-view plus preprocessor designs
- capability and property contracts
- metadata and pushdown rewrite patterns
- low-latency design rules
- `adapterNotes` usage for semantic wrappers
- packaging and runtime caveats discovered from live Exasol validation
- companion `SQL_PREPROCESSOR_SCRIPT` patterns
- `EXPLAIN VIRTUAL` validation workflow
- cold-vs-warm performance profiling guidance for rewrite-heavy semantic wrappers

## Intended Audience

This material is written for coding agents and advanced implementers, not as a
beginner tutorial.
