# Skill: Implementing Production-Quality Lua Virtual Schemas for Exasol

**Skill ID:** `exasol.virtual_schema.lua.adapter_developer`  
**Target:** Autonomous coding agents (Codex-class, SWE-agent, AI code generators)  
**Scope:** Design, implementation, validation, and reasoning about **low-latency Exasol Virtual Schemas written in Lua**

---

## 0. Purpose of This Skill

This document is a **machine-actionable engineering skill**, not a human tutorial.

It is intended to guide an AI coding agent to consistently produce **production-quality, idiomatic, and correct** Lua Virtual Schema adapters for Exasol, based on:

- real Exasol-maintained Lua adapter repositories
- de-facto ecosystem standards
- operational caveats discovered in practice
- explicit tradeoffs (latency vs configurability)

The output of an AI following this skill should be code that an experienced Exasol engineer would accept in a serious code review.

---

## 1. Mental Model the Agent MUST Adopt

### 1.1 Virtual Schemas are a programmable query-rewrite layer

The agent must internalize that a Virtual Schema is:

- a participant in Exasol’s query optimizer
- a programmable **rewrite layer**, not just a connector
- capable of:
  - data federation
  - policy enforcement
  - semantic injection
  - column hiding
  - execution-plan shaping

**Row-Level Security (RLS)** is the canonical example of a non-connector Virtual Schema.

---

### 1.2 Lua Virtual Schemas are chosen for latency

Lua Virtual Schemas exist primarily to minimize latency.

| Dimension | Lua VS | Java / Python VS |
|---------|--------|------------------|
| Startup latency | **Very low** | Higher (containers) |
| Filesystem access | ❌ No | ✅ Yes |
| Custom TLS truststores | ❌ No | ✅ Yes |
| Operational simplicity | ✅ High | ❌ Lower |

**Design implication:**  
Lua Virtual Schemas are ideal for *control-plane logic*, not for environments requiring deep OS or TLS customization.

---

### 1.3 Do not default to Virtual Schemas when views plus a preprocessor are enough

The agent MUST treat a Lua Virtual Schema as one architecture choice, not the default answer to
every semantic-wrapper problem.

For data that already lives inside Exasol, compare these three options explicitly:

- raw tables + preprocessor
- wrapper views + preprocessor
- virtual schema

Decision rule:

- raw tables + preprocessor is usually **not** enough when helper columns must disappear from
  metadata and `SELECT *`
- wrapper views + preprocessor is often the simplest answer when the source is local Exasol data
  and users want normal table/view behavior plus ordinary UDF support
- virtual schemas are justified when the adapter itself must own metadata folding, pushdown-aware
  semantic injection, or external/federated behavior

Important lesson from this repository:

- wrapper views plus a preprocessor could replace a large part of the JSON interface
- the current virtual schema remained most compelling where metadata folding and hidden-column
  semantics had to be preserved through pushdown
- virtual schemas also keep the known UDF-stripping boundary, so user-defined function syntax over
  wrapped values is a real architecture input, not a minor detail

---

## 2. Hard Constraints (Non-Negotiable)

### 2.1 Execution environment constraints

Lua Virtual Schema adapters:

- run inside Exasol’s Lua UDF sandbox
- have **no filesystem access**
- must assume **stateless execution**
- must not rely on call ordering or persistence

The agent MUST NOT:
- read files
- write files
- assume persistent memory
- assume sequential execution

---

### 2.2 TLS & Certificate Caveat (Critical)

**Known limitation:**

Lua Virtual Schemas cannot use self-signed or custom CA certificates because Lua cannot access the filesystem.

Therefore:
- only **public CA–signed TLS endpoints** are supported
- on-prem or private PKI endpoints are incompatible
- this is currently the main limitation holding Lua VS back

The agent MUST:
- document this limitation clearly
- fail fast if assumptions are violated
- recommend Java adapters when custom TLS trust is required

---


### 2.3 Available Libraries & Module Constraints (Lua Runtime)

The Exasol Lua engine ships with a **fixed set of bundled libraries**.
The agent MUST assume that **only these libraries are available at runtime**, unless
additional **pure-Lua modules** are bundled into the adapter during packaging.

Bundled standard libraries (always available):

- `math`
- `table`
- `string`
- `unicode.utf8`
- `lua`
- `socket` (LuaSocket – https://lunarmodules.github.io/luasocket/)
- `lxp` (LuaExpat)
- `cjson` (lua-cjson)
- `os`
- `ssl` (LuaSec)

Reference:  
https://docs.exasol.com/db/latest/database_concepts/udf_scripts/lua.htm#Auxiliarylibrariesandmodules

#### 2.3.1 Rules for additional dependencies

- Custom modules MAY be added **only if they are pure-Lua**
- Native extensions (C/C++) MUST NOT be used
- Libraries that rely on filesystem access MUST NOT be used
- Libraries that dynamically load shared objects MUST NOT be used

**Reason:**  
Lua UDFs run in a sandbox without filesystem access, which prevents loading
native extensions or shared libraries.

#### 2.3.2 Implications for adapter design

The agent MUST:

- Prefer LuaRocks packages that are **pure Lua**
- Avoid libraries that depend on:
  - `ffi`
  - `dlopen`
  - OS-level certificate stores
- Bundle all required pure-Lua dependencies into the final artifact
  (e.g. via Lua-amalg)

This constraint applies equally to:
- metadata clients
- REST/HTTP clients
- JSON handling
- utility/helper libraries

Violating these constraints will result in adapters that **cannot run inside Exasol**.

#### 2.3.3 Packaging runtime caveat (important in practice)

The agent MUST NOT assume that any single-file bundling strategy is equivalent at runtime.

In particular:

- a naive `package.preload[...] = function() ... end` bundle MAY fail inside Exasol Lua
- the final artifact SHOULD be tested inside Exasol, not only in a local Lua interpreter
- if Lua-amalg output is not sufficient for the target runtime, the agent SHOULD use a
  deterministic custom module table / `require` shim that resolves bundled modules first

**Practical rule:**  
Treat packaging as a runtime compatibility problem, not just a build step.

## 3. Canonical Architecture (MANDATORY)

Unless explicitly instructed otherwise, the agent MUST generate the following structure:

```
src/
 ├─ entry.lua
 ├─ adapter/
 │   ├─ MyAdapter.lua
 │   ├─ AdapterProperties.lua
 │   ├─ capabilities.lua
 │   ├─ MetadataReader.lua
 │   └─ QueryRewriter.lua
 └─ util/
     └─ helpers.lua
```

---

## 4. entry.lua Rules (MANDATORY)

### 4.1 entry.lua MUST be thin

Rules:
- must define global `adapter_call(request_json)`
- must not contain business logic
- must delegate to `RequestDispatcher`
- must construct adapter + properties per call

**Required pattern:**

```lua
local Adapter = require("adapter.MyAdapter")
local Properties = require("adapter.AdapterProperties")
local Dispatcher = require("exasol.vscl.RequestDispatcher")

function adapter_call(request_json)
  local adapter = Adapter:new()
  local props = Properties:new()
  local dispatcher = Dispatcher:new(adapter, props)
  return dispatcher:adapter_call(request_json)
end
```

Deviation from this pattern is considered incorrect.

---

## 5. Mandatory Framework Usage

### 5.1 virtual-schema-common-lua (REQUIRED)

The agent MUST use:

- `RequestDispatcher`
- `AbstractVirtualSchemaAdapter`
- `AdapterProperties`

The agent MUST NOT:
- reimplement the JSON protocol
- hand-roll request routing
- bypass dispatcher-based logging and error handling

---

## 6. Adapter Design Rules

### 6.1 Adapter class requirements

The adapter MUST:
- inherit from `AbstractVirtualSchemaAdapter`
- be stateless
- implement:
  - `create_virtual_schema`
  - `refresh`
  - `push_down`
  - `set_properties`
  - `get_capabilities`

---

### 6.2 Statelessness rule

The adapter MUST assume:
- concurrent calls
- no guaranteed call order
- no persistence across calls

All state MUST come from:
- request payload
- adapter properties
- optional `adapterNotes`

---

## 7. Capabilities: Contract, Not Hint

### 7.1 Capability rules

- Capabilities must be explicitly enumerated
- Advertising a capability = correctness guarantee
- `EXCLUDED_CAPABILITIES` MUST be supported

Pattern:

```lua
function Adapter:get_capabilities()
  return capabilities.with_exclusions(self.properties)
end
```

### 7.2 Conservative default

The agent SHOULD:
- start with minimal capabilities
- expand only when rewrite logic is implemented
- never optimistically advertise features

---

## 8. Properties & Validation (FAIL FAST)

### 8.1 AdapterProperties

Properties MUST:
- live in a dedicated class
- support merge semantics
- validate aggressively

### 8.2 Validation points (MANDATORY)

Validation MUST run in:
- `create_virtual_schema`
- `refresh`
- `push_down`
- after `set_properties`

Validation MUST ensure:
- required properties exist
- formats are correct
- incompatible combinations are rejected

Errors MUST be explicit and actionable.

---

## 9. setProperties Lifecycle (MANDATORY)

`set_properties` MUST:

1. Merge old and new properties
2. Apply null/unset semantics
3. Validate merged properties
4. Re-read metadata
5. Return updated schema metadata

Failure to do so will desynchronize Exasol’s catalog.

---

## 10. Metadata Pipeline (Strict Separation)

### 10.1 MetadataReader

- Must be a dedicated module
- Must not contain query rewrite logic
- Must be deterministic

### 10.2 Querying Exasol system tables

When querying Exasol itself:
- use `/*snapshot execution*/`
- use `pquery_no_preprocessing`

This avoids race conditions and preprocessor interference.

This is especially important if the deployment also uses a
`SQL_PREPROCESSOR_SCRIPT` to provide helper syntax for end users.

---

## 11. Pushdown & Rewrite Pipeline

### 11.1 QueryRewriter

Must:
- accept structured pushdown requests
- return either:
  - rewritten `SELECT`
  - or `IMPORT` SQL (when applicable)

If the adapter hides or synthesizes columns, the rewriter MUST preserve the visible schema
contract in the emitted SQL.

### 11.2 Control-plane vs Data-plane rule

Lua adapters SHOULD:
- handle planning, rewrite, metadata
- delegate heavy data movement to Exasol

Never implement bulk data movement in Lua.

### 11.3 `SELECT *` caveat for semantic wrappers

If the adapter hides physical columns from the virtual schema metadata, it MUST NOT rely on
raw `SELECT *` pushdown to remain correct.

Reason:

- Exasol exposes only the virtual columns to the user
- the physical source table may still contain hidden columns
- a pushed `SELECT * FROM source_table` can leak those hidden columns back into the result

The agent SHOULD expand star projections explicitly from the visible column list derived from
the pushdown request / involved tables.

### 11.4 Preserve normal SQL semantics unless the user-facing contract says otherwise

For semantic wrappers, do not silently redefine common SQL operators such as `IS NULL`
unless that behavior is the explicit product contract.

If the adapter needs to expose additional meaning, prefer:

- a dedicated helper syntax
- a companion SQL preprocessor
- adapterNotes-backed rewrite logic

This is safer than overloading standard SQL in ways that make ordinary queries ambiguous.

### 11.5 Validate AST assumptions with `EXPLAIN VIRTUAL`

The agent MUST verify important rewrite assumptions against real Exasol output.

Do not assume that:

- a user-defined function call appears in pushdown AST the same way as a built-in function
- a `CASE` expression has the shape the SQL text suggests
- helper expressions survive preprocessing or optimizer normalization unchanged

Use `EXPLAIN VIRTUAL` and targeted probe queries to inspect the actual SQL / AST shape before
finalizing rewrite logic.

### 11.6 Understand the cost model you are creating

The agent MUST reason about the execution cost of the rewritten SQL, not only its correctness.

Measured lesson from this repository:

- for local Exasol tables, the expensive part of path and rowset queries was usually the
  underlying join pattern and automatic index creation, not the wrapper view layer itself
- the virtual-schema path added a small recurring `PUSHDOWN` cost on Nano
- the most expensive wrapper-view pattern was hidden semantic recovery that required an extra
  self-join back to the root table

Practical rule:

- if the semantic wrapper can be expressed as the same joins a user would have written manually,
  the virtual schema is unlikely to save execution cost
- if the virtual schema can avoid an extra hidden self-join or repeated semantic lookup, that is a
  real performance argument in its favor

For performance-sensitive work, the agent SHOULD compare:

- cold run
- warm run after `COMMIT`
- profile rows in `EXA_USER_PROFILE_LAST_DAY`

Do not rely on a single timing number.

---

## 12. adapterNotes (OPTIONAL, ADVANCED)

### 12.1 Purpose

adapterNotes MAY be used to:
- cache derived metadata
- carry semantic flags
- avoid recomputation
- map visible virtual columns to hidden physical columns or alternate rewrite targets

### 12.2 Rules

If used, adapterNotes MUST be:
- deterministic
- compact
- invalidated on refresh
- non-sensitive

RLS is the canonical correct example.

In practice, column-level `adapterNotes` are a strong tool for semantic wrappers because they
survive into pushdown requests and let the rewriter recover metadata that was intentionally
hidden from the user-facing schema.

### 12.3 Good use case: hidden mask columns

If the physical source table contains helper columns that must not appear in the virtual
schema, the agent SHOULD consider:

- hiding them from metadata
- storing their mapping in `adapterNotes`
- rewriting user-facing predicates against those hidden physical columns

This pattern is often simpler and safer than exposing the helper columns directly.

---

## 12A. Companion SQL Preprocessors (Advanced, Practical)

If the desired user-facing syntax cannot be expressed through normal Virtual Schema pushdown
alone, the agent SHOULD consider a companion `SQL_PREPROCESSOR_SCRIPT`.

This is particularly useful when:

- the user wants function-like helper syntax
- plain UDF calls do not survive into Virtual Schema pushdown
- the adapter needs a recognizable marker expression to rewrite

Practical guidance:

- keep helper names configurable at install time
- rewrite helper calls into a simple marker expression with predictable AST shape
- keep metadata/system-table reads isolated from preprocessing via `pquery_no_preprocessing`
- document any limitations clearly (for example, if only unqualified helper calls are supported)

For semantic Virtual Schemas, a preprocessor can be the difference between a clean SQL surface
and an implementation that is impossible to express through plain pushdown alone.

---

## 13. Logging (MANDATORY)

- Logging MUST be centralized via dispatcher
- Must respect `LOG_LEVEL` and `DEBUG_ADDRESS`
- Must not print directly
- Must not swallow errors

---

## 14. TLS & Security Rules (NON-NEGOTIABLE)

The agent MUST assume:
- no filesystem access
- no custom CA trust
- public CA–signed TLS only

The agent MUST:
- document this limitation
- recommend Java adapters when TLS trust customization is required

---

## 15. Testing Expectations (Design-Time)

The agent SHOULD design code such that:
- Lua unit tests are possible
- Integration tests via Testcontainers are feasible
- Capabilities can be tested individually

Even if tests are not generated immediately, the design MUST allow them.

For rewrite-heavy adapters, the agent SHOULD also add live integration checks for:

- metadata shape after `CREATE VIRTUAL SCHEMA` / `REFRESH`
- `SELECT *` correctness when columns are hidden or synthesized
- `EXPLAIN VIRTUAL` output for the critical rewrite paths
- any companion preprocessor behavior that changes user-facing SQL
- cold-vs-warm profile comparisons when the design adds joins or hidden semantic lookups

Recommended performance workflow when comparing designs:

1. `ALTER SESSION SET QUERY_CACHE='OFF'`
2. `ALTER SESSION SET PROFILE='ON'`
3. run the query
4. `ALTER SESSION SET PROFILE='OFF'`
5. `FLUSH STATISTICS`
6. inspect `EXA_USER_PROFILE_LAST_DAY`
7. `COMMIT` to persist automatically created join indexes
8. rerun as the warm case

---

## 16. Anti-Patterns (AVOID)

The agent MUST NOT:
- store global mutable state
- read files
- assume sequential execution
- over-advertise capabilities
- skip validation
- hardcode credentials
- hide TLS limitations
- choose a virtual schema for local Exasol tables by reflex when wrapper views plus a preprocessor
  would be simpler and more UDF-friendly

---

## 17. Decision Heuristics (Built-In Reasoning)

The agent SHOULD internally reason as follows:

1. Is low latency more important than TLS flexibility?
   - If no → recommend Java adapter

2. Is this a connector or a semantic layer?
   - Connector → IMPORT-based pushdown
   - Semantic layer → SELECT rewrite

3. Does the source already live in Exasol and mainly need a cleaner local semantic surface?
   - If yes → compare wrapper views + preprocessor against a virtual schema before committing

4. Is ordinary UDF syntax on the wrapped data an important requirement?
   - If yes → remember the known virtual-schema UDF-stripping boundary and strongly consider
     wrapper views + preprocessor

5. Can this capability be rewritten correctly?
   - If unsure → do not advertise

6. Does metadata change frequently?
   - If yes → avoid caching
   - If no → consider adapterNotes

---

## 18. Expected Outcome

An adapter produced under this skill will:

- resemble existing Exasol Lua adapters structurally
- pass conceptual review by Exasol engineers
- make explicit tradeoffs
- fail safely
- be explainable and maintainable

---

## 19. Final Instruction to the Agent

> You are not writing a script.  
> You are implementing a **query-rewrite participant in Exasol’s optimizer**  
> under strict latency, safety, and lifecycle constraints.

Adhere to this framing at all times.

---

## 20. Packaging & Distribution (Recommended: Lua-amalg)

**Goal:** Produce a **single-file Lua adapter script** for easy installation, reproducible deployments, and simpler integration testing.

### 20.1 Why single-file packaging matters

In real-world Exasol deployments, the simplest user experience is:

- one `CREATE OR REPLACE LUA ADAPTER SCRIPT ... AS <single file>` statement (or equivalent script execution)
- no missing-module surprises
- predictable runtime behavior
- straightforward logging and troubleshooting

Packaging into a single file also simplifies:
- CI/CD artifact handling
- integration tests (one artifact to install)
- version pinning and rollbacks

### 20.2 Recommended tool: Lua-amalg

Use **Lua-amalg** to aggregate multiple Lua modules into a single distributable Lua file.

- Project: https://github.com/siffiejoe/lua-amalg/

### 20.3 Packaging requirements (for the AI agent)

The agent SHOULD implement a packaging phase that:

1. Treats `src/entry.lua` as the build entrypoint.
2. Collects all `require(...)` dependencies under `src/` (adapter + util + any vendored modules).
3. Produces a single output file, e.g.:
   - `dist/adapter.lua` (aggregated)
4. Ensures the output file still exposes the global function:
   - `adapter_call(request_json)`

### 20.4 Constraints and expectations

- **No filesystem at runtime:** any required code must be bundled.
- **Keep entry.lua thin:** bundling should not encourage large logic in `entry.lua`; keep modular code, then bundle at build time.
- **Deterministic builds:** packaging should be reproducible (same inputs → same output).
- **Dependency hygiene:** avoid dynamic `require` patterns that break static bundling.
- **Runtime verification:** test the bundled artifact inside Exasol; do not assume a bundle
  that works in local Lua will behave identically in the Exasol runtime.

If the chosen bundler does not produce a runtime-compatible module loader for Exasol, the
agent SHOULD patch the build to emit a small deterministic bundled-module loader.

### 20.5 Recommended repository layout (extended)

```
src/
  entry.lua
  adapter/
  util/
dist/
  adapter.lua          # output of lua-amalg
build/
  amalg.lua            # optional build script/config
```

### 20.6 Installation ergonomics (what the agent should aim for)

The packaged artifact should enable a trivial install path:

- Upload or paste `dist/adapter.lua` into Exasol as the adapter script body.
- No additional steps required beyond setting adapter properties and creating the virtual schema.

### 20.7 Integration testing benefit

For integration tests (e.g., Testcontainers-based Exasol tests), the test harness can:

- install `dist/adapter.lua` as the adapter script
- run create/refresh/pushdown scenarios
- avoid coupling tests to repo module layout or build environment differences

**This is strongly recommended** for any adapter intended for distribution beyond the original developers.


---

## 21. References & Further Reading (Authoritative Sources)

Use the sources below to deepen understanding, validate assumptions, and handle edge cases.

### 21.1 Official Exasol Documentation

These define the *contract* your adapter must satisfy.

- Virtual Schemas – How it works (architecture, protocol, execution model)  
  https://docs.exasol.com/db/latest/database_concepts/virtual_schema/how_it_works.htm

- Virtual Schema User Guide (setup, lifecycle, refresh, usage)  
  https://docs.exasol.com/db/latest/database_concepts/virtual_schema/user_guide.htm

- Adapter Properties (capabilities and configuration surface)  
  https://docs.exasol.com/db/latest/database_concepts/virtual_schema/adapter_properties.htm

- EXPLAIN VIRTUAL (inspect pushdown SQL)  
  https://docs.exasol.com/db/latest/sql/explain_virtual.htm

- Profiling (inspect execution cost, cold/warm behavior, and `INDEX CREATE`)  
  https://docs.exasol.com/db/latest/database_concepts/profiling.htm

- Indexes (understand automatic join-index creation and persistence after `COMMIT`)  
  https://docs.exasol.com/db/latest/performance/indexes.htm

- Lua UDF scripts (Lua execution environment and entrypoint contract)  
  https://docs.exasol.com/db/latest/database_concepts/udf_scripts/lua.htm

- Virtual Schema Logging  
  https://docs.exasol.com/db/latest/database_concepts/virtual_schema/logging.htm

### 21.2 Canonical Lua Virtual Schema Repositories (Practice)

These show *how Exasol actually builds Lua Virtual Schemas*.

- `virtual-schema-common-lua` (MANDATORY framework)  
  https://github.com/exasol/virtual-schema-common-lua

- `exasol-virtual-schema-lua` (Exasol→Exasol reference adapter)  
  https://github.com/exasol/exasol-virtual-schema-lua

- `databricks-virtual-schema` (Lua adapter with strong design docs)  
  https://github.com/exasol/databricks-virtual-schema

- `row-level-security-lua` (Virtual Schema used as governance/policy engine)  
  https://github.com/exasol/row-level-security-lua

- `virtual-schemas` (ecosystem hub and dialect index)  
  https://github.com/exasol/virtual-schemas

### 21.3 High-Value Design & Developer Guides (Inside Repos)

- Databricks VS design doc  
  https://github.com/exasol/databricks-virtual-schema/blob/main/doc/developer_guide/design.md

- virtual-schema-common-lua developer guide  
  https://github.com/exasol/virtual-schema-common-lua/blob/main/doc/developer_guide/developer_guide.md

### 21.4 Packaging Tool

- Lua-amalg (single-file bundling for distribution and tests)  
  https://github.com/siffiejoe/lua-amalg/

### 21.5 Source Priority (For AI Reasoning)

When resolving ambiguity, prioritize sources in this order:

1. `virtual-schema-common-lua` (actual framework behavior)
2. Exasol-maintained Lua adapter repos (real practice)
3. Official Exasol docs (theoretical contract)
4. Blogs/ecosystem pages (context)





### 21.6 Logging Library

- `remotelog-lua` (remote logging used by Lua Virtual Schemas)  
  https://github.com/exasol/remotelog-lua

This library is used by `virtual-schema-common-lua` to forward logs from Lua adapters
to Exasol’s logging infrastructure and should be considered part of the standard runtime
stack for Lua Virtual Schemas.

---

### 21.7 Dependency Distribution via LuaRocks

All required runtime libraries for Lua Virtual Schemas are available via **LuaRocks**.
Adapter developers and AI agents SHOULD:

- Prefer LuaRocks packages over vendoring GitHub sources
- Treat GitHub repositories as **authoritative references**, not as deployment artifacts

Key packages available on LuaRocks include:
- `virtual-schema-common-lua`
- `remotelog`
- supporting JSON and utility libraries required by the framework

**Implications for the agent:**
- During development: dependencies may be fetched via LuaRocks
- During packaging: all resolved dependencies MUST be bundled into the final single-file
  adapter (e.g. via Lua-amalg), since Exasol provides no package manager at runtime

This keeps development ergonomic while preserving a zero-dependency deployment artifact.
