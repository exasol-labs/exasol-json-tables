# Proposal: Make The JSON Query Surface More Expressive Without Leaving SQL

## Executive Summary

The current interface already does three important things well:

- it makes nested object traversal feel natural with dot syntax
- it restores important JSON semantics such as explicit-`null` vs missing
- it makes scalar positional array access ergonomic with `array[index]`, `FIRST`, `LAST`, and `SIZE`

That is enough to make many read queries pleasant. It is not enough to make arrays feel first-class.

The main gap is not scalar access. The main gap is that arrays still do not participate in SQL as *relations*.

The strongest pattern across modern nested-data query systems is:

1. scalar path navigation for the common case
2. explicit quantifiers for existential / universal predicates
3. rowset expansion for arrays
4. higher-order operations only where the host language can represent array values naturally

For this project, the best next step is therefore:

**Do not try to keep stretching scalar path syntax to cover all array use cases.**

**Make arrays first-class row sources inside SQL.**

Once arrays can enter `FROM` / `JOIN` cleanly, the rest of SQL becomes available: `EXISTS`, `COUNT`, `GROUP BY`, `HAVING`, `DISTINCT`, window functions, correlated subqueries, and ordinary projection.

That gets us much closer to the expressiveness of NSPredicate, MQL, and SQL engines with strong ARRAY support, while staying aligned with Exasol’s strengths and with this adapter’s architecture.

One comparison caveat matters throughout this document:

- `NSPredicate` is primarily a filter language, not a relational query language
- the `NSPredicate` snippets below therefore show the closest *predicate fragment*, not a full `SELECT` / `JOIN` / `GROUP BY` equivalent
- when a SQL example relies on row expansion, projection, or aggregation, there may be no exact one-to-one `NSPredicate` form

## What Other Query Languages Teach

### NSPredicate

Relevant patterns from Apple’s predicate system:

- dot-style key-path traversal for nested properties
- aggregate qualifiers such as `ANY`, `ALL`, `NONE`, `SOME`
- direct array affordances: `array[index]`, `array[FIRST]`, `array[LAST]`, `array[SIZE]`
- `SUBQUERY(collection, $x, predicate)` for same-element binding and nested filtering
- collection operators such as `@count`, `@sum`, `@avg`, `@min`, `@max`

What NSPredicate gets very right:

- it treats existential and universal array predicates as first-class
- it distinguishes “any element matches” from “count the matching elements”
- it gives users an explicit iterator variable for same-element semantics

Implication for this project:

- we should add first-class quantified array predicates
- we should support a subquery-like way to bind one array element and use full predicate logic against it

### MongoDB Query Language (MQL)

Relevant patterns from MongoDB:

- dot notation over arrays has implicit existential semantics
- `$elemMatch` expresses “one array element must satisfy all these conditions”
- `$filter` returns a filtered array
- `$map` transforms array elements
- `$size` counts array elements

What MQL gets very right:

- it makes “same element must satisfy multiple predicates” explicit
- it has a clean split between simple dot navigation and explicit array filtering / mapping

What MQL gets less right for this project:

- it is document-centric, not SQL-centric
- array transformations often produce arrays again, which fits MQL better than Exasol SQL

Implication for this project:

- we should absolutely adopt the **same-element binding** lesson from `$elemMatch`
- we should not try to copy MQL’s array-returning operators too literally, because SQL rowsets are a better fit than array-valued expressions in Exasol

### EdgeQL

Relevant patterns from EdgeQL / Gel:

- path-first deep traversal over links, with no manual join noise
- shapes for nested projection, including nested filters inside the projected structure
- a set-based execution model: everything is a set, and arrays are commonly converted into sets with `array_unpack()` because sets are easier to manipulate than arrays
- first-class existence and emptiness operators such as `exists` and `??`
- explicit cardinality assertions such as `assert_exists()` and `assert_single()`
- composable set iteration with `for`

What EdgeQL gets very right:

- it treats deep traversal, filtering, and output shaping as one composable expression model
- it makes absence and multiplicity first-class concepts instead of accidental edge cases
- it strongly encourages users to turn ordered containers into set-like query inputs before filtering and aggregating them
- it provides explicit escape hatches when the query author knows more than the static cardinality inference can prove

What EdgeQL gets less right for this project:

- its core data model is object-relational from the ground up, while this project is adapting nested JSON onto SQL tables
- its native output model is already nested, so its projection shapes do not map directly onto ordinary Exasol result sets
- its “everything is a set” semantics are elegant, but SQL does not have an exact analogue for a scalar expression evaluating to an empty set

Implication for this project:

- arrays should be treated primarily as things to **unpack into rowsets**, not as containers to keep probing with more and more scalar syntax
- we should add explicit **existence and cardinality** helpers, not just null helpers
- we should consider a **missing-only coalescing surface**, inspired by EdgeQL’s `??`, because this project already distinguishes missing from explicit JSON `null`
- longer term, EdgeQL-style **shape projection** could be a useful optional layer for producing nested JSON output, but it should sit on top of SQL rather than replace SQL

### SQL Engines With Strong ARRAY / Nested Support

#### BigQuery

BigQuery combines:

- nested `STRUCT` access with dot notation
- zero-based array subscripting
- safe vs strict-ish access via `SAFE_OFFSET` / `SAFE_ORDINAL`
- repeated fields as arrays

Key lesson:

- direct path access is valuable, but safe access modes matter

#### Trino

Trino is especially instructive because it treats arrays as first-class SQL values and then layers higher-order SQL on top:

- `UNNEST`
- `WITH ORDINALITY`
- lambdas
- `any_match`, `all_match`, `none_match`
- `filter`, `transform`, `reduce`
- `array_first`, `array_last`, `cardinality`

Key lesson:

- once arrays are real SQL values or rowsets, a small number of generic higher-order primitives go very far

#### DuckDB

DuckDB is particularly interesting because it covers several parts of the problem at once:

- `LIST` / `STRUCT` native nested types with dot and bracket access
- `unnest(...)` plus recursive unnesting with `max_depth`
- higher-order list functions with lambdas such as `list_filter`, `list_transform`, and `list_reduce`
- JSON extraction with both path syntax and table functions like `json_each` / `json_tree`
- structure-driven JSON transformation via `json_structure()`, `json_transform()`, and `json_transform_strict()`
- a native tagged `UNION` type with explicit `union_tag()` and `union_extract()`

Key lessons:

- recursive row expansion should eventually support explicit depth control
- higher-order array operations are valuable, but they work best as a second-tier layer after row expansion already exists
- variant values are easier to reason about when they behave like tagged unions, not just “a scalar plus some casts”
- structure-driven typed extraction is a strong model for future bulk projection helpers

#### PostgreSQL

PostgreSQL gives two useful models:

- plain SQL arrays with `unnest`, `array_position`, containment operators, etc.
- SQL/JSON path with `jsonb_path_exists`, `jsonb_path_query`, variables, and `strict` / `lax` modes

Key lesson:

- a path-language escape hatch is useful for advanced cases
- `strict` vs `lax` is a valuable concept whenever paths may or may not match

#### Snowflake

Snowflake’s semi-structured model combines:

- colon / dot / bracket path traversal
- index-based array access
- `LATERAL FLATTEN`
- recursive flattening and path introspection

Key lesson:

- for analytics, the real power comes from turning nested structures into rows

#### Cosmos DB’s SQL-like JSON Query Model

Cosmos DB is especially relevant because it stays close to SQL shape while still being JSON-native:

- dot and bracket notation
- zero-based array access
- `JOIN x IN p.array`
- `EXISTS(SELECT ... FROM x IN p.array WHERE ...)`
- `IN` iteration in `FROM`

Key lesson:

- array iteration can be expressed in a *very SQL-like* way without forcing users into a separate path DSL

## Synthesis: The Missing Primitives And Contracts

Across these systems, the same four primitives keep showing up:

1. **Path navigation**
2. **Positional access**
3. **Quantified predicates**
4. **Rowset expansion**

This project already has (1) and a useful subset of (2).

The biggest missing pieces are (3) and (4).

EdgeQL adds one more important lesson:

5. **Existence and cardinality contracts**

That is not a “primitive” in the same sense as path navigation or rowset expansion, but it is a major part of why EdgeQL queries stay readable and safe even as they become deeply nested.

DuckDB adds another useful contract:

6. **Variant tagging and recursive expansion controls**

That matters here because this repository already simulates variant values across multiple physical columns, and because deep nested traversal becomes much more manageable when recursion is explicit and depth-bounded.

## Design Principles For The Next Stage

### 1. Stay SQL-first

The interface should become *more SQL-shaped*, not less.

That means preferring:

- `JOIN`
- `EXISTS`
- `COUNT`
- `GROUP BY`
- correlated subqueries

over inventing a document language that bypasses SQL.

### 2. Keep object and array semantics distinct

Dot syntax should continue to mean object traversal.

Do **not** overload:

```sql
"items.value"
```

to mean “some array element has value”. That would copy one of MQL’s convenient but ambiguous behaviors.

Instead:

- object traversal stays implicit
- array traversal becomes explicit

### 3. Make same-element binding explicit

This is the most important array feature after simple indexing.

Users need a clean way to say:

> Find rows where there exists one array element for which both `value = 'x'` and `label = 'y'`.

That should not require manual child-table joins.

### 4. Prefer rowsets over array-valued transformations

`$filter`, `transform()`, and similar operators are great in languages with native array values.

Exasol’s real advantage is SQL over rows.

So instead of trying to fully emulate array-returning `FILTER` / `MAP`, the interface should let users:

- expand an array into rows
- use SQL to filter and project those rows
- aggregate if they want to collapse again

### 5. Preserve normal SQL semantics

The current project already learned this lesson with null semantics.

New array helpers should be explicit opt-in features, not silent reinterpretations of existing SQL operators.

### 6. Make existence and multiplicity explicit

One of EdgeQL’s strongest ideas is that query authors should be able to say:

- this path must exist
- this expression may be empty
- this result must have at most one value

That kind of contract is especially valuable in this project, because JSON has three distinct states that ordinary SQL tends to blur:

- value exists and is non-null
- value exists and is explicitly `null`
- value is missing

### 7. Make variant values explicit

DuckDB’s `UNION` type is a strong reminder that heterogeneous values are much easier to work with when the “active alternative” is a first-class concept.

This project already approximates that through:

- `TYPEOF(...)`
- `CAST(...)`
- hidden physical sibling columns

That is directionally correct. The next improvement should be to make the surface feel more intentionally like a tagged union.

## Recommended Direction

## 1. Make Arrays First-Class Row Sources

### Recommended syntax

Introduce SQL-like array iteration in `FROM` / `JOIN`, inspired by Cosmos DB:

```sql
SELECT
  s."id",
  item._index,
  item.value,
  item.label
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items";
```

Closest `NSPredicate` comparison:

Not directly possible. `NSPredicate` can test array contents, but it cannot expand an array into row-like bindings for projection.

The closest idiomatic non-emptiness check is:

```text
items.@count > 0
```

If the point of the comparison is specifically to show the iterator-binding flavor that is closer to `JOIN item IN ...`, the nearest `NSPredicate` shape is:

```text
SUBQUERY(items, $item, TRUEPREDICATE).@count > 0
```

And for scalar arrays:

```sql
SELECT
  s."id",
  tag
FROM JSON_VIEW.SAMPLE s
JOIN VALUE tag IN s."tags";
```

Closest `NSPredicate` comparison:

Again, there is no direct rowset/projection equivalent. The nearest existential filter shape is:

```text
ANY tags == "blue"
```

### Why this is the right center of gravity

This gives arrays a relational surface. Once that exists, users can naturally write:

- `WHERE`
- `EXISTS`
- `COUNT`
- `GROUP BY`
- window functions
- `DISTINCT`
- nested joins into arrays-of-objects

without learning a second query language.

### EdgeQL-inspired interpretation

This is also the closest SQL analogue to EdgeQL’s habit of converting arrays into sets before working with them. Inference from the EdgeQL docs: the key usability win is not “array syntax”, it is “once the values are a set, the rest of the language applies cleanly.”

### Proposed semantics

- `JOIN alias IN row_alias."path.to.array"`
  Expands an array into rows.
- `LEFT JOIN alias IN ...`
  Preserves outer rows when the array is empty or missing.
- `JOIN VALUE alias IN ...`
  For scalar arrays, binds the scalar element directly.
- `alias._index`
  Always available for ordinality / position.
- Arrays of objects expose object fields on `alias`.
- Arrays of scalars expose `_value`, and `JOIN VALUE` is sugar for direct scalar binding.

### Example: same-element binding

```sql
SELECT DISTINCT s."id"
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items"
WHERE item.value = 'second'
  AND item.label = 'B';
```

Closest `NSPredicate`:

```text
SUBQUERY(items, $item, $item.value == "second" AND $item.label == "B").@count > 0
```

This is the clean SQL analogue of:

- NSPredicate `SUBQUERY(...).@count`
- MQL `$elemMatch`
- Trino `any_match(...)`

### Example: nested arrays

```sql
SELECT
  d."doc_id",
  entry.value,
  extra
FROM JSON_VIEW.DEEPDOC d
JOIN entry IN d."chain.next.next.next.next.next.next.next.entries"
LEFT JOIN VALUE extra IN entry."extras";
```

Closest `NSPredicate` comparison:

Not directly possible as written, because `NSPredicate` cannot yield one row per `entry` / `extra` pair. The nearest nested existence test is:

```text
SUBQUERY(chain.next.next.next.next.next.next.next.entries,
         $entry,
         SUBQUERY($entry.extras, $extra, TRUEPREDICATE).@count > 0).@count > 0
```

## 2. Add Quantifier Helpers As Sugar On Top

Once arrays are row sources, users can already write quantified predicates with ordinary SQL:

```sql
WHERE EXISTS (
  SELECT 1
  FROM item IN s."items"
  WHERE item.value = 'second'
    AND item.label = 'B'
)
```

Closest `NSPredicate`:

```text
SUBQUERY(items, $item, $item.value == "second" AND $item.label == "B").@count > 0
```

That may already be enough for many users.

Still, a small set of helper functions would improve readability and mirror what nested-data users expect:

```sql
JSON_EXISTS(s, "items", item, item.value = 'second' AND item.label = 'B')
JSON_ALL(s, "items", item, item.value IS NOT NULL)
JSON_NONE(s, "items", item, item.kind = 'deprecated')
JSON_COUNT(s, "items", item, item.label = 'B')
```

Closest `NSPredicate` forms:

```text
SUBQUERY(items, $item, $item.value == "second" AND $item.label == "B").@count > 0
SUBQUERY(items, $item, NOT ($item.value != NIL)).@count == 0
SUBQUERY(items, $item, $item.kind == "deprecated").@count == 0
SUBQUERY(items, $item, $item.label == "B").@count
```

### Why helper functions are still useful

- they mirror NSPredicate `ANY` / `ALL` / `NONE`
- they mirror Trino `any_match` / `all_match` / `none_match`
- they avoid verbose correlated subqueries for simple cases

### Recommended implementation model

These should be *desugaring helpers*, not a separate execution model:

- `JSON_EXISTS(...)` rewrites to `EXISTS (...)`
- `JSON_ALL(...)` rewrites to `NOT EXISTS (...)` over non-matching elements
- `JSON_NONE(...)` rewrites to `NOT EXISTS (...)`
- `JSON_COUNT(...)` rewrites to a correlated `COUNT(*)`

That keeps the feature explainable and optimizer-friendly.

## 3. Extend Path Syntax In Set Contexts, Not Bare Scalar Contexts

The current scalar syntax should remain simple:

- `arr[0]`
- `arr[FIRST]`
- `arr[LAST]`
- `arr[SIZE]`

For more expressive array work, extend the *path language used in set-producing or predicate contexts*:

- `arr[*]`
- `arr[1:4]`
- `arr[-1]`
- `arr[-3:]`

Closest `NSPredicate` comparison:

- `arr[-1]` maps naturally to `arr[LAST]`
- `arr[*]` has no direct path token; the closest filter form is `ANY arr ...` or `SUBQUERY(arr, $x, ...).@count > 0`
- `arr[1:4]` and `arr[-3:]` have no direct `NSPredicate` equivalent

### Recommendation

Support these only inside:

- `JOIN ... IN ...`
- quantifier helpers
- future path-predicate helpers

Avoid allowing slices or wildcards as ordinary scalar select expressions, because Exasol does not naturally represent “an array of results” as a first-class SQL scalar in the same way engines like Trino or BigQuery do.

### Suggested semantics

- zero-based indexing stays the default
- negative indexes count from the end, as in DuckDB and functions like Trino’s `element_at(array, -1)`
- slices are half-open and zero-based: `[start:end]`
- `[*]` means “iterate all elements”

### Why this matters

It lets users express:

- subsets
- tails
- windows over arrays
- all-elements traversal

without creating more scalar special cases.

## 4. Add Existence And Cardinality Helpers

EdgeQL strongly suggests that existence and multiplicity should be explicit parts of the query surface.

Recommended additions:

```sql
JSON_EXISTS_PATH(s, "profile.nickname")
JSON_ASSERT_EXISTS(s, "profile.nickname")
JSON_ASSERT_SINGLE(s, "items[*].label")
```

### Suggested semantics

- `JSON_EXISTS_PATH(...)`
  Returns true when the path exists, even if the terminal JSON value is explicitly `null`.
- `JSON_ASSERT_EXISTS(...)`
  Returns the value if the path exists; otherwise raises a user-facing error.
- `JSON_ASSERT_SINGLE(...)`
  Returns the value if the path resolves to at most one item; otherwise raises a user-facing error.

### Why this matters

The current interface already has `JSON_IS_EXPLICIT_NULL(...)`, which is important, but it only answers one of the three big JSON-state questions.

Users also need to ask:

- does this path exist at all?
- am I accidentally matching more than one item?
- should this query fail loudly instead of silently returning `NULL`?

### Implementation note

These helpers should be defined in terms of the same rowset / path machinery as the rest of the interface. They should not become a parallel execution model.

## 5. Add Missing-Only Coalescing

One particularly interesting EdgeQL idea is the `??` operator, which coalesces *empty* results rather than SQL `NULL`.

Inference from that design for this project:

Because this repository already distinguishes missing from explicit JSON `null`, it may be useful to add a helper that fills in defaults only for **missing** paths.

Possible surfaces:

```sql
JSON_MISSING_COALESCE("profile.nickname", 'anonymous')
```

or, if the preprocessor surface can support it clearly:

```sql
"profile.nickname" ?? 'anonymous'
```

### Suggested semantics

- missing path => use the fallback
- explicit JSON `null` => keep `NULL`
- concrete value => keep the value

### Why this matters

Ordinary `COALESCE(...)` is still useful, but it collapses explicit `null` and missing into the same outcome. That is exactly the distinction this project has worked hard to preserve.

## 6. Make Variant Values Feel More Like Tagged Unions

DuckDB’s native `UNION` support suggests a cleaner way to think about this repository’s existing variant support.

Recommended direction:

```sql
JSON_TAG("value")
JSON_ASSERT_TYPE("value", 'NUMBER')
JSON_ASSERT_TYPE("value", 'STRING')
```

### Suggested semantics

- `JSON_TAG(expr)`
  Returns the active logical JSON type in a small stable vocabulary such as `NUMBER`, `STRING`, `BOOLEAN`, `OBJECT`, `ARRAY`, `NULL`, `MISSING`.
- `JSON_ASSERT_TYPE(expr, tag)`
  Returns the value if the active tag matches; otherwise raises a clear user-facing error.
- ordinary `CAST(expr AS ...)`
  Continues to be the ergonomic extraction surface for normal query use.

### Why this matters

The current `TYPEOF(...)` + `CAST(...)` surface is already useful. DuckDB’s `UNION` model suggests the missing piece is not a radically different extraction syntax, but a stronger **tag contract** around variants.

That would make variant-heavy queries easier to reason about and easier to debug.

## 7. Add Safe / Strict Access Modes

BigQuery and PostgreSQL both show the value of differentiating:

- safe / lax access for production convenience
- strict access for correctness checks and debugging

The current interface is effectively *lax by default*, which is the right default.

Recommended extension:

- keep default path behavior null-tolerant
- add an explicit strict mode for advanced users

Possible surfaces:

```sql
JSON_STRICT_EXISTS(...)
STRICT("items[5]")
```

or:

```sql
JSON_PATH_EXISTS(..., MODE => 'STRICT')
```

Closest `NSPredicate` comparison:

No direct equivalent. `NSPredicate` does not expose a standard `STRICT` / `LAX` path mode distinction.

The exact syntax matters less than the capability.

## 8. Add Recursive Expansion With Depth Control

DuckDB’s `unnest(..., recursive := true, max_depth := N)` is a strong example of a nested-data feature that is powerful without being mysterious.

Recommended future direction:

```sql
JOIN RECURSIVE node IN s."comments" MAX_DEPTH 3
```

or, if it fits the implementation better:

```sql
JSON_DESCENDANTS(s, "comments", MAX_DEPTH => 3)
```

### Why this matters

- deeply nested arrays and objects are common in real JSON
- unlimited recursive flattening is often too blunt
- explicit depth limits make the query safer and easier to understand

### Recommendation

Treat this as an advanced rowset feature, not as a scalar path extension.

## 9. Add A SQL/JSON-Style Escape Hatch For Advanced Cases

No curated surface will cover everything.

PostgreSQL’s `jsonb_path_exists` / `jsonb_path_query` and SQL/JSON path are useful not because they are the nicest everyday syntax, but because they provide an escape hatch for advanced predicates.

Recommended advanced layer:

```sql
JSON_PATH_EXISTS(s, '$.items[*] ? (@.value == "second" && @.label == "B")')
JSON_PATH_QUERY(s, '$.items[*].value')
```

Closest `NSPredicate` comparison:

There is no direct JSONPath-style analogue. The nearest equivalent for the first line is:

```text
SUBQUERY(items, $item, $item.value == "second" AND $item.label == "B").@count > 0
```

For the second line there is no exact `NSPredicate` equivalent, because `NSPredicate` is not designed to return projected arrays the way `JSON_PATH_QUERY(...)` would.

### Important recommendation

This should be positioned as an **advanced escape hatch**, not the primary user surface.

The primary user surface should remain:

- dot paths
- bracket access
- `JOIN ... IN ...`
- explicit SQL

### Why

Path DSLs are powerful, but they are harder to compose with:

- `GROUP BY`
- ordinary joins
- SQL aggregation
- relational projections

## 10. Consider Higher-Order Array Helpers Later

DuckDB’s `list_filter`, `list_transform`, and `list_reduce` show that higher-order array operations can be concise and expressive once the language already has a good story for nested values.

For this project, they should be a **later** layer, not the next layer.

Possible future surface:

```sql
JSON_ARRAY_FILTER("items", item, item.label = 'B')
JSON_ARRAY_TRANSFORM("items", item, item.value)
JSON_ARRAY_REDUCE("numbers", acc, x, acc + x, 0)
```

### Recommendation

Only consider this after:

- rowset expansion exists
- quantified predicates exist
- the missing/null/type contracts are solid

Otherwise these helpers risk becoming a second, array-only mini language.

## 11. Consider Shape-Style Projection Later

EdgeQL’s shapes are not primarily a filtering feature; they are a projection feature. They let users say both:

- what to traverse
- what output structure to emit

That does not translate directly to Exasol tables, but it still suggests a useful future direction:

- keep the core query interface SQL-first and rowset-oriented
- add an optional projection layer for users who want nested JSON output, built on top of SQL JSON constructors and the rowset primitives above

A rough future shape might look like:

```sql
JSON_PROJECT(
  s,
  {
    id,
    profile: { nickname },
    items: [
      { value, label }
      FILTER .label = 'B'
      ORDER BY .value
      LIMIT 3
    ]
  }
)
```

This is a design inference from EdgeQL’s shape system, not something EdgeQL itself had to bolt onto SQL. It should be treated as a later ergonomic layer, not the next core primitive.

DuckDB’s `json_structure()` / `json_transform()` pair also supports this direction: one function describes structure, another projects into typed nested values. That is a useful model if this repository eventually adds schema-driven bulk projection helpers.

## 12. Add Object/Path Introspection Later

Once array rowset support exists, the next useful introspection features are:

- `JSON_KEYS(row, "object.path")`
- `JSON_TYPEOF(row, "path")`
- recursive flatten / descendant traversal

These are valuable, but they are not the first priority. Array iteration and same-element binding are much more important.

## Concrete Proposal: A Layered Query Surface

## Layer 1: Existing Scalar Navigation

Keep and strengthen:

- `"a.b.c"`
- `"arr[0]"`
- `"arr[FIRST]"`
- `"arr[LAST]"`
- `"arr[SIZE]"`

Optional additions:

- `"arr[-1]"`
- `"arr[-2]"`

## Layer 2: Array Iteration In SQL

Add:

```sql
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items"

FROM JSON_VIEW.SAMPLE s
LEFT JOIN VALUE tag IN s."tags"
```

Closest `NSPredicate` comparison:

No direct equivalent. This layer is precisely the relational capability that `NSPredicate` lacks.

This should become the main way to work with arrays.

## Layer 3: Quantified Convenience

Add:

```sql
JSON_EXISTS(row, "path", alias, predicate)
JSON_ALL(row, "path", alias, predicate)
JSON_NONE(row, "path", alias, predicate)
JSON_COUNT(row, "path", alias, predicate)
```

Closest `NSPredicate` comparison:

```text
SUBQUERY(path, $alias, predicate).@count > 0
SUBQUERY(path, $alias, NOT (predicate)).@count == 0
SUBQUERY(path, $alias, predicate).@count == 0
SUBQUERY(path, $alias, predicate).@count
```

All of these should lower to SQL over Layer 2.

## Layer 4: Existence And Cardinality Contracts

Add:

```sql
JSON_TAG(expr)
JSON_ASSERT_TYPE(expr, 'NUMBER')
JSON_EXISTS_PATH(row, "path")
JSON_ASSERT_EXISTS(row, "path")
JSON_ASSERT_SINGLE(row, "path")
JSON_MISSING_COALESCE(expr, fallback)
```

These should make missing-vs-null-vs-many semantics explicit and make variant values easier to reason about.

## Layer 5: Advanced Path / Escape Hatch

Add later:

```sql
JSON_PATH_EXISTS(...)
JSON_PATH_QUERY(...)
```

Closest `NSPredicate` comparison:

No direct equivalent. The closest building blocks are key paths, `ANY` / `ALL` / `NONE`, and `SUBQUERY(...)`.

with optional `STRICT` / `LAX` mode.

## Layer 6: Recursive And Higher-Order Power Tools

Add later:

```sql
JOIN RECURSIVE node IN row."path" MAX_DEPTH 3
JSON_ARRAY_FILTER(...)
JSON_ARRAY_TRANSFORM(...)
JSON_ARRAY_REDUCE(...)
```

These should build on top of the earlier rowset and contract layers, not bypass them.

## Why This Is Better Than Trying To Add More Scalar Shortcuts

The temptation is to keep adding syntax like:

- `"items[*].value"`
- `"items[?price > 100]"`
- `"items[@label='B'].value"`

Some of that is attractive, but it keeps arrays in *expression space* when SQL is strongest in *relation space*.

The better model is:

- scalar syntax for scalar extraction
- relational syntax for arrays

That mirrors what the best systems do:

- Snowflake: `FLATTEN`
- Trino/PostgreSQL: `UNNEST`
- Cosmos DB: `JOIN x IN p.array`

## Example End State

Here is what “expressive but still SQL” could look like:

```sql
SELECT
  s."id",
  COUNT(*) AS matching_items,
  MAX(item._index) AS last_match_pos
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items"
WHERE item.value = 'second'
  AND item.label = 'B'
GROUP BY s."id";
```

Closest `NSPredicate` comparison:

Not directly possible as a single predicate because `COUNT(*)`, `MAX(...)`, and grouped projection are outside `NSPredicate`’s scope.

The closest filter fragment is:

```text
SUBQUERY(items, $item, $item.value == "second" AND $item.label == "B").@count > 0
```

And for existential logic:

```sql
SELECT
  s."id",
  CASE
    WHEN JSON_IS_EXPLICIT_NULL("note") THEN 'explicit-null'
    WHEN "note" IS NULL THEN 'missing'
    ELSE 'value'
  END AS note_state
FROM JSON_VIEW.SAMPLE s
WHERE EXISTS (
  SELECT 1
  FROM tag IN s."tags"
  WHERE tag = 'blue'
)
  AND EXISTS (
    SELECT 1
    FROM item IN s."items"
    WHERE item.value = 'second'
      AND item.label = 'B'
  );
```

Closest `NSPredicate` comparison for the `WHERE` logic:

```text
(SUBQUERY(tags, $tag, $tag == "blue").@count > 0)
AND
(SUBQUERY(items, $item, $item.value == "second" AND $item.label == "B").@count > 0)
```

There is no standard `NSPredicate` equivalent for this repository’s explicit-`null` versus missing-value distinction. Whether that distinction can be expressed depends on the host object model preserving `NSNull` separately from absent keys.

That is already much closer to the expressive power of nested-data-native systems, but it still feels like SQL.

## Phased Roadmap

### Phase 1: Highest ROI

- `JOIN alias IN row."array_path"`
- `LEFT JOIN`
- scalar `VALUE` variant
- `_index` exposure
- support inside correlated subqueries

This alone unlocks most currently awkward array cases.

### Phase 2: Ergonomic Sugar

- `JSON_EXISTS`
- `JSON_ALL`
- `JSON_NONE`
- `JSON_COUNT`
- negative indexes
- `JSON_TAG`
- `JSON_ASSERT_TYPE`
- `JSON_EXISTS_PATH`
- `JSON_ASSERT_EXISTS`
- `JSON_ASSERT_SINGLE`
- missing-only coalescing

### Phase 3: Range / Wildcard Set Contexts

- `[*]`
- `[start:end]`
- negative slicing

### Phase 4: Advanced Path Escape Hatch

- `JSON_PATH_EXISTS`
- `JSON_PATH_QUERY`
- `STRICT` / `LAX`
- recursive expansion with `MAX_DEPTH`
- recursive flatten / descendant traversal

### Phase 5: Optional Projection Layer And Higher-Order Helpers

- shape-style nested projection helpers
- SQL/JSON constructor integration
- path-local nested filter/order/limit for JSON output shaping
- optional higher-order array helpers

## Implementation Notes For This Repository

This design fits the current architecture well:

- the preprocessor is already the right place to add new user-facing syntax
- the adapter already knows how to traverse object links and array child tables
- array child tables already carry parent linkage and ordinality (`_parent`, `_pos`)
- `SIZE` already uses the parent `<name>|array` length column

### Practical rewrite strategy

`JOIN item IN s."meta.items"` can lower to:

1. the object-join chain needed to reach `meta`
2. the array-child join for `items`
3. an alias surface over the resulting child table

`EXISTS (SELECT 1 FROM item IN s."items" WHERE ...)` can lower to:

- a correlated subquery or injected lateral-like join shape, depending on what Exasol pushdown handles best

### Important constraint

Do **not** reintroduce synthetic root-table path columns for this.

The whole point is to make arrays usable *without* polluting metadata or depending on deep path expansion ahead of time.

## Final Recommendation

If only one major feature gets built next, it should be this:

**Add first-class array iteration in `FROM` / `JOIN`.**

Everything else becomes much simpler once arrays can be treated as rows.

That is the shortest path from the current interface to something that feels much closer to nested-data-native query systems, while still taking full advantage of SQL instead of fighting it.

## Sources Researched

- Apple NSPredicate docs: https://developer.apple.com/documentation/foundation/nspredicate
- Apple Predicate format syntax: https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/Predicates/Articles/pSyntax.html
- Apple Predicate Programming Guide introduction: https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/Predicates/AdditionalChapters/Introduction.html
- Apple subquery expression docs: https://developer.apple.com/documentation/foundation/nsexpression/init%28forsubquery%3Ausingiteratorvariable%3Apredicate%3A%29
- Apple collection operators: https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/KeyValueCoding/CollectionOperators.html
- MongoDB array-of-documents querying: https://www.mongodb.com/docs/manual/tutorial/query-array-of-documents/
- MongoDB `$elemMatch`: https://www.mongodb.com/docs/manual/reference/operator/query/elemmatch/
- MongoDB `$filter`: https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/
- MongoDB `$map`: https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/
- MongoDB `$size`: https://www.mongodb.com/docs/manual/reference/operator/aggregation/size/
- BigQuery operators: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/operators
- BigQuery array functions: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/array_functions
- BigQuery nested and repeated fields: https://docs.cloud.google.com/bigquery/docs/nested-repeated
- Trino arrays: https://trino.io/docs/current/functions/array.html
- Trino lambdas: https://trino.io/docs/current/functions/lambda.html
- Trino `UNNEST`: https://trino.io/docs/current/sql/select.html
- DuckDB JSON processing functions: https://duckdb.org/docs/stable/data/json/json_functions
- DuckDB `unnest`: https://duckdb.org/docs/stable/sql/query_syntax/unnest
- DuckDB lambda functions: https://duckdb.org/docs/stable/sql/functions/lambda
- DuckDB list functions: https://duckdb.org/docs/current/sql/functions/list.html
- DuckDB struct functions: https://duckdb.org/docs/stable/sql/functions/struct
- DuckDB `UNION` type: https://duckdb.org/docs/lts/sql/data_types/union.html
- DuckDB union functions: https://duckdb.org/docs/stable/sql/functions/union.html
- PostgreSQL arrays: https://www.postgresql.org/docs/current/functions-array.html
- PostgreSQL JSON / SQL-JSON path: https://www.postgresql.org/docs/current/functions-json.html
- Snowflake semi-structured querying: https://docs.snowflake.com/en/user-guide/querying-semistructured
- Snowflake `ARRAY_FLATTEN`: https://docs.snowflake.com/en/sql-reference/functions/array_flatten
- Cosmos DB JSON querying: https://learn.microsoft.com/en-us/cosmos-db/query/get-started-json
- Cosmos DB self-joins over arrays: https://learn.microsoft.com/en-us/cosmos-db/query/join
- Cosmos DB subqueries and array construction: https://learn.microsoft.com/en-us/cosmos-db/query/subquery
- EdgeQL overview / design goals: https://docs.geldata.com/reference/edgeql
- EdgeQL paths: https://docs.geldata.com/reference/edgeql/paths
- EdgeQL shapes: https://docs.geldata.com/database/reference/edgeql/shapes
- EdgeQL sets and `array_unpack()`: https://docs.geldata.com/database/edgeql/sets
- EdgeQL `select` / nested filters: https://docs.geldata.com/reference/edgeql/select
- EdgeQL `for`: https://docs.geldata.com/reference/edgeql/for
- EdgeQL cardinality: https://docs.geldata.com/reference/reference/edgeql/cardinality
- EdgeQL set functions and assertions: https://docs.geldata.com/reference/stdlib/set
- EdgeQL generic operators including `?=`: https://docs.geldata.com/reference/stdlib/generic
