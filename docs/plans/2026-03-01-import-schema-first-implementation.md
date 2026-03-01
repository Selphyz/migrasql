# Schema-First CSV Import Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make CSV import validate and parse values using the real destination table schema (when table exists), failing fast on first incompatible row with full row-content diagnostics.

**Architecture:** Extend the database session contract to expose destination column schema metadata, normalize engine-native types into shared logical kinds, and route import parsing through that schema instead of CSV inference whenever the table already exists. Keep current bootstrap behavior (infer+create) only for non-existing tables.

**Tech Stack:** Rust, Tokio async, SQLx, anyhow, csv, indicatif, module-level unit tests.

---

### Task 1: Add shared schema metadata types and trait contract

**Files:**
- Modify: `src/engine/mod.rs`

**Step 1: Write the failing test**

Create a compile-time usage in an existing test module or add a minimal new unit test in `src/import.rs` that references the new types (`ColumnSchema`, `ColumnKind`) and `describe_table_columns` in a mock implementation to force missing API compile errors.

**Step 2: Run test to verify it fails**

Run: `cargo test import::tests::schema_types_are_usable -- --nocapture`
Expected: FAIL/compile error because schema types or trait method do not exist yet.

**Step 3: Write minimal implementation**

In `src/engine/mod.rs`:
- Add `ColumnKind` enum with normalized variants: `Int`, `Float`, `Bool`, `Date`, `Timestamp`, `String`.
- Add `ColumnSchema` struct with fields:
  - `name: String`
  - `kind: ColumnKind`
  - `nullable: bool`
  - `db_type_name: String`
- Extend `DbSession` trait with:
  - `async fn describe_table_columns(&mut self, table: &str) -> Result<Vec<ColumnSchema>>;`

**Step 4: Run test to verify it passes**

Run: `cargo test import::tests::schema_types_are_usable -- --nocapture`
Expected: PASS for the targeted test.

**Step 5: Commit**

Do not run git commands in this repository unless explicitly requested.

---

### Task 2: Implement schema introspection for MySQL session

**Files:**
- Modify: `src/engine/mysql.rs`
- Test: `src/engine/mysql.rs` (module tests)

**Step 1: Write the failing test**

Add unit tests for the type-normalization helper (pure function) that maps representative MySQL type names to `ColumnKind`:
- `int`, `bigint`, `tinyint` -> `Int` (except `tinyint(1)` can be handled as `Bool` if desired by current project rules)
- `decimal`, `float`, `double` -> `Float`
- `date` -> `Date`
- `datetime`, `timestamp` -> `Timestamp`
- `varchar`, `text`, unknown -> `String`

**Step 2: Run test to verify it fails**

Run: `cargo test mysql::tests::maps_mysql_type_names -- --nocapture`
Expected: FAIL because helper does not exist.

**Step 3: Write minimal implementation**

Implement `describe_table_columns` for MySQL session:
- Query `information_schema.columns` using current database/schema and target table.
- Return ordered columns (`ordinal_position`).
- Fill `ColumnSchema` with normalized `ColumnKind`, nullable flag, and raw DB type string.

Add/implement normalization helper function and keep it deterministic and small.

**Step 4: Run test to verify it passes**

Run: `cargo test mysql::tests::maps_mysql_type_names -- --nocapture`
Expected: PASS.

**Step 5: Commit**

Do not run git commands in this repository unless explicitly requested.

---

### Task 3: Implement schema introspection for Postgres session

**Files:**
- Modify: `src/engine/postgres.rs`
- Test: `src/engine/postgres.rs` (module tests)

**Step 1: Write the failing test**

Add unit tests for Postgres type normalization helper:
- `smallint`, `integer`, `bigint` -> `Int`
- `real`, `double precision`, `numeric` -> `Float`
- `boolean` -> `Bool`
- `date` -> `Date`
- `timestamp`, `timestamp with time zone` -> `Timestamp`
- `character varying`, `text`, `jsonb`, unknown -> `String`

**Step 2: Run test to verify it fails**

Run: `cargo test postgres::tests::maps_postgres_type_names -- --nocapture`
Expected: FAIL because helper does not exist.

**Step 3: Write minimal implementation**

Implement `describe_table_columns` for Postgres session:
- Query `information_schema.columns` (schema + table), ordered by ordinal position.
- Build `ColumnSchema` entries with raw type name and normalized kind.

**Step 4: Run test to verify it passes**

Run: `cargo test postgres::tests::maps_postgres_type_names -- --nocapture`
Expected: PASS.

**Step 5: Commit**

Do not run git commands in this repository unless explicitly requested.

---

### Task 4: Wire import flow to schema-first typing when table exists

**Files:**
- Modify: `src/import.rs`
- Test: `src/import.rs` (existing `#[cfg(test)]` module)

**Step 1: Write the failing test**

Add tests around import planning/parsing helpers to verify:
- when table exists, inferred types are not used for parse decisions
- type vector is built from `describe_table_columns` according to mapped destination columns

Use focused helper-level tests with a fake schema vector to avoid heavy integration setup.

**Step 2: Run test to verify it fails**

Run: `cargo test import::tests::table_schema_overrides_inference -- --nocapture`
Expected: FAIL because schema-first resolution path is missing.

**Step 3: Write minimal implementation**

In `src/import.rs`:
- After checking table existence:
  - if table exists: call `describe_table_columns`, validate mapping, resolve expected per-column schema.
  - if not: keep infer+create path.
- Refactor parse path to consume schema-aware expected types.
- Keep behavior of `start_line`, batching, and progress tracker unchanged.

**Step 4: Run test to verify it passes**

Run: `cargo test import::tests::table_schema_overrides_inference -- --nocapture`
Expected: PASS.

**Step 5: Commit**

Do not run git commands in this repository unless explicitly requested.

---

### Task 5: Enforce strict fail-fast error payload with row content

**Files:**
- Modify: `src/import.rs`
- Test: `src/import.rs`

**Step 1: Write the failing test**

Add tests verifying first type mismatch error includes:
- CSV line number
- destination column name
- expected DB type name
- raw value
- row preview content

**Step 2: Run test to verify it fails**

Run: `cargo test import::tests::schema_mismatch_error_contains_row_context -- --nocapture`
Expected: FAIL because message lacks one or more required fields.

**Step 3: Write minimal implementation**

Enhance error construction in parse/validation path:
- Create a dedicated formatting helper for row mismatch diagnostics.
- Ensure `bail!` path in fail-fast mode uses this helper.
- Preserve existing last-success tracker output.

**Step 4: Run test to verify it passes**

Run: `cargo test import::tests::schema_mismatch_error_contains_row_context -- --nocapture`
Expected: PASS.

**Step 5: Commit**

Do not run git commands in this repository unless explicitly requested.

---

### Task 6: Add mapping and nullability pre-validation

**Files:**
- Modify: `src/import.rs`
- Test: `src/import.rs`

**Step 1: Write the failing test**

Add tests:
- mapped destination column missing in table schema -> preflight error
- empty value for non-nullable column -> fail-fast explicit error

**Step 2: Run test to verify it fails**

Run: `cargo test import::tests::mapping_and_nullability_validation -- --nocapture`
Expected: FAIL because validations are missing/incomplete.

**Step 3: Write minimal implementation**

Implement:
- preflight mapping validation against `describe_table_columns` results
- nullability checks in row parsing/validation
- clear error messages with line/column context

**Step 4: Run test to verify it passes**

Run: `cargo test import::tests::mapping_and_nullability_validation -- --nocapture`
Expected: PASS.

**Step 5: Commit**

Do not run git commands in this repository unless explicitly requested.

---

### Task 7: Run full verification and document behavior

**Files:**
- Modify: `README.md` (if import behavior docs exist)
- Optionally modify: `docs/csv-import-implementation.md`

**Step 1: Write the failing test**

No new failing test required here; this is verification/documentation.

**Step 2: Run tests and checks**

Run:
- `cargo fmt -- --check`
- `cargo clippy --all-targets --all-features`
- `cargo test`

Expected:
- all commands pass
- import-related tests cover schema-first + fail-fast diagnostics

**Step 3: Write minimal documentation updates**

Document:
- existing-table imports are schema-driven
- fail-fast diagnostics include row content and expected type
- recommendation to use `--skip-errors=false` for strict imports

**Step 4: Re-run focused checks**

Run: `cargo test import::tests -- --nocapture`
Expected: PASS.

**Step 5: Commit**

Do not run git commands in this repository unless explicitly requested.
