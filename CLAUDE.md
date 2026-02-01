# yarrql - TypeScript Query Builder for PostgreSQL

## What is this?
**yarrql** is a TypeScript query builder that compiles to PostgreSQL. It provides a fluent API for building complex relational queries with support for nested subqueries, JSON aggregation, and correlated expressions.

## Key Files
- **`src/ast.ts`** - Core AST definitions, type system, and builder classes (`ArrayBuilder`, `NumberBuilder`, `StringBuilder`, `BooleanBuilder`, `NullBuilder`)
- **`src/postgres.ts`** - SQL code generation from AST
- **`test/run.ts`** - Integration tests against a real Postgres database
- **`test/seed.sql`** - Test data (Teachers, Students, Classes, Enrollments)

## Important Design Decisions

### 1. All internal fields use `__` prefix
This avoids conflicts with user-defined record fields:
- Type discriminator: `__kind` (not `type`)
- Expression discriminator: `__type`
- All other internal fields: `__source`, `__field`, `__filter`, `__map`, `__left`, `__right`, `__op`, `__expr`, `__number`, `__string`, etc.
- Builder node property: `__node`

### 2. `exprType(e)` helper
Use this to get the `__type` of an expression safely.

### 3. `first()` returns a single row
Returns `ExprBuilder<T>`, not an array. Field access on `first()` results emits scalar subqueries.

### 4. Correlation tracking via `ctx.aliasMap`
Maps expression objects (by reference) to SQL aliases. **All query types must register themselves**: `ctx.aliasMap.set(q, alias)`

### 5. JSON aggregation
Nested arrays in map results use `json_agg()`. Filtering on JSON arrays uses `json_array_elements()`.

## Running Tests
```bash
# Start postgres
docker run --name yarrql-pg -e POSTGRES_PASSWORD=test -p 5432:5432 -d postgres

# Seed database
docker exec -i yarrql-pg psql -U postgres < test/seed.sql

# Run tests
npx tsx test/run.ts
```

## Type System
- `NumberType`, `StringType`, `BoolType`, `NullType` - Scalar types with `__kind` discriminator
- `ArrayType` - Has `__el` for element type
- `RecordType` - Has `__fields` mapping field names to types
- `ValueOf<T>` - Union of literals, `Expr<T>`, and `ExprBuilder<T>`
- `InferType<V>` - Infers the `Type` from a value (builder, literal, or record object)

## Builder Methods

### ArrayBuilder
- `filter(fn)` - Filter rows
- `map(fn)` - Transform rows
- `sort(fn)` - Order rows (use `.desc()` on numbers for descending)
- `first()` - Get first row as single element
- `limit(n)` / `offset(n)` - Pagination
- `count()` - Count rows
- `any(fn)` / `every(fn)` - Boolean checks
- `average()` / `sum()` / `max()` / `min()` - Aggregations

### NumberBuilder
- `eq()`, `neq()`, `gt()`, `gte()`, `lt()`, `lte()` - Comparisons
- `plus()`, `minus()` - Arithmetic
- `desc()` - For descending sort order

### StringBuilder
- `eq()`, `neq()` - Equality
- `like(pattern)` - SQL LIKE pattern matching
- `contains(str)`, `startsWith(str)`, `endsWith(str)` - Substring checks
- `concat(str)` - String concatenation
- `toLowerCase()`, `toUpperCase()` - Case transformation
- `length()` → NumberBuilder
- `desc()` - For descending sort order

### BooleanBuilder
- `eq()`, `neq()` - Equality
- `and()`, `or()` - Logical operators
- `not()` - Negation
