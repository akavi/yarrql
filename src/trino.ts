// SQL code generator for Trino dialect
// Used for Stripe Sigma's query engine
import type { Query, Schema, Expr } from "./ast"
import { unreachable, exprType } from "./ast"

// --- ALIAS UTILS ---
function* aliasGenerator() {
  let n = 0;
  while (true) yield `_t${n++}`;
}

// --- MAIN ---
// Query types that should be emitted as table expressions
const QUERY_TYPES = new Set([
  "table", "filter", "map", "sort", "limit", "offset",
  "set_op", "group_by", "flat_map", "array"
]);

export function toSql(q: Expr<any>): string {
  let aliasGen = aliasGenerator();
  const ctx: SqlGenCtx = { aliasGen, depth: 0, aliasMap: new Map(), subqueryFields: new Map() };

  // Route to appropriate emitter based on expression type
  const t = exprType(q);
  if (QUERY_TYPES.has(t)) {
    const { sql } = emitQuery(q as Query<Schema>, ctx);
    return sql;
  } else {
    // Scalar expression (count, aggregates, etc.)
    return emitExpr(q, "", ctx);
  }
}

// --- RECURSIVE HELPERS ---
type SqlGenCtx = {
  aliasGen: Generator<string>,
  depth: number,
  // Maps row source expressions to their SQL aliases (for closure scoping)
  aliasMap: Map<Expr<any>, string>,
  // Maps field expressions to their source queries (for deferred aggregation)
  // Key is the field expr, value is the original query expr
  subqueryFields: Map<Expr<any>, Expr<any>>
};

// Emit a predicate expression for JSON element access
// Used when filtering over unnested JSON arrays
function emitJsonPredicate(e: Expr<any>, jsonAlias: string, ctx: SqlGenCtx): string {
  const t = exprType(e);
  switch (t) {
    case "field": {
      // Access JSON field using json_extract
      const fieldName = (e as any).__field;
      return `json_extract(${jsonAlias}, '$."${fieldName}"')`;
    }
    case "count": {
      // Count of a JSON array field
      const source = (e as any).__source;
      if (exprType(source) === "field") {
        const fieldAccess = emitJsonPredicate(source, jsonAlias, ctx);
        return `COALESCE(json_array_length(${fieldAccess}), 0)`;
      }
      // Fallback to regular count
      const { sql: srcSql } = emitQuery(source as Query<Schema>, ctx);
      return `(SELECT COUNT(*) FROM ${srcSql})`;
    }
    case "comparison_op": {
      const op = (e as any).__op;
      const left = (e as any).__left;
      const right = (e as any).__right;
      const leftSql = emitJsonPredicate(left, jsonAlias, ctx);
      const rightSql = emitJsonPredicate(right, jsonAlias, ctx);
      switch (op) {
        case "gt": return `(${leftSql} > ${rightSql})`;
        case "lt": return `(${leftSql} < ${rightSql})`;
        case "gte": return `(${leftSql} >= ${rightSql})`;
        case "lte": return `(${leftSql} <= ${rightSql})`;
        default: return `(${leftSql} = ${rightSql})`;
      }
    }
    case "logical_op": {
      const op = (e as any).__op;
      const left = (e as any).__left;
      const right = (e as any).__right;
      const opStr = op === "and" ? " AND " : " OR ";
      return `(${emitJsonPredicate(left, jsonAlias, ctx)}${opStr}${emitJsonPredicate(right, jsonAlias, ctx)})`;
    }
    case "number":
      return String((e as any).__number);
    case "string":
      return `'${(e as any).__string.replace(/'/g, "''")}'`;
    case "boolean":
      return (e as any).__boolean ? 'TRUE' : 'FALSE';
    case "row":
      // Reference to the JSON element itself
      return jsonAlias;
    default:
      // Fallback to regular expression emission
      return emitExpr(e, jsonAlias, ctx);
  }
}

function emitQuery(q: Query<Schema>, ctx: SqlGenCtx): { sql: string, alias: string } {
  const t = exprType(q);

  // Handle filter/map whose source is a field (JSON array) - use UNNEST with CAST
  const sourceField = (q as any).__source;
  if (sourceField && exprType(sourceField) === "field") {
    if (t === "filter") {
      // Filter on a JSON array field: iterate with UNNEST
      const alias = ctx.aliasGen.next().value;
      const jsonAlias = ctx.aliasGen.next().value;

      // Get the outer table alias from the field's row source
      const rowExpr = sourceField.__source;
      let outerAlias = alias;
      if (rowExpr && exprType(rowExpr) === "row") {
        const rowSource = rowExpr.__source as Expr<any>;
        outerAlias = ctx.aliasMap.get(rowSource) ?? alias;
      }

      // Emit the field access using the outer alias
      const fieldName = sourceField.__field;
      const fieldSql = `${outerAlias}."${fieldName}"`;

      // Register this query so nested field accesses work
      ctx.aliasMap.set(q, jsonAlias);

      // For the filter predicate, emit with JSON field access
      const filterExpr = (q as any).__filter;
      const filterSql = emitJsonPredicate(filterExpr, jsonAlias, ctx);

      // Trino: Use UNNEST with CAST to iterate JSON array, then re-aggregate
      return {
        sql: `(SELECT CAST(ARRAY_AGG(${jsonAlias}) AS JSON) FROM UNNEST(CAST(${fieldSql} AS ARRAY<JSON>)) AS t(${jsonAlias}) WHERE ${filterSql}) AS ${alias}`,
        alias
      };
    }
  }

  switch (t) {
    case "table": {
      const alias = ctx.aliasGen.next().value;
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const cols = Object.keys((q as any).__schema.__fields).map(col => `"${col}"`).join(", ");
      return {
        sql: `(SELECT ${cols} FROM "${(q as any).__name}") AS ${alias}`,
        alias
      };
    }
    case "filter": {
      const { sql: sourceSql, alias } = emitQuery((q as any).__source as Query<Schema>, ctx);
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const exprSql = emitExpr((q as any).__filter, alias, ctx);
      return {
        sql: `(SELECT * FROM ${sourceSql} WHERE ${exprSql}) AS ${alias}`,
        alias
      };
    }
    case "map": {
      const { sql: sourceSql, alias: srcAlias } = emitQuery((q as any).__source as Query<Schema>, ctx);
      const mapExpr = (q as any).__map;

      // Identity map - just pass through
      if (exprType(mapExpr) === "row") {
        ctx.aliasMap.set(q, srcAlias);
        return { sql: sourceSql, alias: srcAlias };
      }

      // Record construction - emit each field with its name
      if (exprType(mapExpr) === "record") {
        const fields = emitRecordFields(mapExpr, srcAlias, ctx);
        const alias = ctx.aliasGen.next().value;
        ctx.aliasMap.set(q, alias);
        return {
          sql: `(SELECT ${fields} FROM ${sourceSql}) AS ${alias}`,
          alias
        };
      }

      // Scalar map - alias as "value" for aggregation compatibility
      const scalarExpr = emitExpr(mapExpr, srcAlias, ctx);
      const alias = ctx.aliasGen.next().value;
      ctx.aliasMap.set(q, alias);
      return {
        sql: `(SELECT ${scalarExpr} AS value FROM ${sourceSql}) AS ${alias}`,
        alias
      };
    }
    case "sort": {
      const { sql: srcSql, alias } = emitQuery((q as any).__source as Query<Schema>, ctx);
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const orderingSql = emitExpr((q as any).__sort, alias, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} ORDER BY ${orderingSql}) AS ${alias}`,
        alias
      };
    }
    case "limit": {
      const { sql: srcSql, alias } = emitQuery((q as any).__source as Query<Schema>, ctx);
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const limitSql = emitExpr((q as any).__limit as Expr<any>, alias, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} LIMIT ${limitSql}) AS ${alias}`,
        alias
      };
    }
    case "offset": {
      const { sql: srcSql, alias } = emitQuery((q as any).__source as Query<Schema>, ctx);
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const offsetSql = emitExpr((q as any).__offset as Expr<any>, alias, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} OFFSET ${offsetSql}) AS ${alias}`,
        alias
      };
    }
    case "set_op": {
      const { sql: leftSql, alias: leftAlias } = emitQuery((q as any).__left as Query<Schema>, ctx);
      const { sql: rightSql } = emitQuery((q as any).__right as Query<Schema>, ctx);
      // Register this query's alias for row references
      ctx.aliasMap.set(q, leftAlias);
      const op = (q as any).__op === "difference" ? "EXCEPT" : (q as any).__op.toUpperCase();
      return {
        sql: `(${leftSql} ${op} ${rightSql})`,
        alias: leftAlias
      };
    }
    case "group_by": {
      const { sql: srcSql, alias } = emitQuery((q as any).__source as Query<Schema>, ctx);
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const keyExpr = emitExpr((q as any).__key, alias, ctx);
      // Trino: Use ARRAY_AGG and cast to JSON
      return {
        sql: `(SELECT ${keyExpr} AS key, CAST(ARRAY_AGG(CAST(ROW(${alias}.*) AS JSON)) AS JSON) AS vals FROM ${srcSql} GROUP BY ${keyExpr}) AS ${alias}`,
        alias
      };
    }
    case "flat_map": {
      const { sql: sourceSql } = emitQuery((q as any).__source as Query<Schema>, ctx);
      const { sql: flatMapSql, alias: fmAlias } = emitQuery((q as any).__flatMap as Query<Schema>, ctx);
      const alias = ctx.aliasGen.next().value;
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      // Trino supports CROSS JOIN UNNEST for lateral-like behavior
      return {
        sql: `(SELECT ${fmAlias}.* FROM ${sourceSql}, LATERAL ${flatMapSql}) AS ${alias}`,
        alias
      };
    }
    case "array": {
      // Literal array - emit as VALUES
      const alias = ctx.aliasGen.next().value;
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const arr = (q as any).__array;
      if (arr.length === 0) {
        return {
          sql: `(SELECT * FROM (SELECT NULL) AS empty WHERE FALSE) AS ${alias}`,
          alias
        };
      }
      const values = arr.map((v: any) => `(${emitLiteral(v)})`).join(", ");
      return {
        sql: `(SELECT * FROM (VALUES ${values}) AS t(value)) AS ${alias}`,
        alias
      };
    }
    default:
      throw new Error(`Unknown query type: ${t}`);
  }
}

function emitRecordFields(e: Expr<any>, tableAlias: string, ctx: SqlGenCtx): string {
  const t = exprType(e);
  // Handle different expression types that could produce record-like output
  if (t === "field") {
    // Single field access
    return `${tableAlias}."${(e as any).__field}"`;
  }

  if (t === "record") {
    // Record literal - emit as comma-separated "expr AS name" pairs
    return Object.entries((e as any).__fields)
      .map(([name, expr]) => `${emitExpr(expr as Expr<any>, tableAlias, ctx)} AS "${name}"`)
      .join(", ");
  }

  // For now, just emit the expression as-is
  return emitExpr(e, tableAlias, ctx);
}

function emitExpr(e: Expr<any>, tableAlias: string, ctx: SqlGenCtx): string {
  const t = exprType(e);
  switch (t) {
    case "field": {
      // Look up the alias for the field's source
      const fieldSource = (e as any).__source;
      const fieldName = (e as any).__field;
      const sourceType = exprType(fieldSource);

      if (sourceType === "row") {
        const arraySource = fieldSource.__source;
        const alias = ctx.aliasMap.get(arraySource) ?? tableAlias;
        return `${alias}."${fieldName}"`;
      }

      if (sourceType === "first") {
        // Field access on a first() result - emit as scalar subquery
        const { sql: srcSql, alias: srcAlias } = emitQuery(fieldSource.__source as Query<Schema>, ctx);
        return `(SELECT ${srcAlias}."${fieldName}" FROM ${srcSql} LIMIT 1)`;
      }

      return `${tableAlias}."${fieldName}"`;
    }
    case "row": {
      // Row reference - look up the alias for the row's source
      const alias = ctx.aliasMap.get((e as any).__source as Expr<any>) ?? tableAlias;
      return `${alias}.*`;
    }
    case "first": {
      const { sql: srcSql, alias: srcAlias } = emitQuery((e as any).__source as Query<Schema>, ctx);
      // Trino: Cast row to JSON
      return `(SELECT CAST(ROW(${srcAlias}.*) AS JSON) FROM ${srcSql} LIMIT 1)`;
    }
    case "number":
      return String((e as any).__number);
    case "string":
      return `'${(e as any).__string.replace(/'/g, "''")}'`;
    case "boolean":
      return (e as any).__boolean ? 'TRUE' : 'FALSE';
    case "null":
      return 'NULL';
    case "eq": {
      // Handle NULL comparisons specially - SQL uses IS NULL, not = NULL
      if (exprType((e as any).__right) === "null") {
        return `(${emitExpr((e as any).__left, tableAlias, ctx)} IS NULL)`;
      }
      if (exprType((e as any).__left) === "null") {
        return `(${emitExpr((e as any).__right, tableAlias, ctx)} IS NULL)`;
      }
      return `(${emitExpr((e as any).__left, tableAlias, ctx)} = ${emitExpr((e as any).__right, tableAlias, ctx)})`;
    }
    case "string_comparison": {
      const op = (e as any).__op as 'like' | 'contains' | 'starts_with' | 'ends_with';
      const left = emitExpr((e as any).__left, tableAlias, ctx);
      const right = (e as any).__right;
      // Trino: Use CONCAT for string concatenation instead of ||
      switch (op) {
        case "like":
          return `(${left} LIKE ${emitExpr(right, tableAlias, ctx)})`;
        case "contains":
          return `(${left} LIKE CONCAT('%', ${emitExpr(right, tableAlias, ctx)}, '%'))`;
        case "starts_with":
          return `(${left} LIKE CONCAT(${emitExpr(right, tableAlias, ctx)}, '%'))`;
        case "ends_with":
          return `(${left} LIKE CONCAT('%', ${emitExpr(right, tableAlias, ctx)}))`;
        default:
          return unreachable(op);
      }
    }
    case "concat": {
      const left = emitExpr((e as any).__left, tableAlias, ctx);
      const right = emitExpr((e as any).__right, tableAlias, ctx);
      // Trino: Use CONCAT function
      return `CONCAT(${left}, ${right})`;
    }
    case "lower":
      return `LOWER(${emitExpr((e as any).__expr, tableAlias, ctx)})`;
    case "upper":
      return `UPPER(${emitExpr((e as any).__expr, tableAlias, ctx)})`;
    case "length":
      return `LENGTH(${emitExpr((e as any).__expr, tableAlias, ctx)})`;
    case "string_desc": {
      const innerSql = emitExpr((e as any).__expr, tableAlias, ctx);
      return `${innerSql} DESC`;
    }
    case "math_op": {
      const op = (e as any).__op as 'plus' | 'minus';
      const left = (e as any).__left;
      const right = (e as any).__right;
      switch (op) {
        case "plus": return `(${emitExpr(left, tableAlias, ctx)} + ${emitExpr(right, tableAlias, ctx)})`;
        case "minus": return `(${emitExpr(left, tableAlias, ctx)} - ${emitExpr(right, tableAlias, ctx)})`;
        default: return unreachable(op);
      }
    }
    case "comparison_op": {
      const op = (e as any).__op as 'gt' | 'lt' | 'gte' | 'lte';
      const left = (e as any).__left;
      const right = (e as any).__right;
      switch (op) {
        case "gt": return `(${emitExpr(left, tableAlias, ctx)} > ${emitExpr(right, tableAlias, ctx)})`;
        case "lt": return `(${emitExpr(left, tableAlias, ctx)} < ${emitExpr(right, tableAlias, ctx)})`;
        case "gte": return `(${emitExpr(left, tableAlias, ctx)} >= ${emitExpr(right, tableAlias, ctx)})`;
        case "lte": return `(${emitExpr(left, tableAlias, ctx)} <= ${emitExpr(right, tableAlias, ctx)})`;
        default: return unreachable(op);
      }
    }
    case "logical_op": {
      const op = (e as any).__op === "and" ? " AND " : " OR ";
      return `(${emitExpr((e as any).__left, tableAlias, ctx)}${op}${emitExpr((e as any).__right, tableAlias, ctx)})`;
    }
    case "not":
      return `(NOT ${emitExpr((e as any).__expr, tableAlias, ctx)})`;
    case "count": {
      // If source is a field (JSON array from subquery), use json_array_length
      const sourceType = exprType((e as any).__source);
      if (sourceType === "field" || sourceType === "row") {
        const fieldSql = emitExpr((e as any).__source as Expr<any>, tableAlias, ctx);
        return `COALESCE(json_array_length(${fieldSql}), 0)`;
      }
      const { sql: srcSql } = emitQuery((e as any).__source as Query<Schema>, ctx);
      return `(SELECT COUNT(*) FROM ${srcSql})`;
    }
    case "desc": {
      const innerSql = emitExpr((e as any).__expr, tableAlias, ctx);
      return `${innerSql} DESC`;
    }
    case "number_window": {
      const op = (e as any).__op as 'average' | 'max' | 'min' | 'sum';
      const source = (e as any).__source;
      const { sql: srcSql, alias: srcAlias } = emitQuery(source as unknown as Query<Schema>, ctx);
      switch (op) {
        case "average": return `(SELECT AVG(${srcAlias}.value) FROM ${srcSql})`;
        case "max": return `(SELECT MAX(${srcAlias}.value) FROM ${srcSql})`;
        case "min": return `(SELECT MIN(${srcAlias}.value) FROM ${srcSql})`;
        case "sum": return `(SELECT COALESCE(SUM(${srcAlias}.value), 0) FROM ${srcSql})`;
        default: return unreachable(op);
      }
    }
    case "scalar_window": {
      const op = (e as any).__op as 'max' | 'min';
      const source = (e as any).__source;
      const { sql: srcSql, alias: srcAlias } = emitQuery(source as unknown as Query<Schema>, ctx);
      switch (op) {
        case "max": return `(SELECT MAX(${srcAlias}.value) FROM ${srcSql})`;
        case "min": return `(SELECT MIN(${srcAlias}.value) FROM ${srcSql})`;
        default: return unreachable(op);
      }
    }
    // Handle query types that can appear as expressions (table-valued)
    case "record": {
      // Record literal - Trino uses CAST(MAP(...) AS JSON)
      const keys = Object.keys((e as any).__fields).map(k => `'${k}'`).join(", ");
      const values = Object.values((e as any).__fields).map(expr => emitExpr(expr as Expr<any>, tableAlias, ctx)).join(", ");
      return `CAST(MAP(ARRAY[${keys}], ARRAY[${values}]) AS JSON)`;
    }
    case "table":
    case "filter":
    case "map":
    case "sort":
    case "limit":
    case "offset":
    case "set_op":
    case "group_by":
    case "flat_map":
    case "array": {
      // These are array/query expressions - emit as subquery with array aggregation cast to JSON
      const { sql: srcSql, alias: srcAlias } = emitQuery(e as Query<Schema>, ctx);
      return `(SELECT COALESCE(CAST(ARRAY_AGG(CAST(ROW(${srcAlias}.*) AS JSON)) AS JSON), CAST('[]' AS JSON)) FROM ${srcSql})`;
    }
    default:
      throw new Error(`Unknown expr type: ${t}`);
  }
}

function emitLiteral(v: unknown): string {
  if (v === null) return 'NULL';
  if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
  if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE';
  if (typeof v === 'number') return String(v);
  throw new Error(`Unsupported literal type: ${typeof v}`);
}
