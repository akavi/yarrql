import type { Query, Schema, Expr } from "./ast"
import { unreachable } from "./ast"

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
  const ctx: SqlGenCtx = { aliasGen, depth: 0, aliasMap: new Map() };

  // Route to appropriate emitter based on expression type
  if (QUERY_TYPES.has(q.type)) {
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
  aliasMap: Map<Expr<any>, string>
};

function emitQuery(q: Query<Schema>, ctx: SqlGenCtx): { sql: string, alias: string } {
  switch (q.type) {
    case "table": {
      const alias = ctx.aliasGen.next().value;
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const cols = Object.keys(q.schema.fields).map(col => `"${col}"`).join(", ");
      return {
        sql: `(SELECT ${cols} FROM "${q.name}") AS ${alias}`,
        alias
      };
    }
    case "filter": {
      const { sql: sourceSql, alias } = emitQuery(q.source as Query<Schema>, ctx);
      // Register this query's alias for row references
      ctx.aliasMap.set(q, alias);
      const exprSql = emitExpr(q.filter, alias, ctx);
      return {
        sql: `(SELECT * FROM ${sourceSql} WHERE ${exprSql}) AS ${alias}`,
        alias
      };
    }
    case "map": {
      const { sql: sourceSql, alias: srcAlias } = emitQuery(q.source as Query<Schema>, ctx);
      const mapExpr = q.map;

      // Identity map - just pass through
      if (mapExpr.type === "row") {
        return { sql: sourceSql, alias: srcAlias };
      }

      // Record construction - emit each field with its name
      if (mapExpr.type === "record") {
        const fields = emitRecordFields(mapExpr, srcAlias, ctx);
        const alias = ctx.aliasGen.next().value;
        return {
          sql: `(SELECT ${fields} FROM ${sourceSql}) AS ${alias}`,
          alias
        };
      }

      // Scalar map - alias as "value" for aggregation compatibility
      const scalarExpr = emitExpr(mapExpr, srcAlias, ctx);
      const alias = ctx.aliasGen.next().value;
      return {
        sql: `(SELECT ${scalarExpr} AS value FROM ${sourceSql}) AS ${alias}`,
        alias
      };
    }
    case "sort": {
      const { sql: srcSql, alias } = emitQuery(q.source as Query<Schema>, ctx);
      const orderingSql = emitExpr(q.sort, alias, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} ORDER BY ${orderingSql}) AS ${alias}`,
        alias
      };
    }
    case "limit": {
      const { sql: srcSql, alias } = emitQuery(q.source as Query<Schema>, ctx);
      const limitSql = emitExpr(q.limit as Expr<any>, alias, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} LIMIT ${limitSql}) AS ${alias}`,
        alias
      };
    }
    case "offset": {
      const { sql: srcSql, alias } = emitQuery(q.source as Query<Schema>, ctx);
      const offsetSql = emitExpr(q.offset as Expr<any>, alias, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} OFFSET ${offsetSql}) AS ${alias}`,
        alias
      };
    }
    case "set_op": {
      const { sql: leftSql, alias: leftAlias } = emitQuery(q.left as Query<Schema>, ctx);
      const { sql: rightSql } = emitQuery(q.right as Query<Schema>, ctx);
      const op = q.op === "difference" ? "EXCEPT" : q.op.toUpperCase();
      return {
        sql: `(${leftSql} ${op} ${rightSql})`,
        alias: leftAlias
      };
    }
    case "group_by": {
      const { sql: srcSql, alias } = emitQuery(q.source as Query<Schema>, ctx);
      const keyExpr = emitExpr(q.key, alias, ctx);
      return {
        sql: `(SELECT ${keyExpr} AS key, json_agg(${alias}) AS vals FROM ${srcSql} GROUP BY ${keyExpr}) AS ${alias}`,
        alias
      };
    }
    case "flat_map": {
      const { sql: sourceSql, alias: srcAlias } = emitQuery(q.source as Query<Schema>, ctx);
      const { sql: flatMapSql, alias: fmAlias } = emitQuery(q.flatMap as Query<Schema>, ctx);
      const alias = ctx.aliasGen.next().value;
      return {
        sql: `(SELECT ${fmAlias}.* FROM ${sourceSql}, LATERAL ${flatMapSql}) AS ${alias}`,
        alias
      };
    }
    case "array": {
      // Literal array - emit as VALUES
      const alias = ctx.aliasGen.next().value;
      if (q.array.length === 0) {
        return {
          sql: `(SELECT * FROM (SELECT NULL) AS empty WHERE FALSE) AS ${alias}`,
          alias
        };
      }
      const values = q.array.map(v => `(${emitLiteral(v)})`).join(", ");
      return {
        sql: `(SELECT * FROM (VALUES ${values}) AS t(value)) AS ${alias}`,
        alias
      };
    }
    default:
      throw new Error(`Unknown query type: ${(q as any).type}`);
  }
}

function emitRecordFields(e: Expr<any>, tableAlias: string, ctx: SqlGenCtx): string {
  // Handle different expression types that could produce record-like output
  if (e.type === "field") {
    // Single field access
    return `${tableAlias}."${e.field}"`;
  }

  if (e.type === "record") {
    // Record literal - emit as comma-separated "expr AS name" pairs
    return Object.entries(e.fields)
      .map(([name, expr]) => `${emitExpr(expr as Expr<any>, tableAlias, ctx)} AS "${name}"`)
      .join(", ");
  }

  // For now, just emit the expression as-is
  return emitExpr(e, tableAlias, ctx);
}

function emitExpr(e: Expr<any>, tableAlias: string, ctx: SqlGenCtx): string {
  switch (e.type) {
    case "field": {
      // Look up the alias for the field's source (a row's source array)
      const rowSource = e.source;
      if (rowSource.type === "row") {
        const arraySource = rowSource.source;
        const alias = ctx.aliasMap.get(arraySource) ?? tableAlias;
        return `${alias}."${e.field}"`;
      }
      return `${tableAlias}."${e.field}"`;
    }
    case "row": {
      // Row reference - look up the alias for the row's source
      const alias = ctx.aliasMap.get(e.source as Expr<any>) ?? tableAlias;
      return `${alias}.*`;
    }
    case "first": {
      const srcSql = emitExpr(e.source as Expr<any>, tableAlias, ctx);
      return `(SELECT * FROM ${srcSql} LIMIT 1)`;
    }
    case "number":
      return String(e.number);
    case "string":
      return `'${e.string.replace(/'/g, "''")}'`;
    case "boolean":
      return e.boolean ? 'TRUE' : 'FALSE';
    case "null":
      return 'NULL';
    case "eq": {
      // Handle NULL comparisons specially - SQL uses IS NULL, not = NULL
      if (e.right.type === "null") {
        return `(${emitExpr(e.left, tableAlias, ctx)} IS NULL)`;
      }
      if (e.left.type === "null") {
        return `(${emitExpr(e.right, tableAlias, ctx)} IS NULL)`;
      }
      return `(${emitExpr(e.left, tableAlias, ctx)} = ${emitExpr(e.right, tableAlias, ctx)})`;
    }
    case "math_op": {
      const { op, left, right } = e;
      switch (op) {
        case "plus": return `(${emitExpr(left, tableAlias, ctx)} + ${emitExpr(right, tableAlias, ctx)})`;
        case "minus": return `(${emitExpr(left, tableAlias, ctx)} - ${emitExpr(right, tableAlias, ctx)})`;
        default: return unreachable(op);
      }
    }
    case "comparison_op": {
      const { op, left, right } = e;
      switch (op) {
        case "gt": return `(${emitExpr(left, tableAlias, ctx)} > ${emitExpr(right, tableAlias, ctx)})`;
        case "lt": return `(${emitExpr(left, tableAlias, ctx)} < ${emitExpr(right, tableAlias, ctx)})`;
        case "gte": return `(${emitExpr(left, tableAlias, ctx)} >= ${emitExpr(right, tableAlias, ctx)})`;
        case "lte": return `(${emitExpr(left, tableAlias, ctx)} <= ${emitExpr(right, tableAlias, ctx)})`;
        default: return unreachable(op);
      }
    }
    case "logical_op":
      const op = e.op === "and" ? " AND " : " OR ";
      return `(${emitExpr(e.left, tableAlias, ctx)}${op}${emitExpr(e.right, tableAlias, ctx)})`;
    case "not":
      return `(NOT ${emitExpr(e.expr, tableAlias, ctx)})`;
    case "count": {
      // If source is a field (JSON array from subquery), use json_array_length
      if (e.source.type === "field" || e.source.type === "row") {
        const fieldSql = emitExpr(e.source as Expr<any>, tableAlias, ctx);
        return `COALESCE(json_array_length(${fieldSql}), 0)`;
      }
      const { sql: srcSql } = emitQuery(e.source as Query<Schema>, ctx);
      return `(SELECT COUNT(*) FROM ${srcSql})`;
    }
    case "number_window": {
      const { op, source } = e;
      const { sql: srcSql, alias: srcAlias } = emitQuery(source as unknown as Query<Schema>, ctx);
      switch (op) {
        case "average": return `(SELECT AVG(${srcAlias}.value) FROM ${srcSql})`;
        case "max": return `(SELECT MAX(${srcAlias}.value) FROM ${srcSql})`;
        case "min": return `(SELECT MIN(${srcAlias}.value) FROM ${srcSql})`;
        default: return unreachable(op);
      }
    }
    case "scalar_window": {
      const { op, source } = e;
      const { sql: srcSql, alias: srcAlias } = emitQuery(source as unknown as Query<Schema>, ctx);
      switch (op) {
        case "max": return `(SELECT MAX(${srcAlias}.value) FROM ${srcSql})`;
        case "min": return `(SELECT MIN(${srcAlias}.value) FROM ${srcSql})`;
        default: return unreachable(op);
      }
    }
    // Handle query types that can appear as expressions (table-valued)
    case "record": {
      // Record literal - emit as json_build_object
      const pairs = Object.entries(e.fields)
        .map(([name, expr]) => `'${name}', ${emitExpr(expr as Expr<any>, tableAlias, ctx)}`)
        .join(", ");
      return `json_build_object(${pairs})`;
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
      // These are array/query expressions - emit as subquery with json_agg
      const { sql: srcSql, alias: srcAlias } = emitQuery(e as Query<Schema>, ctx);
      return `(SELECT COALESCE(json_agg(${srcAlias}), '[]') FROM ${srcSql})`;
    }
    default:
      throw new Error(`Unknown expr type: ${(e as any).type}`);
  }
}

function emitLiteral(v: unknown): string {
  if (v === null) return 'NULL';
  if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
  if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE';
  if (typeof v === 'number') return String(v);
  throw new Error(`Unsupported literal type: ${typeof v}`);
}
