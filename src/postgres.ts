import type { Query, Schema, Expr } from "./ast"
import { unreachable } from "./ast"

// --- ALIAS UTILS ---
function aliasHash(str: string) {
  // Simple poor-man's hash, for uniqueness in deeply nested cases
  let h = 0; for (let i = 0; i < str.length; ++i) h = (h * 31 + str.charCodeAt(i)) | 0;
  return "a" + (Math.abs(h)).toString(36);
}
function isExpr(x: any): x is Expr {
  return x && typeof x === "object" && "type" in x && [
    "expr_column", "value", "eq", "binary_op", "logical_op", "not", "agg"
  ].includes(x.type);
}

// --- MAIN ---
export function toSql(q: Query<Schema>): string {
  let aliasGen = aliasGenerator();
  const { sql } = emitQuery(q, { aliasGen, depth: 0 });
  return sql;
}

// --- RECURSIVE HELPERS ---
type SqlGenCtx = { aliasGen: Generator<string>, depth: number };

function* aliasGenerator() {
  let n = 0;
  while (true) yield `_t${n++}`;
}

function emitQuery(q: Query<Schema>, ctx: SqlGenCtx): { sql: string, alias: string } {
  switch (q.type) {
    case "table": {
      const alias = ctx.aliasGen.next().value;
      // Select all columns as base
      const cols = Object.keys(q.schema).map(col => `"${col}"`).join(", ");
      return {
        sql: `(SELECT ${cols} FROM "${q.name}") AS ${alias}`,
        alias
      };
    }
    case "filter": {
      const { sql: sourceSql, alias } = emitQuery(q.source, ctx);
      const exprSql = emitExpr(q.filter, alias, ctx);
      return {
        sql: `(SELECT * FROM ${sourceSql} WHERE ${exprSql}) AS ${alias}`,
        alias
      };
    }
    case "map": {
      const { sql: sourceSql, alias: srcAlias } = emitQuery(q.source, ctx);
      // For each projected field: if it's an Expr, render as scalar; if it's a Query, render as LATERAL json_agg
      const fields = Object.entries(q.mapped).map(([k, v]) => {
        if (isExpr(v)) {
          return `${emitExpr(v, srcAlias, ctx)} AS "${k}"`;
        } else {
          // Table-valued column: subquery, must use LATERAL + json_agg
          const subAlias = aliasHash(k + "_" + JSON.stringify(v));
          const { sql: subSql, alias: sA } = emitQuery(v, { ...ctx, depth: ctx.depth + 1 });
          // We need to correlate the subquery to the current row via LATERAL
          return `(SELECT COALESCE(json_agg(${sA}), '[]') FROM ${subSql} WHERE TRUE) AS "${k}"`;
        }
      }).join(",\n  ");
      const alias = ctx.aliasGen.next().value;
      return {
        sql: `(SELECT ${fields} FROM ${sourceSql}) AS ${alias}`,
        alias
      };
    }
    case "order_by": {
      const { sql: srcSql, alias } = emitQuery(q.source, ctx);
      const orderingSql = q.orderings.map(ord =>
        `${emitExpr(ord.expr, alias, ctx)} ${ord.direction}`
      ).join(", ");
      return {
        sql: `(SELECT * FROM ${srcSql} ORDER BY ${orderingSql}) AS ${alias}`,
        alias
      };
    }
    case "limit": {
      const { sql: srcSql, alias } = emitQuery(q.source, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} LIMIT ${q.limit}) AS ${alias}`,
        alias
      };
    }
    case "offset": {
      const { sql: srcSql, alias } = emitQuery(q.source, ctx);
      return {
        sql: `(SELECT * FROM ${srcSql} OFFSET ${q.offset}) AS ${alias}`,
        alias
      };
    }
    case "set_op": {
      const { sql: leftSql, alias: leftAlias } = emitQuery(q.left, ctx);
      const { sql: rightSql } = emitQuery(q.right, ctx);
      return {
        sql: `(${leftSql} ${q.op.toUpperCase()} ${rightSql})`,
        alias: leftAlias // pick left arbitrarily
      };
    }
    case "group_by": {
      const { sql: srcSql, alias } = emitQuery(q.source, ctx);
      const keyExpr = emitExpr(q.key, alias, ctx);
      return {
        sql: `(SELECT ${keyExpr} AS key, json_agg(${alias}) AS values FROM ${srcSql} GROUP BY ${keyExpr}) AS ${alias}`,
        alias
      };
    }
    case "query_column": {
      // Select just one column from the source (for table-valued columns)
      const { sql: parentSql, alias: parentAlias } = emitQuery(q.source, ctx);
      return {
        sql: `(SELECT "${q.column}" FROM ${parentSql}) AS ${parentAlias}`,
        alias: parentAlias
      };
    }
    default:
      throw new Error(`Unknown query type: ${(q as any).type}`);
  }
}

function emitExpr(e: Expr, tableAlias: string, ctx: SqlGenCtx): string {
  switch (e.type) {
    case "expr_column":
      return `${tableAlias}."${e.column}"`;
    case "value":
      // Only string, number, boolean, null supported
      if (e.value === null) return 'NULL';
      if (typeof e.value === 'string') return `'${e.value.replace(/'/g, "''")}'`;
      if (typeof e.value === 'boolean') return e.value ? 'TRUE' : 'FALSE';
      return String(e.value);
    case "eq":
      return `(${emitExpr(e.left, tableAlias, ctx)} = ${emitExpr(e.right, tableAlias, ctx)})`;
    case "binary_op":
      switch (e.op) {
        case "plus": return `(${emitExpr(e.left, tableAlias, ctx)} + ${emitExpr(e.right, tableAlias, ctx)})`;
        case "minus": return `(${emitExpr(e.left, tableAlias, ctx)} - ${emitExpr(e.right, tableAlias, ctx)})`;
        case "gt": return `(${emitExpr(e.left, tableAlias, ctx)} > ${emitExpr(e.right, tableAlias, ctx)})`;
        case "lt": return `(${emitExpr(e.left, tableAlias, ctx)} < ${emitExpr(e.right, tableAlias, ctx)})`;
        case "gte": return `(${emitExpr(e.left, tableAlias, ctx)} >= ${emitExpr(e.right, tableAlias, ctx)})`;
        case "lte": return `(${emitExpr(e.left, tableAlias, ctx)} <= ${emitExpr(e.right, tableAlias, ctx)})`;
        case "ne": return `(${emitExpr(e.left, tableAlias, ctx)} <> ${emitExpr(e.right, tableAlias, ctx)})`;
        default: return unreachable(e)
      }
    case "logical_op":
      if (e.args.length === 0) return e.op === "and" ? "TRUE" : "FALSE";
      return `(${e.args.map(arg => emitExpr(arg, tableAlias, ctx)).join(
        e.op === "and" ? " AND " : " OR "
      )})`;
    case "not":
      return `(NOT ${emitExpr(e.expr, tableAlias, ctx)})`;
    case "agg": {
      // e.source: Query<Schema>
      const { sql: srcSql, alias: srcAlias } = emitQuery(e.source, ctx);
      let colExpr = `${srcAlias}.*`;
      // In a real impl, we'd want to select just one column!
      switch (e.op) {
        case "count": return `(SELECT COUNT(*) FROM ${srcSql})`;
        case "average": return `(SELECT AVG(${colExpr}) FROM ${srcSql})`;
        case "max": return `(SELECT MAX(${colExpr}) FROM ${srcSql})`;
        case "min": return `(SELECT MIN(${colExpr}) FROM ${srcSql})`;
        default: return unreachable(e)
      }
    }
    default:
      throw new Error(`Unknown expr type: ${(e as any).type}`);
  }
}
