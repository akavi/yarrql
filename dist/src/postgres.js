"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toSql = void 0;
const ast_1 = require("./ast");
// --- ALIAS UTILS ---
function* aliasGenerator() {
    let n = 0;
    while (true)
        yield `_t${n++}`;
}
// --- MAIN ---
// Query types that should be emitted as table expressions
const QUERY_TYPES = new Set([
    "table", "filter", "map", "sort", "limit", "offset",
    "set_op", "group_by", "flat_map", "array"
]);
function toSql(q) {
    let aliasGen = aliasGenerator();
    const ctx = { aliasGen, depth: 0, aliasMap: new Map(), subqueryFields: new Map() };
    // Route to appropriate emitter based on expression type
    const t = (0, ast_1.exprType)(q);
    if (QUERY_TYPES.has(t)) {
        const { sql } = emitQuery(q, ctx);
        return sql;
    }
    else {
        // Scalar expression (count, aggregates, etc.)
        return emitExpr(q, "", ctx);
    }
}
exports.toSql = toSql;
// Emit a predicate expression for JSON element access
// Used when filtering over jsonb_array_elements
function emitJsonPredicate(e, jsonAlias, ctx) {
    const t = (0, ast_1.exprType)(e);
    switch (t) {
        case "field": {
            // Access JSON field: jsonAlias->>'fieldName' for scalars, jsonAlias->'fieldName' for nested
            const fieldName = e.__field;
            // For nested arrays (like 'students'), use -> to keep as JSON
            return `${jsonAlias}->'${fieldName}'`;
        }
        case "count": {
            // Count of a JSON array field
            const source = e.__source;
            if ((0, ast_1.exprType)(source) === "field") {
                const fieldAccess = emitJsonPredicate(source, jsonAlias, ctx);
                return `COALESCE(json_array_length(${fieldAccess}), 0)`;
            }
            // Fallback to regular count
            const { sql: srcSql } = emitQuery(source, ctx);
            return `(SELECT COUNT(*) FROM ${srcSql})`;
        }
        case "comparison_op": {
            const op = e.__op;
            const left = e.__left;
            const right = e.__right;
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
            const op = e.__op;
            const left = e.__left;
            const right = e.__right;
            const opStr = op === "and" ? " AND " : " OR ";
            return `(${emitJsonPredicate(left, jsonAlias, ctx)}${opStr}${emitJsonPredicate(right, jsonAlias, ctx)})`;
        }
        case "number":
            return String(e.__number);
        case "string":
            return `'${e.__string.replace(/'/g, "''")}'`;
        case "boolean":
            return e.__boolean ? 'TRUE' : 'FALSE';
        case "row":
            // Reference to the JSON element itself
            return jsonAlias;
        default:
            // Fallback to regular expression emission
            return emitExpr(e, jsonAlias, ctx);
    }
}
// Resolve a field expression to its original source query by traversing the AST
// e.g., t.classes where t is from Teachers.map(t => ({classes: Classes.filter(...)}))
// should resolve to the Classes.filter(...) expression
function resolveFieldToQuery(e) {
    if ((0, ast_1.exprType)(e) !== "field")
        return null;
    const fieldExpr = e;
    const rowExpr = fieldExpr.__source;
    if ((0, ast_1.exprType)(rowExpr) !== "row")
        return null;
    const rowSource = rowExpr.__source;
    // The row source should be a map expression with a record
    if ((0, ast_1.exprType)(rowSource) !== "map")
        return null;
    const mapExpr = rowSource;
    const mapResult = mapExpr.__map;
    // The map result should be a record with the field we're looking for
    if ((0, ast_1.exprType)(mapResult) !== "record")
        return null;
    const recordExpr = mapResult;
    const fieldValue = recordExpr.__fields[fieldExpr.__field];
    if (!fieldValue)
        return null;
    return fieldValue;
}
function emitQuery(q, ctx) {
    const t = (0, ast_1.exprType)(q);
    // Handle filter/map whose source is a field (JSON array) - use jsonb_array_elements
    const sourceField = q.__source;
    if (sourceField && (0, ast_1.exprType)(sourceField) === "field") {
        if (t === "filter") {
            // Filter on a JSON array field: iterate with jsonb_array_elements
            const alias = ctx.aliasGen.next().value;
            const jsonAlias = ctx.aliasGen.next().value;
            // Get the outer table alias from the field's row source
            const rowExpr = sourceField.__source;
            let outerAlias = alias;
            if (rowExpr && (0, ast_1.exprType)(rowExpr) === "row") {
                const rowSource = rowExpr.__source;
                outerAlias = ctx.aliasMap.get(rowSource) ?? alias;
            }
            // Emit the field access using the outer alias
            const fieldName = sourceField.__field;
            const fieldSql = `${outerAlias}."${fieldName}"`;
            // Register this query so nested field accesses work
            ctx.aliasMap.set(q, jsonAlias);
            // For the filter predicate, emit with JSON field access
            const filterExpr = q.__filter;
            const filterSql = emitJsonPredicate(filterExpr, jsonAlias, ctx);
            return {
                sql: `(SELECT json_agg(${jsonAlias}) FROM json_array_elements(${fieldSql}) AS ${jsonAlias} WHERE ${filterSql}) AS ${alias}`,
                alias
            };
        }
    }
    switch (t) {
        case "table": {
            const alias = ctx.aliasGen.next().value;
            // Register this query's alias for row references
            ctx.aliasMap.set(q, alias);
            const cols = Object.keys(q.__schema.__fields).map(col => `"${col}"`).join(", ");
            return {
                sql: `(SELECT ${cols} FROM "${q.__name}") AS ${alias}`,
                alias
            };
        }
        case "filter": {
            const { sql: sourceSql, alias } = emitQuery(q.__source, ctx);
            // Register this query's alias for row references
            ctx.aliasMap.set(q, alias);
            const exprSql = emitExpr(q.__filter, alias, ctx);
            return {
                sql: `(SELECT * FROM ${sourceSql} WHERE ${exprSql}) AS ${alias}`,
                alias
            };
        }
        case "map": {
            const { sql: sourceSql, alias: srcAlias } = emitQuery(q.__source, ctx);
            const mapExpr = q.__map;
            // Identity map - just pass through
            if ((0, ast_1.exprType)(mapExpr) === "row") {
                ctx.aliasMap.set(q, srcAlias);
                return { sql: sourceSql, alias: srcAlias };
            }
            // Record construction - emit each field with its name
            if ((0, ast_1.exprType)(mapExpr) === "record") {
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
            const { sql: srcSql, alias } = emitQuery(q.__source, ctx);
            const orderingSql = emitExpr(q.__sort, alias, ctx);
            return {
                sql: `(SELECT * FROM ${srcSql} ORDER BY ${orderingSql}) AS ${alias}`,
                alias
            };
        }
        case "limit": {
            const { sql: srcSql, alias } = emitQuery(q.__source, ctx);
            const limitSql = emitExpr(q.__limit, alias, ctx);
            return {
                sql: `(SELECT * FROM ${srcSql} LIMIT ${limitSql}) AS ${alias}`,
                alias
            };
        }
        case "offset": {
            const { sql: srcSql, alias } = emitQuery(q.__source, ctx);
            const offsetSql = emitExpr(q.__offset, alias, ctx);
            return {
                sql: `(SELECT * FROM ${srcSql} OFFSET ${offsetSql}) AS ${alias}`,
                alias
            };
        }
        case "set_op": {
            const { sql: leftSql, alias: leftAlias } = emitQuery(q.__left, ctx);
            const { sql: rightSql } = emitQuery(q.__right, ctx);
            const op = q.__op === "difference" ? "EXCEPT" : q.__op.toUpperCase();
            return {
                sql: `(${leftSql} ${op} ${rightSql})`,
                alias: leftAlias
            };
        }
        case "group_by": {
            const { sql: srcSql, alias } = emitQuery(q.__source, ctx);
            const keyExpr = emitExpr(q.__key, alias, ctx);
            return {
                sql: `(SELECT ${keyExpr} AS key, json_agg(${alias}) AS vals FROM ${srcSql} GROUP BY ${keyExpr}) AS ${alias}`,
                alias
            };
        }
        case "flat_map": {
            const { sql: sourceSql, alias: srcAlias } = emitQuery(q.__source, ctx);
            const { sql: flatMapSql, alias: fmAlias } = emitQuery(q.__flatMap, ctx);
            const alias = ctx.aliasGen.next().value;
            return {
                sql: `(SELECT ${fmAlias}.* FROM ${sourceSql}, LATERAL ${flatMapSql}) AS ${alias}`,
                alias
            };
        }
        case "array": {
            // Literal array - emit as VALUES
            const alias = ctx.aliasGen.next().value;
            const arr = q.__array;
            if (arr.length === 0) {
                return {
                    sql: `(SELECT * FROM (SELECT NULL) AS empty WHERE FALSE) AS ${alias}`,
                    alias
                };
            }
            const values = arr.map((v) => `(${emitLiteral(v)})`).join(", ");
            return {
                sql: `(SELECT * FROM (VALUES ${values}) AS t(value)) AS ${alias}`,
                alias
            };
        }
        default:
            throw new Error(`Unknown query type: ${t}`);
    }
}
function emitRecordFields(e, tableAlias, ctx) {
    const t = (0, ast_1.exprType)(e);
    // Handle different expression types that could produce record-like output
    if (t === "field") {
        // Single field access
        return `${tableAlias}."${e.__field}"`;
    }
    if (t === "record") {
        // Record literal - emit as comma-separated "expr AS name" pairs
        return Object.entries(e.__fields)
            .map(([name, expr]) => `${emitExpr(expr, tableAlias, ctx)} AS "${name}"`)
            .join(", ");
    }
    // For now, just emit the expression as-is
    return emitExpr(e, tableAlias, ctx);
}
function emitExpr(e, tableAlias, ctx) {
    const t = (0, ast_1.exprType)(e);
    switch (t) {
        case "field": {
            // Look up the alias for the field's source (a row's source array)
            const rowSource = e.__source;
            if ((0, ast_1.exprType)(rowSource) === "row") {
                const arraySource = rowSource.__source;
                const alias = ctx.aliasMap.get(arraySource) ?? tableAlias;
                return `${alias}."${e.__field}"`;
            }
            return `${tableAlias}."${e.__field}"`;
        }
        case "row": {
            // Row reference - look up the alias for the row's source
            const alias = ctx.aliasMap.get(e.__source) ?? tableAlias;
            return `${alias}.*`;
        }
        case "first": {
            const srcSql = emitExpr(e.__source, tableAlias, ctx);
            return `(SELECT * FROM ${srcSql} LIMIT 1)`;
        }
        case "number":
            return String(e.__number);
        case "string":
            return `'${e.__string.replace(/'/g, "''")}'`;
        case "boolean":
            return e.__boolean ? 'TRUE' : 'FALSE';
        case "null":
            return 'NULL';
        case "eq": {
            // Handle NULL comparisons specially - SQL uses IS NULL, not = NULL
            if ((0, ast_1.exprType)(e.__right) === "null") {
                return `(${emitExpr(e.__left, tableAlias, ctx)} IS NULL)`;
            }
            if ((0, ast_1.exprType)(e.__left) === "null") {
                return `(${emitExpr(e.__right, tableAlias, ctx)} IS NULL)`;
            }
            return `(${emitExpr(e.__left, tableAlias, ctx)} = ${emitExpr(e.__right, tableAlias, ctx)})`;
        }
        case "math_op": {
            const op = e.__op;
            const left = e.__left;
            const right = e.__right;
            switch (op) {
                case "plus": return `(${emitExpr(left, tableAlias, ctx)} + ${emitExpr(right, tableAlias, ctx)})`;
                case "minus": return `(${emitExpr(left, tableAlias, ctx)} - ${emitExpr(right, tableAlias, ctx)})`;
                default: return (0, ast_1.unreachable)(op);
            }
        }
        case "comparison_op": {
            const op = e.__op;
            const left = e.__left;
            const right = e.__right;
            switch (op) {
                case "gt": return `(${emitExpr(left, tableAlias, ctx)} > ${emitExpr(right, tableAlias, ctx)})`;
                case "lt": return `(${emitExpr(left, tableAlias, ctx)} < ${emitExpr(right, tableAlias, ctx)})`;
                case "gte": return `(${emitExpr(left, tableAlias, ctx)} >= ${emitExpr(right, tableAlias, ctx)})`;
                case "lte": return `(${emitExpr(left, tableAlias, ctx)} <= ${emitExpr(right, tableAlias, ctx)})`;
                default: return (0, ast_1.unreachable)(op);
            }
        }
        case "logical_op": {
            const op = e.__op === "and" ? " AND " : " OR ";
            return `(${emitExpr(e.__left, tableAlias, ctx)}${op}${emitExpr(e.__right, tableAlias, ctx)})`;
        }
        case "not":
            return `(NOT ${emitExpr(e.__expr, tableAlias, ctx)})`;
        case "count": {
            // If source is a field (JSON array from subquery), use json_array_length
            const sourceType = (0, ast_1.exprType)(e.__source);
            if (sourceType === "field" || sourceType === "row") {
                const fieldSql = emitExpr(e.__source, tableAlias, ctx);
                return `COALESCE(json_array_length(${fieldSql}), 0)`;
            }
            const { sql: srcSql } = emitQuery(e.__source, ctx);
            return `(SELECT COUNT(*) FROM ${srcSql})`;
        }
        case "number_window": {
            const op = e.__op;
            const source = e.__source;
            const { sql: srcSql, alias: srcAlias } = emitQuery(source, ctx);
            switch (op) {
                case "average": return `(SELECT AVG(${srcAlias}.value) FROM ${srcSql})`;
                case "max": return `(SELECT MAX(${srcAlias}.value) FROM ${srcSql})`;
                case "min": return `(SELECT MIN(${srcAlias}.value) FROM ${srcSql})`;
                default: return (0, ast_1.unreachable)(op);
            }
        }
        case "scalar_window": {
            const op = e.__op;
            const source = e.__source;
            const { sql: srcSql, alias: srcAlias } = emitQuery(source, ctx);
            switch (op) {
                case "max": return `(SELECT MAX(${srcAlias}.value) FROM ${srcSql})`;
                case "min": return `(SELECT MIN(${srcAlias}.value) FROM ${srcSql})`;
                default: return (0, ast_1.unreachable)(op);
            }
        }
        // Handle query types that can appear as expressions (table-valued)
        case "record": {
            // Record literal - emit as json_build_object
            const pairs = Object.entries(e.__fields)
                .map(([name, expr]) => `'${name}', ${emitExpr(expr, tableAlias, ctx)}`)
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
            const { sql: srcSql, alias: srcAlias } = emitQuery(e, ctx);
            return `(SELECT COALESCE(json_agg(${srcAlias}), '[]') FROM ${srcSql})`;
        }
        default:
            throw new Error(`Unknown expr type: ${t}`);
    }
}
function emitLiteral(v) {
    if (v === null)
        return 'NULL';
    if (typeof v === 'string')
        return `'${v.replace(/'/g, "''")}'`;
    if (typeof v === 'boolean')
        return v ? 'TRUE' : 'FALSE';
    if (typeof v === 'number')
        return String(v);
    throw new Error(`Unsupported literal type: ${typeof v}`);
}
