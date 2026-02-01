"use strict";
/**
 *
 * A TS library that allows specifying queries against a relational algebra that
 * allows table valued columns.
 *
 * Eg, Imagine we have the following tables:
 * table Student {
 *   id: UUID,
 *   name: string,
 *   age: number
 * }
 *
 * table Teacher {
 *   id: UUID
 * }
 *
 * table Class {
 *   id: UUID
 *   name: string,
 *   teacher_id: UUID,
 * }
 *
 * table Enrollment {
 *   id: UUID
 *   student_id: UUID
 *   class_id: UUID
 *   grade: undefined | number
 * }
 *
 * You can specify queries as follows:
 * const Teachers = Table({...schema from above})
 * const Students = Table({...schema from above})
 * const Enrollments = Table({...schema from above})
 * const Classes = Table({...schema from above})
 * const teachersWithStudents = Teachers.map(t => ({
 *     ...t,
 *     students: Students.filter(s => {
 *        return Enrollment.any(e => {
 *            return Class.any(c => c.teacher_id.eq(t.id).and(c.id.eq(e.class_id)).and(e.student_id.eq(s.id))
 *         })
 *    })
 * })
 *
 * and generate the logically equivalent SQL
 * console.log(teachersWithStudents.toSql())
 *
 * MORE EXAMPLE QUERIES:
 *
 * const teachersWithMatureClasses = Teachers.map(t => ({
 *   ...t,
 *   matureClasses: Classes.filter(c => c.teacher_id.eq(t.id))
 *     .map(c => ({
 *       averageStudentAge: Students.filter(s =>
 *         Enrollments.some(e => e.class_id.eq(c.id).and(e.student_id.eq(s.id)))
 *       ).map(s => {age: s.age}).avg(),
 *     }))
 *     .filter(c => c.averageStudentAge.gt(20))
 * }));
 *
 * const studentsWithPendingGrades = Students.map(s => ({
 *   ...s,
 *   id: s.id,
 *   pendingClasses: Classes.filter(c =>
 *     Enrollments.any(e =>
 *       e.student_id.eq(s.id)
 *         .and(e.class_id.eq(c.id))
 *         .and(e.grade.eq(undefined))
 *     )
 *   )
 * })).filter(s => s.pendingClasses.count().gt(0))
 *
 * const classesWithWideGradeRange = Classes.map(c => ({
 *   ....c,
 *   grades: Enrollments.where(e => e.class_id.eq(c.id).and(e.grade.neq(undefined))).map(e => {grade: e.grade})
 * })).map(c => ({
 *   ...c,
 *   maxGrade: grades.max()
 *   minGrade: grades.min()
 * }).filter(c => c.maxGrade.minus(c.minGrade).gt(20))
 *
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Table = exports.unreachable = exports.exprType = void 0;
const lodash_1 = require("lodash");
// Helper to get __type from an expression
function exprType(e) {
    return e.__type;
}
exports.exprType = exprType;
class ArrayBuilder {
    constructor(__node) {
        this.__node = __node;
    }
    filter(fn) {
        const __filter = withRowBuilder(this.__node, fn);
        return new ArrayBuilder({ __type: "filter", __source: this.__node, __filter });
    }
    sort(fn) {
        const __sort = withRowBuilder(this.__node, fn);
        return new ArrayBuilder({ __type: "sort", __source: this.__node, __sort });
    }
    groupBy(fn) {
        const __key = withRowBuilder(this.__node, fn);
        return new ArrayBuilder({ __type: "group_by", __source: this.__node, __key });
    }
    map(fn) {
        const __map = withRowBuilder(this.__node, fn);
        return new ArrayBuilder({ __type: "map", __source: this.__node, __map });
    }
    limit(value) {
        const __limit = toExpr(value);
        return new ArrayBuilder({ __type: "limit", __source: this.__node, __limit });
    }
    offset(value) {
        const __offset = toExpr(value);
        return new ArrayBuilder({ __type: "offset", __source: this.__node, __offset });
    }
    union(other) {
        const __right = other instanceof ArrayBuilder ? other.__node : other;
        return new ArrayBuilder({ __type: "set_op", __op: "union", __left: this.__node, __right });
    }
    intersection(other) {
        const __right = other instanceof ArrayBuilder ? other.__node : other;
        return new ArrayBuilder({ __type: "set_op", __op: "intersect", __left: this.__node, __right });
    }
    difference(other) {
        const __right = other instanceof ArrayBuilder ? other.__node : other;
        return new ArrayBuilder({ __type: "set_op", __op: "difference", __left: this.__node, __right });
    }
    count() {
        const __node = { __type: "count", __source: this.__node };
        return new NumberBuilder(__node);
    }
    average() {
        const type = getType(this.__node);
        if (type.__kind !== "array") {
            throw new Error("Cannot get average of non-array");
        }
        else if (type.__el.__kind !== "number") {
            throw new Error("Cannot get average of non-numeric-array");
        }
        return new NumberBuilder({ __type: 'number_window', __op: "average", __source: this.__node });
    }
    max() {
        const type = getType(this.__node);
        if (type.__kind !== "array") {
            throw new Error("Cannot get max of non-array");
        }
        else if (type.__el.__kind !== "number" && type.__el.__kind !== "string") {
            throw new Error("Cannot get max of non-numeric/string-array");
        }
        if (type.__el.__kind === "number") {
            return new NumberBuilder({ __type: 'scalar_window', __op: "max", __source: this.__node });
        }
        else if (type.__el.__kind === "string") {
            return new StringBuilder({ __type: 'scalar_window', __op: "max", __source: this.__node });
        }
        else {
            throw new Error("Cannot get max of non-numeric/string-array");
        }
    }
    min() {
        const type = getType(this.__node);
        if (type.__kind !== "array") {
            throw new Error("Cannot get min of non-array");
        }
        else if (type.__el.__kind !== "number" && type.__el.__kind !== "string") {
            throw new Error("Cannot get min of non-numeric/string-array");
        }
        if (type.__el.__kind === "number") {
            return new NumberBuilder({ __type: 'scalar_window', __op: "min", __source: this.__node });
        }
        else if (type.__el.__kind === "string") {
            return new StringBuilder({ __type: 'scalar_window', __op: "min", __source: this.__node });
        }
        else {
            throw new Error("Cannot get min of non-numeric/string-array");
        }
    }
    first() {
        return this.limit(1);
    }
    // helpers
    any(fn) {
        return this.filter(fn).count().gt(0);
    }
    every(fn) {
        return this.filter((val) => {
            const result = fn(val);
            // Convert to BooleanBuilder if needed, then negate
            const boolBuilder = result instanceof BooleanBuilder
                ? result
                : new BooleanBuilder(valueToExpr(result));
            return boolBuilder.not();
        }).count().eq(0);
    }
}
function getLiteralType(val) {
    if (val === null) {
        return { __kind: "null" };
    }
    else if ((0, lodash_1.isNumber)(val)) {
        return { __kind: "number" };
    }
    else if ((0, lodash_1.isString)(val)) {
        return { __kind: "string" };
    }
    else if ((0, lodash_1.isBoolean)(val)) {
        return { __kind: "bool" };
    }
    else if (val instanceof Array) {
        return { __kind: "array", __el: getLiteralType(val[0]) };
    }
    else if (val instanceof Object) {
        const __fields = (0, lodash_1.fromPairs)(Object.entries(val).map(([k, v]) => [k, getLiteralType(v)]));
        return { __kind: "record", __fields };
    }
    else {
        throw new Error("Wat");
    }
}
function getType(expr) {
    const t = expr.__type;
    if (t === "field") {
        const recordType = getType(expr.__source);
        return recordType.__fields[expr.__field];
    }
    else if (t === "first") {
        const arrayType = getType(expr.__source);
        return arrayType.__el;
    }
    else if (t === "row") {
        const arrayType = getType(expr.__source);
        return arrayType.__el;
    }
    else if (t === "scalar_window") {
        const arrayType = getType(expr.__source);
        return arrayType.__el;
    }
    else if (t === "number") {
        return { __kind: "number" };
    }
    else if (t === "math_op") {
        return { __kind: "number" };
    }
    else if (t === "number_window") {
        return { __kind: "number" };
    }
    else if (t === "count") {
        return { __kind: "number" };
    }
    else if (t === "boolean") {
        return { __kind: "bool" };
    }
    else if (t === "not") {
        return { __kind: "bool" };
    }
    else if (t === "eq") {
        return { __kind: "bool" };
    }
    else if (t === "comparison_op") {
        return { __kind: "bool" };
    }
    else if (t === "logical_op") {
        return { __kind: "bool" };
    }
    else if (t === "array") {
        // TODO: support unknown arrays;
        return getLiteralType(expr.__array);
    }
    else if (t === "table") {
        return { __kind: "array", __el: expr.__schema };
    }
    else if (t === "filter") {
        return getType(expr.__source);
    }
    else if (t === "sort") {
        return getType(expr.__source);
    }
    else if (t === "limit") {
        return getType(expr.__source);
    }
    else if (t === "offset") {
        return getType(expr.__source);
    }
    else if (t === "set_op") {
        return getType(expr.__right);
    }
    else if (t === "map") {
        const elType = getType(expr.__map);
        return { __kind: "array", __el: elType };
    }
    else if (t === "flat_map") {
        return getType(expr.__flatMap);
    }
    else if (t === "group_by") {
        const valType = getType(expr.__source);
        const keyType = getType(expr.__key);
        return { __kind: "record", __fields: { vals: valType, key: keyType } };
    }
    else if (t === "string") {
        return { __kind: "string" };
    }
    else if (t === "null") {
        return { __kind: "null" };
    }
    else if (t === "record") {
        const __fields = {};
        for (const [key, fieldExpr] of Object.entries(expr.__fields)) {
            __fields[key] = getType(fieldExpr);
        }
        return { __kind: "record", __fields };
    }
    else {
        throw new Error(`Unknown expr type: ${t}`);
    }
}
function createBuilder(expr, type) {
    if (type.__kind === "null") {
        return new NullBuilder(expr);
    }
    else if (type.__kind === "bool") {
        return new BooleanBuilder(expr);
    }
    else if (type.__kind === "number") {
        return new NumberBuilder(expr);
    }
    else if (type.__kind === "string") {
        return new StringBuilder(expr);
    }
    else if (type.__kind === "array") {
        return new ArrayBuilder(expr);
    }
    else if (type.__kind === "record") {
        const result = {};
        for (const [fieldName, fieldType] of Object.entries(type.__fields)) {
            const fieldExpr = {
                __type: 'field',
                __source: expr,
                __field: fieldName
            };
            result[fieldName] = createBuilder(fieldExpr, fieldType);
        }
        return result;
    }
    throw new Error(`Unknown type: ${type.__kind}`);
}
// Converts any ValueOf<T> (builder, expr, literal, or record object) to an Expr<T>
function valueToExpr(val) {
    // Handle builder instances
    if (val instanceof ArrayBuilder) {
        return val.__node;
    }
    else if (val instanceof NumberBuilder) {
        return val.__node;
    }
    else if (val instanceof StringBuilder) {
        return val.__node;
    }
    else if (val instanceof BooleanBuilder) {
        return val.__node;
    }
    else if (val instanceof NullBuilder) {
        return val.__node;
    }
    // Handle literals
    if (val === null) {
        return { __type: 'null' };
    }
    else if (typeof val === 'number') {
        return { __type: 'number', __number: val };
    }
    else if (typeof val === 'string') {
        return { __type: 'string', __string: val };
    }
    else if (typeof val === 'boolean') {
        return { __type: 'boolean', __boolean: val };
    }
    // Handle Expr objects (have __brand or __type property)
    if (typeof val === 'object' && val !== null && '__type' in val) {
        return val;
    }
    // Handle plain objects as record literals
    if (typeof val === 'object' && val !== null) {
        const __fields = {};
        for (const [key, value] of Object.entries(val)) {
            __fields[key] = valueToExpr(value);
        }
        return { __type: 'record', __fields };
    }
    throw new Error("Cannot convert value to expression");
}
function withRowBuilder(source, fn) {
    // Get the element type from the source array
    const sourceType = getType(source);
    if (sourceType.__kind !== "array") {
        throw new Error("withRowBuilder source must be an array");
    }
    const elementType = sourceType.__el;
    // Create a row expression representing a single element
    const rowExpr = { __type: 'row', __source: source };
    // Create the appropriate builder based on element type
    const rowBuilder = createBuilder(rowExpr, elementType);
    // Call the callback and convert result to expression
    const result = fn(rowBuilder);
    return valueToExpr(result);
}
function toExpr(val) {
    // Check for builder instances first (they don't have __brand)
    if (val instanceof ArrayBuilder || val instanceof NumberBuilder ||
        val instanceof StringBuilder || val instanceof BooleanBuilder ||
        val instanceof NullBuilder) {
        return val.__node;
    }
    // If it's already an Expr (has __brand or __type), return it
    if (typeof val === 'object' && val !== null && ('__brand' in val || '__type' in val)) {
        return val;
    }
    // Convert literal to Expr
    if (val === null) {
        return { __type: 'null' };
    }
    else if (typeof val === 'number') {
        return { __type: 'number', __number: val };
    }
    else if (typeof val === 'string') {
        return { __type: 'string', __string: val };
    }
    else if (typeof val === 'boolean') {
        return { __type: 'boolean', __boolean: val };
    }
    else if (Array.isArray(val)) {
        return { __type: 'array', __array: val };
    }
    // Fallback
    return val;
}
class NumberBuilder {
    constructor(__node) {
        this.__node = __node;
    }
    eq(value) {
        const __right = value === null ? { __type: 'null' } : toExpr(value);
        return new BooleanBuilder({ __type: "eq", __left: this.__node, __right });
    }
    gt(value) {
        const __right = toExpr(value);
        return new BooleanBuilder({ __type: "comparison_op", __op: "gt", __left: this.__node, __right });
    }
    lt(value) {
        const __right = toExpr(value);
        return new BooleanBuilder({ __type: "comparison_op", __op: "lt", __left: this.__node, __right });
    }
    minus(value) {
        const __right = toExpr(value);
        return new NumberBuilder({ __type: "math_op", __op: "minus", __left: this.__node, __right });
    }
    plus(value) {
        const __right = toExpr(value);
        return new NumberBuilder({ __type: "math_op", __op: "plus", __left: this.__node, __right });
    }
}
class StringBuilder {
    constructor(__node) {
        this.__node = __node;
    }
    eq(value) {
        const __right = value === null ? { __type: 'null' } : toExpr(value);
        return new BooleanBuilder({ __type: "eq", __left: this.__node, __right });
    }
}
class BooleanBuilder {
    constructor(__node) {
        this.__node = __node;
    }
    eq(value) {
        const __right = value === null ? { __type: 'null' } : toExpr(value);
        return new BooleanBuilder({ __type: "eq", __left: this.__node, __right });
    }
    and(value) {
        const __right = toExpr(value);
        return new BooleanBuilder({ __type: "logical_op", __op: "and", __left: this.__node, __right });
    }
    or(value) {
        const __right = toExpr(value);
        return new BooleanBuilder({ __type: "logical_op", __op: "or", __left: this.__node, __right });
    }
    not() {
        return new BooleanBuilder({ __type: "not", __expr: this.__node });
    }
}
class NullBuilder {
    constructor(__node) {
        this.__node = __node;
    }
    or(value) {
        return new BooleanBuilder({ __type: "eq", __left: this.__node, __right: toExpr(value) });
    }
}
function unreachable(val) {
    throw new Error(`Unexpected val: ${val}`);
}
exports.unreachable = unreachable;
function typeFromName(name) {
    switch (name) {
        case 'string': return { __kind: 'string' };
        case 'uuid': return { __kind: 'string' };
        case 'number': return { __kind: 'number' };
        case 'bool': return { __kind: 'bool' };
        case 'null': return { __kind: 'null' };
        default: return unreachable(name);
    }
}
function Table(name, schema) {
    const __fields = {};
    for (const [key, typeName] of Object.entries(schema)) {
        __fields[key] = typeFromName(typeName);
    }
    const recordType = { __kind: 'record', __fields };
    const tableExpr = {
        __type: 'table',
        __name: name,
        __schema: recordType
    };
    return new ArrayBuilder(tableExpr);
}
exports.Table = Table;
