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
exports.Table = exports.unreachable = void 0;
const lodash_1 = require("lodash");
class ArrayBuilder {
    constructor(node) {
        this.node = node;
    }
    filter(fn) {
        const filter = withRowBuilder(this.node, fn);
        return new ArrayBuilder({ type: "filter", source: this.node, filter });
    }
    sort(fn) {
        const sort = withRowBuilder(this.node, fn);
        return new ArrayBuilder({ type: "sort", source: this.node, sort });
    }
    groupBy(fn) {
        const key = withRowBuilder(this.node, fn);
        return new ArrayBuilder({ type: "group_by", source: this.node, key });
    }
    map(fn) {
        const map = withRowBuilder(this.node, fn);
        return new ArrayBuilder({ type: "map", source: this.node, map });
    }
    limit(value) {
        const limit = toExpr(value);
        return new ArrayBuilder({ type: "limit", source: this.node, limit });
    }
    offset(value) {
        const offset = toExpr(value);
        return new ArrayBuilder({ type: "offset", source: this.node, offset });
    }
    union(other) {
        const right = other instanceof ArrayBuilder ? other.node : other;
        return new ArrayBuilder({ type: "set_op", op: "union", left: this.node, right });
    }
    intersection(other) {
        const right = other instanceof ArrayBuilder ? other.node : other;
        return new ArrayBuilder({ type: "set_op", op: "intersect", left: this.node, right });
    }
    difference(other) {
        const right = other instanceof ArrayBuilder ? other.node : other;
        return new ArrayBuilder({ type: "set_op", op: "difference", left: this.node, right });
    }
    count() {
        const node = { type: "count", source: this.node };
        return new NumberBuilder(node);
    }
    average() {
        const type = getType(this.node);
        if (type.type !== "array") {
            throw new Error("Cannot get average of non-array");
        }
        else if (type.el.type !== "number") {
            throw new Error("Cannot get average of non-numeric-array");
        }
        return new NumberBuilder({ type: 'number_window', op: "average", source: this.node });
    }
    max() {
        const type = getType(this.node);
        if (type.type !== "array") {
            throw new Error("Cannot get max of non-array");
        }
        else if (type.el.type !== "number" && type.el.type !== "string") {
            throw new Error("Cannot get max of non-numeric/string-array");
        }
        if (type.el.type === "number") {
            return new NumberBuilder({ type: 'scalar_window', op: "max", source: this.node });
        }
        else if (type.el.type === "string") {
            return new StringBuilder({ type: 'scalar_window', op: "max", source: this.node });
        }
        else {
            throw new Error("Cannot get max of non-numeric/string-array");
        }
    }
    min() {
        const type = getType(this.node);
        if (type.type !== "array") {
            throw new Error("Cannot get min of non-array");
        }
        else if (type.el.type !== "number" && type.el.type !== "string") {
            throw new Error("Cannot get min of non-numeric/string-array");
        }
        if (type.el.type === "number") {
            return new NumberBuilder({ type: 'scalar_window', op: "min", source: this.node });
        }
        else if (type.el.type === "string") {
            return new StringBuilder({ type: 'scalar_window', op: "min", source: this.node });
        }
        else {
            throw new Error("Cannot get min of non-numeric/string-array");
        }
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
        return { type: "null" };
    }
    else if ((0, lodash_1.isNumber)(val)) {
        return { type: "number" };
    }
    else if ((0, lodash_1.isString)(val)) {
        return { type: "string" };
    }
    else if ((0, lodash_1.isBoolean)(val)) {
        return { type: "bool" };
    }
    else if (val instanceof Array) {
        return { type: "array", el: getLiteralType(val[0]) };
    }
    else if (val instanceof Object) {
        const fields = (0, lodash_1.fromPairs)(Object.entries(val).map(([k, v]) => [k, getLiteralType(v)]));
        return { type: "record", fields };
    }
    else {
        throw new Error("Wat");
    }
}
function getType(expr) {
    if (expr.type === "field") {
        const recordType = getType(expr.source);
        return recordType.fields[expr.field];
    }
    else if (expr.type === "first") {
        const arrayType = getType(expr.source);
        return arrayType.el;
    }
    else if (expr.type === "row") {
        const arrayType = getType(expr.source);
        return arrayType.el;
    }
    else if (expr.type === "scalar_window") {
        const arrayType = getType(expr.source);
        return arrayType.el;
    }
    else if (expr.type === "number") {
        return { type: "number" };
    }
    else if (expr.type === "math_op") {
        return { type: "number" };
    }
    else if (expr.type === "number_window") {
        return { type: "number" };
    }
    else if (expr.type === "count") {
        return { type: "number" };
    }
    else if (expr.type === "boolean") {
        return { type: "bool" };
    }
    else if (expr.type === "not") {
        return { type: "bool" };
    }
    else if (expr.type === "eq") {
        return { type: "bool" };
    }
    else if (expr.type === "comparison_op") {
        return { type: "bool" };
    }
    else if (expr.type === "logical_op") {
        return { type: "bool" };
    }
    else if (expr.type === "array") {
        // TODO: support unknown arrays;
        return getLiteralType(expr.array);
    }
    else if (expr.type === "table") {
        return { type: "array", el: expr.schema };
    }
    else if (expr.type === "filter") {
        return getType(expr.source);
    }
    else if (expr.type === "sort") {
        return getType(expr.source);
    }
    else if (expr.type === "limit") {
        return getType(expr.source);
    }
    else if (expr.type === "offset") {
        return getType(expr.source);
    }
    else if (expr.type === "set_op") {
        return getType(expr.right);
    }
    else if (expr.type === "map") {
        const elType = getType(expr.map);
        return { type: "array", el: elType };
    }
    else if (expr.type === "flat_map") {
        return getType(expr.flatMap);
    }
    else if (expr.type === "group_by") {
        const valType = getType(expr.source);
        const keyType = getType(expr.key);
        return { type: "record", fields: { vals: valType, key: keyType } };
    }
    else if (expr.type === "string") {
        return { type: "string" };
    }
    else if (expr.type === "null") {
        return { type: "null" };
    }
    else if (expr.type === "record") {
        const fields = {};
        for (const [key, fieldExpr] of Object.entries(expr.fields)) {
            fields[key] = getType(fieldExpr);
        }
        return { type: "record", fields };
    }
    else {
        return unreachable(expr);
    }
}
function createBuilder(expr, type) {
    if (type.type === "null") {
        return new NullBuilder(expr);
    }
    else if (type.type === "bool") {
        return new BooleanBuilder(expr);
    }
    else if (type.type === "number") {
        return new NumberBuilder(expr);
    }
    else if (type.type === "string") {
        return new StringBuilder(expr);
    }
    else if (type.type === "array") {
        return new ArrayBuilder(expr);
    }
    else if (type.type === "record") {
        const result = {};
        for (const [fieldName, fieldType] of Object.entries(type.fields)) {
            const fieldExpr = {
                type: 'field',
                source: expr,
                field: fieldName
            };
            result[fieldName] = createBuilder(fieldExpr, fieldType);
        }
        return result;
    }
    throw new Error(`Unknown type: ${type.type}`);
}
// Converts any ValueOf<T> (builder, expr, literal, or record object) to an Expr<T>
function valueToExpr(val) {
    // Handle builder instances
    if (val instanceof ArrayBuilder) {
        return val.node;
    }
    else if (val instanceof NumberBuilder) {
        return val.node;
    }
    else if (val instanceof StringBuilder) {
        return val.node;
    }
    else if (val instanceof BooleanBuilder) {
        return val.node;
    }
    else if (val instanceof NullBuilder) {
        return val.node;
    }
    // Handle literals
    if (val === null) {
        return { type: 'null' };
    }
    else if (typeof val === 'number') {
        return { type: 'number', number: val };
    }
    else if (typeof val === 'string') {
        return { type: 'string', string: val };
    }
    else if (typeof val === 'boolean') {
        return { type: 'boolean', boolean: val };
    }
    // Handle Expr objects (have __brand or type property)
    if (typeof val === 'object' && val !== null && 'type' in val) {
        return val;
    }
    // Handle plain objects as record literals
    if (typeof val === 'object' && val !== null) {
        const fields = {};
        for (const [key, value] of Object.entries(val)) {
            fields[key] = valueToExpr(value);
        }
        return { type: 'record', fields };
    }
    throw new Error("Cannot convert value to expression");
}
function withRowBuilder(source, fn) {
    // Get the element type from the source array
    const sourceType = getType(source);
    if (sourceType.type !== "array") {
        throw new Error("withRowBuilder source must be an array");
    }
    const elementType = sourceType.el;
    // Create a row expression representing a single element
    const rowExpr = { type: 'row', source };
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
        return val.node;
    }
    // If it's already an Expr (has __brand), return it
    if (typeof val === 'object' && val !== null && '__brand' in val) {
        return val;
    }
    // Convert literal to Expr
    if (val === null) {
        return { type: 'null' };
    }
    else if (typeof val === 'number') {
        return { type: 'number', number: val };
    }
    else if (typeof val === 'string') {
        return { type: 'string', string: val };
    }
    else if (typeof val === 'boolean') {
        return { type: 'boolean', boolean: val };
    }
    else if (Array.isArray(val)) {
        return { type: 'array', array: val };
    }
    // Fallback
    return val;
}
class NumberBuilder {
    constructor(node) {
        this.node = node;
    }
    eq(value) {
        const right = value === null ? { type: 'null' } : toExpr(value);
        return new BooleanBuilder({ type: "eq", left: this.node, right });
    }
    gt(value) {
        const right = toExpr(value);
        return new BooleanBuilder({ type: "comparison_op", op: "gt", left: this.node, right });
    }
    lt(value) {
        const right = toExpr(value);
        return new BooleanBuilder({ type: "comparison_op", op: "lt", left: this.node, right });
    }
    minus(value) {
        const right = toExpr(value);
        return new NumberBuilder({ type: "math_op", op: "minus", left: this.node, right });
    }
    plus(value) {
        const right = toExpr(value);
        return new NumberBuilder({ type: "math_op", op: "plus", left: this.node, right });
    }
}
class StringBuilder {
    constructor(node) {
        this.node = node;
    }
    eq(value) {
        const right = value === null ? { type: 'null' } : toExpr(value);
        return new BooleanBuilder({ type: "eq", left: this.node, right });
    }
}
class BooleanBuilder {
    constructor(node) {
        this.node = node;
    }
    eq(value) {
        const right = value === null ? { type: 'null' } : toExpr(value);
        return new BooleanBuilder({ type: "eq", left: this.node, right });
    }
    and(value) {
        const right = toExpr(value);
        return new BooleanBuilder({ type: "logical_op", op: "and", left: this.node, right });
    }
    or(value) {
        const right = toExpr(value);
        return new BooleanBuilder({ type: "logical_op", op: "or", left: this.node, right });
    }
    not() {
        return new BooleanBuilder({ type: "not", expr: this.node });
    }
}
class NullBuilder {
    constructor(node) {
        this.node = node;
    }
    or(value) {
        return new BooleanBuilder({ type: "eq", left: this.node, right: toExpr(value) });
    }
}
function unreachable(val) {
    throw new Error(`Unexpected val: ${val}`);
}
exports.unreachable = unreachable;
function typeFromName(name) {
    switch (name) {
        case 'string': return { type: 'string' };
        case 'uuid': return { type: 'string' };
        case 'number': return { type: 'number' };
        case 'bool': return { type: 'bool' };
        case 'null': return { type: 'null' };
        default: return unreachable(name);
    }
}
function Table(name, schema) {
    const fields = {};
    for (const [key, typeName] of Object.entries(schema)) {
        fields[key] = typeFromName(typeName);
    }
    const recordType = { type: 'record', fields };
    const tableExpr = {
        type: 'table',
        name,
        schema: recordType
    };
    return new ArrayBuilder(tableExpr);
}
exports.Table = Table;
