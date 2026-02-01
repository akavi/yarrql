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

import { isNumber, isString, isBoolean, fromPairs } from "lodash"

// ===== Embedded Types =====
type NumberType = {__kind: "number" }
type StringType = {__kind: "string" }
type NullType = {__kind: "null"}
type BoolType = {__kind: "bool"}
type ScalarType = NumberType | StringType | NullType | BoolType
type ArrayType = {__kind: "array", __el: Type}
type RecordType = {__kind: "record", __fields: {[col: string]: Type}}
type Type = ScalarType | ArrayType | RecordType

type ArrayTypeOf<T extends Readonly<Type>> = { __kind: "array", __el: T}
type RecordTypeOf<T extends Record<string, Type>> = { __kind: "record", __fields: {[K in keyof T]: T[K]}}

// Export Schema and Query types for SQL generation
export type Schema = RecordType
export type Query<S extends Schema> = Expr<ArrayTypeOf<S>>

// ===== Expression Types =====
type AnyExpr<T extends Type> =
  | { __type: 'field'; __source: Expr<RecordTypeOf<Record<string, T>>>; __field: string }
  | { __type: 'first'; __source: Expr<ArrayTypeOf<T>> }
  | { __type: 'row'; __source: Expr<ArrayTypeOf<T>> }
  | { __type: 'record'; __fields: Record<string, Expr<any>> }

type ScalarWindowOperator = "max" | "min"
type ScalarExpr<T extends Type> = never
 | { __type: 'scalar_window'; __op: ScalarWindowOperator; __source: Expr<ArrayTypeOf<T>> }

type MathOperator = 'plus' | 'minus'
type NumberWindowOperator = "max" | "min" | "average"
type NumberExpr =
  | { __type: 'number'; __number: number }
  | { __type: "math_op", __op: MathOperator, __left: Expr<NumberType>, __right: Expr<NumberType> }
 | { __type: 'number_window'; __op: NumberWindowOperator; __source: Expr<ArrayTypeOf<NumberType>> }
  | { __type: 'count'; __source: Expr<ArrayType> }

type LogicalOperator = 'and' | 'or';
type ComparisonOperator = 'gt' | 'lt' | 'gte' | 'lte';
type BooleanExpr =
 | { __type: 'boolean'; __boolean: boolean }
 | { __type: 'not'; __expr: Expr<BoolType> }
 | { __type: "eq", __left: Expr<ScalarType>, __right: Expr<ScalarType> }
 | { __type: 'comparison_op'; __op: ComparisonOperator; __left: Expr<NumberType>; __right: Expr<NumberType> }
 | { __type: 'logical_op'; __op: LogicalOperator; __left: Expr<BoolType>, __right: Expr<BoolType> }

type StringExpr =  { __type: "string", __string: string }
type NullExpr = { __type: "null" }

type ArrayExpr<T extends Type> =  never
  | { __type: 'array'; __array: Array<any> }
  | { __type: 'table'; __name: string; __schema: T }
  | { __type: 'filter'; __source: Expr<ArrayTypeOf<T>>; __filter: Expr<BoolType> }
  | { __type: 'sort'; __source: Expr<ArrayTypeOf<T>>; __sort: Expr<Type> }
  | { __type: 'limit'; __source: Expr<ArrayTypeOf<T>>; __limit: Expr<NumberType> }
  | { __type: 'offset'; __source: Expr<ArrayTypeOf<T>>; __offset: Expr<NumberType> }
  | { __type: 'set_op'; __op: 'union' | 'intersect' | 'difference'; __left: Expr<ArrayTypeOf<T>>; __right: Expr<ArrayTypeOf<T>> }
  | { __type: 'map'; __source: Expr<ArrayTypeOf<Type>>, __map: Expr<T> }
  | { __type: 'flat_map'; __source: Expr<ArrayTypeOf<Type>>, __flatMap: Expr<ArrayTypeOf<T>> }
  | { __type: 'group_by'; __source: Expr<ArrayTypeOf<Type>>; __key: Expr<Type> };

export type Expr<T extends Type> = { __brand?: T } & (
  | (AnyExpr<T>)
  | (T extends ScalarType ? ScalarExpr<T> : never)
  | (T extends BoolType ? BooleanExpr : never)
  | (T extends NumberType ? NumberExpr : never)
  | (T extends StringType ? StringExpr : never)
  | (T extends NullType ? NullExpr : never)
  | (T extends ArrayTypeOf<infer ElemT> ? ElemT extends Type ? ArrayExpr<ElemT> : never : never)
)

// Helper to get __type from an expression
export function exprType(e: Expr<any>): string {
  return (e as any).__type;
}

type LiteralOf<T extends Type> = T extends { __kind: "null" }
   ? null
 : T extends { __kind: "string" }
   ? string
 : T extends { __kind: "bool" }
   ? boolean
 : T extends { __kind: "number" }
   ? number
 : T extends { __kind: "array", __el: Type}
    ? Array<LiteralOf<T["__el"]>>
  : T extends { __kind: "record", __fields: {[col: string]: Type}}
    ? {[K in keyof T["__fields"]]: LiteralOf<T["__fields"][K]>}
  : never

export type ExprBuilder<T extends Type> =  {__brand?: T} & (
  T extends NullType ?
     NullBuilder
  : T extends BoolType ?
     BooleanBuilder
  : T extends NumberType ?
    NumberBuilder
  : T extends StringType ?
    StringBuilder
  : T extends ArrayTypeOf<infer ElemT> ?
     ElemT extends Type
       ? ArrayBuilder<ElemT>
       : never
  : T extends RecordType ?
    { [Key in keyof T["__fields"]]: ExprBuilder<T["__fields"][Key]> }
  : never
)

class ArrayBuilder<T extends Type> {
  constructor(public __node: Expr<ArrayTypeOf<T>>) {}

  filter(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): ArrayBuilder<T> {
    const __filter = withRowBuilder(this.__node, fn)
    return new ArrayBuilder({ __type: "filter", __source: this.__node, __filter } as Expr<ArrayTypeOf<T>>)
  }

  sort(fn: (val: ExprBuilder<T>) => ValueOf<Type>): ArrayBuilder<T> {
    const __sort = withRowBuilder(this.__node, fn)
    return new ArrayBuilder({ __type: "sort", __source: this.__node, __sort } as Expr<ArrayTypeOf<T>>)
  }

  groupBy<K extends Type>(fn: (val: ExprBuilder<T>) => ValueOf<K>): ArrayBuilder<RecordTypeOf<{key: K, values: ArrayTypeOf<T>}>> {
    const __key = withRowBuilder(this.__node, fn)
    type ResultRecord = RecordTypeOf<{key: K, values: ArrayTypeOf<T>}>
    return new ArrayBuilder({ __type: "group_by", __source: this.__node, __key } as Expr<ArrayTypeOf<ResultRecord>>)
  }

  map<R>(fn: (val: ExprBuilder<T>) => R): ArrayBuilder<InferType<R>> {
    const __map = withRowBuilder(this.__node, fn as (val: ExprBuilder<T>) => ValueOf<any>)
    return new ArrayBuilder({ __type: "map", __source: this.__node, __map } as unknown as Expr<ArrayTypeOf<InferType<R>>>)
  }

  limit(value: ValueOf<NumberType>): ArrayBuilder<T> {
    const __limit = toExpr<NumberType>(value);
    return new ArrayBuilder({ __type: "limit", __source: this.__node, __limit } as Expr<ArrayTypeOf<T>>)
  }

  offset(value: ValueOf<NumberType>): ArrayBuilder<T> {
    const __offset = toExpr<NumberType>(value);
    return new ArrayBuilder({ __type: "offset", __source: this.__node, __offset } as Expr<ArrayTypeOf<T>>)
  }

  union(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T> {
    const __right = other instanceof ArrayBuilder ? other.__node : other;
    return new ArrayBuilder({__type: "set_op", __op: "union", __left: this.__node, __right } as Expr<ArrayTypeOf<T>>)
  }

  intersection(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T> {
    const __right = other instanceof ArrayBuilder ? other.__node : other;
    return new ArrayBuilder({__type: "set_op", __op: "intersect", __left: this.__node, __right } as Expr<ArrayTypeOf<T>>)
  }

  difference(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T> {
    const __right = other instanceof ArrayBuilder ? other.__node : other;
    return new ArrayBuilder({__type: "set_op", __op: "difference", __left: this.__node, __right } as Expr<ArrayTypeOf<T>>)
  }

  count(): NumberBuilder {
    const __node: Expr<NumberType> = {__type: "count", __source: this.__node as Expr<ArrayType> }
    return new NumberBuilder(__node)
  }

  average(): NumberBuilder {
    const type = getType(this.__node)
    if (type.__kind !== "array") {
      throw new Error("Cannot get average of non-array")
    } else if (type.__el.__kind !== "number") {
      throw new Error("Cannot get average of non-numeric-array")
    }
    return new NumberBuilder({ __type: 'number_window', __op: "average", __source: this.__node as Expr<ArrayTypeOf<NumberType>>});
  }

  max(): ExprBuilder<T> {
    const type = getType(this.__node)
    if (type.__kind !== "array") {
      throw new Error("Cannot get max of non-array")
    } else if (type.__el.__kind !== "number" && type.__el.__kind !== "string") {
      throw new Error("Cannot get max of non-numeric/string-array")
    }

    if (type.__el.__kind === "number") {
      return new NumberBuilder({ __type: 'scalar_window', __op: "max", __source: this.__node as Expr<ArrayTypeOf<NumberType>>}) as unknown as ExprBuilder<T>;
    } else if (type.__el.__kind === "string") {
      return new StringBuilder({ __type: 'scalar_window', __op: "max", __source: this.__node as Expr<ArrayTypeOf<StringType>>}) as unknown as ExprBuilder<T>;
    } else {
      throw new Error("Cannot get max of non-numeric/string-array")
    }
  }

  min(): ExprBuilder<T> {
    const type = getType(this.__node)
    if (type.__kind !== "array") {
      throw new Error("Cannot get min of non-array")
    } else if (type.__el.__kind !== "number" && type.__el.__kind !== "string") {
      throw new Error("Cannot get min of non-numeric/string-array")
    }

    if (type.__el.__kind === "number") {
      return new NumberBuilder({ __type: 'scalar_window', __op: "min", __source: this.__node as Expr<ArrayTypeOf<NumberType>>}) as unknown as ExprBuilder<T>;
    } else if (type.__el.__kind === "string") {
      return new StringBuilder({ __type: 'scalar_window', __op: "min", __source: this.__node as Expr<ArrayTypeOf<StringType>>}) as unknown as ExprBuilder<T>;
    } else {
      throw new Error("Cannot get min of non-numeric/string-array")
    }
  }

  first(): ExprBuilder<T> {
    const firstExpr: Expr<T> = { __type: 'first', __source: this.__node } as Expr<T>;
    const elemType = getType(this.__node);
    if (elemType.__kind !== "array") {
      throw new Error("Cannot get first of non-array");
    }
    return createBuilder(firstExpr, elemType.__el as T);
  }

  // helpers
  any(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): BooleanBuilder {
    return this.filter(fn).count().gt(0)
  }

  every(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): BooleanBuilder {
    return this.filter((val) => {
      const result = fn(val);
      // Convert to BooleanBuilder if needed, then negate
      const boolBuilder = result instanceof BooleanBuilder
        ? result
        : new BooleanBuilder(valueToExpr<BoolType>(result));
      return boolBuilder.not();
    }).count().eq(0)
  }
}

function getLiteralType(val: unknown): Type {
  if (val === null) {
    return {__kind: "null" }
  } else if (isNumber(val)) {
    return {__kind: "number"}
  } else if (isString(val)) {
    return {__kind: "string" }
  } else if (isBoolean(val)) {
    return {__kind: "bool" }
  } else if (val instanceof Array) {
    return {__kind: "array", __el: getLiteralType(val[0]) }
  } else if (val instanceof Object) {
    const __fields = fromPairs(Object.entries(val).map(([k, v]) => [k, getLiteralType(v)])) as Record<string, Type>
    return { __kind: "record", __fields }
  } else {
    throw new Error("Wat")
  }
}

function getType<T extends Type>(expr: Expr<T>): T {
  const t = (expr as any).__type;
  if (t === "field") {
    const recordType = getType((expr as any).__source as Expr<RecordType>) as RecordType;
    return recordType.__fields[(expr as any).__field] as T
  } else if (t === "first") {
    const arrayType = getType((expr as any).__source as Expr<ArrayType>) as ArrayType
    return arrayType.__el as T
  } else if (t === "row") {
    const arrayType = getType((expr as any).__source as Expr<ArrayType>) as ArrayType
    return arrayType.__el as T
  } else if (t === "scalar_window") {
    const arrayType = getType((expr as any).__source as Expr<ArrayType>) as ArrayType
    return arrayType.__el as T
  }  else if (t === "number") {
    return {__kind: "number" } as T
  } else if (t === "math_op") {
    return {__kind: "number" } as T
  } else if (t === "number_window") {
    return {__kind: "number" } as T
  } else if (t === "count") {
    return { __kind: "number" } as T
  } else if (t === "boolean") {
    return { __kind: "bool" } as T;
  } else if (t === "not") {
    return { __kind: "bool" } as T;
  } else if (t === "eq") {
    return { __kind: "bool" } as T;
  } else if (t === "comparison_op") {
    return { __kind: "bool" } as T;
  } else if (t === "logical_op") {
    return { __kind: "bool" } as T;
  } else if (t === "array") {
    // TODO: support unknown arrays;
    return getLiteralType((expr as any).__array) as T;
  } else if (t === "table") {
    return { __kind: "array", __el: (expr as any).__schema } as T;
  } else if (t === "filter") {
    return getType((expr as any).__source as Expr<T>)
  } else if (t === "sort") {
    return getType((expr as any).__source as Expr<T>);
  } else if (t === "limit") {
    return getType((expr as any).__source as Expr<T>);
  } else if (t === "offset") {
    return getType((expr as any).__source as Expr<T>);
  } else if (t === "set_op") {
    return getType((expr as any).__right as Expr<T>);
  } else  if (t === "map") {
    const elType = getType((expr as any).__map as Expr<Type>)
    return {__kind: "array", __el: elType} as T
  } else if (t === "flat_map") {
    return getType((expr as any).__flatMap as Expr<T>)
  } else if (t === "group_by") {
    const valType = getType((expr as any).__source as Expr<Type>)
    const keyType = getType((expr as any).__key as Expr<Type>)
    return {__kind: "record", __fields: {vals: valType, key: keyType}} as unknown as T
  } else if (t === "string") {
    return {__kind: "string"} as T
  } else if (t === "null") {
    return {__kind: "null" } as T
  } else if (t === "record") {
    const __fields: Record<string, Type> = {}
    for (const [key, fieldExpr] of Object.entries((expr as any).__fields)) {
      __fields[key] = getType(fieldExpr as Expr<any>)
    }
    return { __kind: "record", __fields } as T
  } else {
    throw new Error(`Unknown expr type: ${t}`)
  }
}

function createBuilder<T extends Type>(expr: Expr<T>, type: T): ExprBuilder<T> {
  if (type.__kind === "null") {
    return new NullBuilder(expr as Expr<NullType>) as unknown as ExprBuilder<T>;
  } else if (type.__kind === "bool") {
    return new BooleanBuilder(expr as Expr<BoolType>) as unknown as ExprBuilder<T>;
  } else if (type.__kind === "number") {
    return new NumberBuilder(expr as Expr<NumberType>) as unknown as ExprBuilder<T>;
  } else if (type.__kind === "string") {
    return new StringBuilder(expr as Expr<StringType>) as unknown as ExprBuilder<T>;
  } else if (type.__kind === "array") {
    return new ArrayBuilder(expr as Expr<ArrayTypeOf<Type>>) as unknown as ExprBuilder<T>;
  } else if (type.__kind === "record") {
    const result: Record<string, ExprBuilder<Type>> = {};
    for (const [fieldName, fieldType] of Object.entries(type.__fields)) {
      const fieldExpr: Expr<Type> = {
        __type: 'field',
        __source: expr as Expr<RecordTypeOf<Record<string, Type>>>,
        __field: fieldName
      } as Expr<Type>;
      result[fieldName] = createBuilder(fieldExpr, fieldType);
    }
    return result as unknown as ExprBuilder<T>;
  }
  throw new Error(`Unknown type: ${(type as Type).__kind}`);
}

// Converts any ValueOf<T> (builder, expr, literal, or record object) to an Expr<T>
function valueToExpr<T extends Type>(val: ValueOf<T>): Expr<T> {
  // Handle builder instances
  if (val instanceof ArrayBuilder) {
    return val.__node as Expr<T>;
  } else if (val instanceof NumberBuilder) {
    return val.__node as Expr<T>;
  } else if (val instanceof StringBuilder) {
    return val.__node as Expr<T>;
  } else if (val instanceof BooleanBuilder) {
    return val.__node as Expr<T>;
  } else if (val instanceof NullBuilder) {
    return val.__node as Expr<T>;
  }

  // Handle literals
  if (val === null) {
    return { __type: 'null' } as Expr<T>;
  } else if (typeof val === 'number') {
    return { __type: 'number', __number: val } as Expr<T>;
  } else if (typeof val === 'string') {
    return { __type: 'string', __string: val } as Expr<T>;
  } else if (typeof val === 'boolean') {
    return { __type: 'boolean', __boolean: val } as Expr<T>;
  }

  // Handle Expr objects (have __brand or __type property)
  if (typeof val === 'object' && val !== null && '__type' in val) {
    return val as Expr<T>;
  }

  // Handle plain objects as record literals
  if (typeof val === 'object' && val !== null) {
    const __fields: Record<string, Expr<any>> = {};
    for (const [key, value] of Object.entries(val)) {
      __fields[key] = valueToExpr(value as ValueOf<any>);
    }
    return { __type: 'record', __fields } as Expr<T>;
  }

  throw new Error("Cannot convert value to expression");
}

function withRowBuilder<A extends Type, B extends Type>(source: Expr<ArrayTypeOf<A>>, fn: (builder: ExprBuilder<A>) => ValueOf<B>): Expr<B> {
  // Get the element type from the source array
  const sourceType = getType(source);
  if (sourceType.__kind !== "array") {
    throw new Error("withRowBuilder source must be an array");
  }
  const elementType = sourceType.__el;

  // Create a row expression representing a single element
  const rowExpr: Expr<A> = { __type: 'row', __source: source } as Expr<A>;

  // Create the appropriate builder based on element type
  const rowBuilder = createBuilder(rowExpr as Expr<Type>, elementType) as ExprBuilder<A>;

  // Call the callback and convert result to expression
  const result = fn(rowBuilder);
  return valueToExpr(result);
}

function toExpr<T extends Type>(val: ValueOf<T>): Expr<T> {
  // Check for builder instances first (they don't have __brand)
  if (val instanceof ArrayBuilder || val instanceof NumberBuilder ||
      val instanceof StringBuilder || val instanceof BooleanBuilder ||
      val instanceof NullBuilder) {
    return val.__node as Expr<T>
  }

  // If it's already an Expr (has __brand or __type), return it
  if (typeof val === 'object' && val !== null && ('__brand' in val || '__type' in val)) {
    return val as Expr<T>
  }

  // Convert literal to Expr
  if (val === null) {
    return { __type: 'null' } as Expr<T>
  } else if (typeof val === 'number') {
    return { __type: 'number', __number: val } as Expr<T>
  } else if (typeof val === 'string') {
    return { __type: 'string', __string: val } as Expr<T>
  } else if (typeof val === 'boolean') {
    return { __type: 'boolean', __boolean: val } as Expr<T>
  } else if (Array.isArray(val)) {
    return { __type: 'array', __array: val } as Expr<T>
  }

  // Fallback
  return val as Expr<T>
}

// Base value types that can be converted to expressions
type BuilderValue = ArrayBuilder<any> | NumberBuilder | StringBuilder | BooleanBuilder | NullBuilder
type RecordObject = { [key: string]: BuilderValue | RecordObject | Expr<any> | string | number | boolean | null }

type ValueOf<T extends Type> =
  | LiteralOf<T>
  | Expr<T>
  | ExprBuilder<T>
  | RecordObject

// Infer the Type from a value (builder, expr, literal, or record object)
type InferType<V> =
  V extends ArrayBuilder<infer E> ? ArrayTypeOf<E> :
  V extends NumberBuilder ? NumberType :
  V extends StringBuilder ? StringType :
  V extends BooleanBuilder ? BoolType :
  V extends NullBuilder ? NullType :
  V extends Expr<infer T> ? T :
  V extends string ? StringType :
  V extends number ? NumberType :
  V extends boolean ? BoolType :
  V extends null ? NullType :
  V extends { [key: string]: any } ? RecordTypeOf<{ [K in keyof V]: InferType<V[K]> }> :
  Type


class NumberBuilder {
  constructor(public __node: Expr<NumberType>) {}

  eq(value: ValueOf<NumberType> | null): BooleanBuilder {
    const __right = value === null ? { __type: 'null' } as Expr<NullType> : toExpr<NumberType>(value);
    return new BooleanBuilder({__type: "eq", __left: this.__node, __right} as Expr<BoolType>)
  }

  gt(value: ValueOf<NumberType>): BooleanBuilder {
    const __right = toExpr<NumberType>(value)
    return new BooleanBuilder({__type: "comparison_op", __op: "gt", __left: this.__node, __right} as Expr<BoolType>)
  }

  lt(value: ValueOf<NumberType>): BooleanBuilder {
    const __right = toExpr<NumberType>(value)
    return new BooleanBuilder({__type: "comparison_op", __op: "lt", __left: this.__node, __right} as Expr<BoolType>)
  }

  minus(value: ValueOf<NumberType>): NumberBuilder {
    const __right = toExpr<NumberType>(value)
    return new NumberBuilder({__type: "math_op", __op: "minus", __left: this.__node, __right} as Expr<NumberType>)
  }

  plus(value: ValueOf<NumberType>): NumberBuilder {
    const __right = toExpr<NumberType>(value)
    return new NumberBuilder({__type: "math_op", __op: "plus", __left: this.__node, __right} as Expr<NumberType>)
  }
}

class StringBuilder {
  constructor(public __node: Expr<StringType>) {}

  eq(value: ValueOf<StringType> | null): BooleanBuilder {
    const __right = value === null ? { __type: 'null' } as Expr<NullType> : toExpr<StringType>(value);
    return new BooleanBuilder({__type: "eq", __left: this.__node, __right} as Expr<BoolType>)
  }
}

class BooleanBuilder {
  constructor(public __node: Expr<BoolType>) {}

  eq(value: ValueOf<BoolType> | null): BooleanBuilder {
    const __right = value === null ? { __type: 'null' } as Expr<NullType> : toExpr<BoolType>(value);
    return new BooleanBuilder({__type: "eq", __left: this.__node, __right} as Expr<BoolType>)
  }

  and(value: ValueOf<BoolType>): BooleanBuilder {
    const __right = toExpr<BoolType>(value)
    return new BooleanBuilder({__type: "logical_op", __op: "and", __left: this.__node, __right} as Expr<BoolType>)
  }

  or(value: ValueOf<BoolType>): BooleanBuilder {
    const __right = toExpr<BoolType>(value)
    return new BooleanBuilder({__type: "logical_op", __op: "or", __left: this.__node, __right} as Expr<BoolType>)
  }

  not(): BooleanBuilder {
    return new BooleanBuilder({__type: "not", __expr: this.__node} as Expr<BoolType>)
  }
}

class NullBuilder {
  constructor(public __node: Expr<NullType>) {}

  or(value: ValueOf<NullType>): BooleanBuilder {
    return new BooleanBuilder({__type: "eq", __left: this.__node, __right: toExpr(value)} as Expr<BoolType>)
  }
}

export function unreachable(val: never): never {
  throw new Error(`Unexpected val: ${val}`)
}

// --- Table Constructor ---

type TypeName = 'string' | 'number' | 'bool' | 'null' | 'uuid'
type SchemaSpec = Record<string, TypeName>

type TypeFromName<N extends TypeName> =
  N extends 'string' ? StringType :
  N extends 'uuid' ? StringType :
  N extends 'number' ? NumberType :
  N extends 'bool' ? BoolType :
  N extends 'null' ? NullType :
  never

type SchemaFromSpec<S extends SchemaSpec> = RecordTypeOf<{
  [K in keyof S]: TypeFromName<S[K]>
}>

function typeFromName(name: TypeName): Type {
  switch (name) {
    case 'string': return { __kind: 'string' }
    case 'uuid': return { __kind: 'string' }
    case 'number': return { __kind: 'number' }
    case 'bool': return { __kind: 'bool' }
    case 'null': return { __kind: 'null' }
    default: return unreachable(name)
  }
}

export function Table<S extends SchemaSpec>(name: string, schema: S): ArrayBuilder<SchemaFromSpec<S>> {
  const __fields: Record<string, Type> = {}
  for (const [key, typeName] of Object.entries(schema)) {
    __fields[key] = typeFromName(typeName as TypeName)
  }
  const recordType: RecordType = { __kind: 'record', __fields }
  const tableExpr: Expr<ArrayTypeOf<SchemaFromSpec<S>>> = {
    __type: 'table',
    __name: name,
    __schema: recordType
  } as Expr<ArrayTypeOf<SchemaFromSpec<S>>>

  return new ArrayBuilder(tableExpr)
}
