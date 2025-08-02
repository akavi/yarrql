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
type NumberType = {type: "number" }
type StringType = {type: "string" }
type NullType = {type: "null"}
type BoolType = {type: "bool"}
type ScalarType = NumberType | StringType | NullType | BoolType
type ArrayType = {type: "array", el: Type}
type RecordType = {type: "record", fields: {[col: string]: Type}}
type Type = ScalarType | ArrayType | RecordType

type ArrayTypeOf<T extends Readonly<Type>> = { type: "array", el: T}
type RecordTypeOf<T extends Record<string, Type>> = { type: "record", fields: {[K in keyof T]: T[K]}}

// ===== Expression Types =====
type AnyExpr<T extends Type> = 
  | { type: 'field'; source: Expr<RecordTypeOf<Record<string, T>>>; field: string }
  | { type: 'first'; source: Expr<ArrayTypeOf<T>> }
  | { type: 'row'; source: Expr<ArrayTypeOf<T>> }

type ScalarWindowOperator = "max" | "min"
type ScalarExpr<T extends Type> = never
 | { type: 'scalar_window'; op: ScalarWindowOperator; source: Expr<ArrayTypeOf<T>> }

type MathOperator = 'plus' | 'minus'
type NumberWindowOperator = "max" | "min" | "average"
type NumberExpr = 
  | { type: 'number'; number: number }
  | { type: "math_op", op: MathOperator, left: Expr<NumberType>, right: Expr<NumberType> }
 | { type: 'number_window'; op: NumberWindowOperator; source: Expr<ArrayTypeOf<NumberType>> }
  | { type: 'count'; source: Expr<ArrayType> }

type LogicalOperator = 'and' | 'or';
type ComparisonOperator = 'gt' | 'lt' | 'gte' | 'lte';
type BooleanExpr =
 | { type: 'boolean'; boolean: boolean }
 | { type: 'not'; expr: Expr<BoolType> }
 | { type: "eq", left: Expr<ScalarType>, right: Expr<ScalarType> }
 | { type: 'comparison_op'; op: ComparisonOperator; left: Expr<NumberType>; right: Expr<NumberType> }
 | { type: 'logical_op'; op: LogicalOperator; left: Expr<BoolType>, right: Expr<BoolType> }

type StringExpr =  { type: "string", string: string }
type NullExpr = { type: "null" }

type ArrayExpr<T extends Type> =  never
  | { type: 'array'; array: Array<any> }
  | { type: 'table'; name: string; schema: T }
  | { type: 'filter'; source: Expr<ArrayTypeOf<T>>; filter: Expr<BoolType> }
  | { type: 'sort'; source: Expr<ArrayTypeOf<T>>; sort: Expr<Type> }
  | { type: 'limit'; source: Expr<ArrayTypeOf<T>>; limit: number }
  | { type: 'offset'; source: Expr<ArrayTypeOf<T>>; offset: number }
  | { type: 'set_op'; op: 'union' | 'intersect' | 'difference'; left: Expr<ArrayTypeOf<T>>; right: Expr<ArrayTypeOf<T>> }
  | { type: 'map'; source: Expr<ArrayTypeOf<Type>>, map: Expr<T> }
  | { type: 'flat_map'; source: Expr<ArrayTypeOf<Type>>, flatMap: Expr<ArrayTypeOf<T>> }
  | { type: 'group_by'; source: Expr<ArrayTypeOf<Type>>; key: Expr<Type> };

export type Expr<T extends Type> = { __brand?: T } & (
  | (AnyExpr<T>)
  | (T extends ScalarType ? ScalarExpr<T> : never)
  | (T extends BoolType ? BooleanExpr : never)
  | (T extends NumberType ? NumberExpr : never)
  | (T extends StringType ? StringExpr : never)
  | (T extends NullType ? NullExpr : never)
  | (T extends ArrayTypeOf<infer ElemT> ? ElemT extends Type ? ArrayExpr<ElemT> : never : never)
)

type LiteralOf<T extends Type> = T extends { type: "null" }
   ? null
 : T extends { type: "string" }
   ? string
 : T extends { type: "bool" }
   ? boolean
 : T extends { type: "number" }
   ? number
 : T extends { type: "array", el: Type}
    ? Array<LiteralOf<T["el"]>>
  : T extends { type: "record", fields: {[col: string]: Type}}
    ? {[K in keyof T["fields"]]: LiteralOf<T["fields"][K]>}
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
    { [Key in keyof T["fields"]]: ExprBuilder<T["fields"][Key]> }
  : never
)

class ArrayBuilder<T extends Type> {
  constructor(public node: Expr<ArrayTypeOf<T>>) {}

  filter(fn: (val: ExprBuilder<T>) => ExprBuilder<BoolType>): ArrayBuilder<T> {
    const filter = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "filter", source: this.node, filter })
  }

  sort(fn: (val: ExprBuilder<T>) => ExprBuilder<Type>): ArrayBuilder<T> {
    const sort = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "sort", source: this.node, sort })
  }
  
  groupBy<K extends Type>(fn: (val: ExprBuilder<T>) => ExprBuilder<K>): ArrayBuilder<RecordTypeOf<{key: K, values: ArrayTypeOf<T>}>> {
    const key = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "group_by", source: this.node, key })
  }

  map<R extends Type>(fn: (val: ExprBuilder<T>) => ExprBuilder<R>): ArrayBuilder<R> {
    const map = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "map", source: this.node, map })
  }

  limit(value: ValueOf<NumberType>): ArrayBuilder<T> {
    const limit = toExpr<NumberType>(value);
    return new ArrayBuilder({ type: "limit", source: this.node, limit})
  }

  offset(value: number): ArrayBuilder<T> {
    const offset = toExpr<NumberType>(value);
    return new ArrayBuilder({ type: "offset", source: this.node, offset})
  }

  union(other: ArrayBuilder<T>): ArrayBuilder<T> {
    const right = toExpr<ArrayTypeOf<T>>(other);
    return new ArrayBuilder({type: "set_op", op: "union", left: this.node, right })
  }

  intersection(other: ArrayBuilder<T>): ArrayBuilder<T> {
    const right = toExpr<ArrayTypeOf<T>>(other);
    return new ArrayBuilder({type: "set_op", op: "intersect", left: this.node, right })
  }

  difference(other: ArrayBuilder<T>): ArrayBuilder<T> {
    const right = toExpr<ArrayTypeOf<T>>(other);
    return new ArrayBuilder({type: "set_op", op: "difference", left: this.node, right })
  }

  count(): NumberBuilder {
    const node: Expr<NumberType> = {type: "count", source: this.node }
    return new NumberBuilder(node)
  }

  average(): NumberBuilder {
    const type = getType(this.node)
    if (type.type !== "array") {
      throw new Error("Cannot get average of non-array")
    } else if (type.el.type !== "number") {
      throw new Error("Cannot get average of non-numeric-array")
    }
    return new NumberBuilder({ type: 'number_window', op: "average", source: this.node as any});
  }

  max(): ExprBuilder<T> {
    const type = getType(this.node)
    if (type.type !== "array") {
      throw new Error("Cannot get max of non-array")
    } else if (type.el.type !== "number" && type.el.type !== "string") {
      throw new Error("Cannot get average of non-numeric/string-array")
    }

    if (type.el.type === "number") {
      return new NumberBuilder({ type: 'scalar_window', op: "max", source: this.node as any}) as any;
    } else if (type.el.type === "string") {
      return new StringBuilder({ type: 'scalar_window', op: "max", source: this.node as any}) as any;
    } else {
      throw new Error("Cannot get max of non-numeric/string-array")
    }
  }

  min(): ExprBuilder<T> {
    const type = getType(this.node)
    if (type.type !== "array") {
      throw new Error("Cannot get min of non-array")
    } else if (type.el.type !== "number" && type.el.type !== "string") {
      throw new Error("Cannot get average of non-numeric/string-array")
    }

    if (type.el.type === "number") {
      return new NumberBuilder({ type: 'scalar_window', op: "min", source: this.node as any}) as any;
    } else if (type.el.type === "string") {
      return new StringBuilder({ type: 'scalar_window', op: "min", source: this.node as any}) as any;
    } else {
      throw new Error("Cannot get min of non-numeric/string-array")
    }
  }

  // helpers
  any(fn: (val: ExprBuilder<T>) => ExprBuilder<BoolType>): BooleanBuilder {
    return this.filter(fn).count().gt(0)
  }

  every(fn: (val: ExprBuilder<T>) => ExprBuilder<BoolType>): BooleanBuilder {
    return this.filter((val) => fn(val).not()).count().eq(0)
  }
}

function getLiteralType(val: unknown): Type {
  if (val === null) {
    return {type: "null" }
  } else if (isNumber(val)) {
    return {type: "number"}
  } else if (isString(val)) {
    return {type: "string" }
  } else if (isBoolean(val)) {
    return {type: "bool" }
  } else if (val instanceof Array) {
    return {type: "array", el: getLiteralType(val[0]) }
  } else if (val instanceof Object) {
    const fields = fromPairs(Object.entries(val).map(([k, v]) => [k, getLiteralType(v)])) as any
    return { type: "record", fields }
  } else {
    throw new Error("Wat")
  }
}

function getType<T extends Type>(expr: Expr<T>): T {
  if (expr.type === "field") {
    const recordType = getType(expr.source) as any;
    return recordType.fields[expr.field]
  } else if (expr.type === "first") {
    const arrayType = getType(expr.source)
    return (arrayType.el as any)
  } else if (expr.type === "row") {
    const arrayType = getType(expr.source)
    return (arrayType.el as any)
  } else if (expr.type === "scalar_window") {
    const arrayType = getType(expr.source) as any
    return (arrayType.el as any)
  }  else if (expr.type === "number") {
    return {type: "number" } as any
  } else if (expr.type === "math_op") {
    return {type: "number" } as any
  } else if (expr.type === "number_window") {
    return {type: "number" } as any
  } else if (expr.type === "count") {
    return { type: "number" } as any
  } else if (expr.type === "boolean") {
    return { type: "bool" } as Type as any;
  } else if (expr.type === "not") {
    return { type: "bool" } as any;
  } else if (expr.type === "eq") {
    return { type: "bool" } as any;
  } else if (expr.type === "comparison_op") {
    return { type: "bool" } as any;
  } else if (expr.type === "logical_op") {
    return { type: "bool" } as any;
  } else if (expr.type === "array") {
    // TODO: support unknown arrays;
    return getLiteralType(expr.array) as any;
  } else if (expr.type === "table") {
    return expr.schema as any;
  } else if (expr.type === "filter") {
    return getType(expr.source) as any
  } else if (expr.type === "sort") {
    return getType(expr.source) as any;
  } else if (expr.type === "limit") {
    return getType(expr.source) as any;
  } else if (expr.type === "offset") {
    return getType(expr.source) as any;
  } else if (expr.type === "set_op") {
    return getType(expr.right) as any;
  } else  if (expr.type === "map") {
    const elType = getType(expr.map)
    return {type: "array", el: elType} as any
  } else if (expr.type === "flat_map") {
    return getType(expr.flatMap) as any
  } else if (expr.type === "group_by") {
    const valType = getType(expr.source)
    const keyType = getType(expr.key)
    return {type: "record", fields: {vals: valType, key: keyType}} as any
  } else if (expr.type === "string") {
    return {type: "string"} as any
  } else if (expr.type === "null") {
    return {type: "null" } as any
  } else {
    return unreachable(expr)
  }
}

function withRowBuilder<A extends Type, B extends Type>(source: Expr<ArrayTypeOf<A>>, fn: (builder: ExprBuilder<A>) => ExprBuilder<B>): Expr<B> {
  // TODO
  return source as any;
}

function toExpr<T extends Type>(val: ValueOf<T>): Expr<T> {
  // TODO
  return val as any
}

type ValueOf<T extends Type> = LiteralOf<T> | Expr<T> | ExprBuilder<T>


class NumberBuilder {
  constructor(public node: Expr<NumberType>) {}

  eq(value: ValueOf<NumberType>): BooleanBuilder {
    const right = toExpr<NumberType>(value);
    return new BooleanBuilder({type: "eq", left: this.node, right})
  }

  gt(value: ValueOf<NumberType>): BooleanBuilder {
    const right = toExpr<NumberType>(value)
    return new BooleanBuilder({type: "comparison_op", op: "gt", left: this.node, right})
  }

  lt(value: ValueOf<NumberType>): BooleanBuilder {
    const right = toExpr<NumberType>(value)
    return new BooleanBuilder({type: "comparison_op", op: "lt", left: this.node, right})
  }

  minus(value: ValueOf<NumberType>): NumberBuilder {
    const right = toExpr<NumberType>(value)
    return new NumberBuilder({type: "math_op", op: "minus", left: this.node, right})
  }

  plus(value: ValueOf<NumberType>): NumberBuilder {
    const right = toExpr<NumberType>(value)
    return new NumberBuilder({type: "math_op", op: "plus", left: this.node, right})
  }
}

class StringBuilder {
  constructor(public node: Expr<StringType>) {}

  eq(value: ValueOf<StringType>): BooleanBuilder {
    const right = toExpr<StringType>(value);
    return new BooleanBuilder({type: "eq", left: this.node, right})
  }
}

class BooleanBuilder {
  constructor(public node: Expr<BoolType>) {}

  eq(value: ValueOf<BoolType>): BooleanBuilder {
    const right = toExpr<BoolType>(value);
    return new BooleanBuilder({type: "eq", left: this.node, right})
  }

  and(value: ValueOf<BoolType>): BooleanBuilder {
    const right = toExpr<BoolType>(value)
    return new BooleanBuilder({type: "logical_op", op: "and", left: this.node, right})
  }

  or(value: ValueOf<BoolType>): BooleanBuilder {
    const right = toExpr<BoolType>(value)
    return new BooleanBuilder({type: "logical_op", op: "or", left: this.node, right})
  }

  not(): BooleanBuilder {
    return new BooleanBuilder({type: "not", expr: this.node})
  }
}

class NullBuilder {
  constructor(public node: Expr<NullType>) {}

  or(value: ValueOf<NullType>): BooleanBuilder {
    return new BooleanBuilder({type: "eq", left: this.node, right: toExpr(value)})
  }
}

function unreachable(val: never): never {
  throw new Error(`Unexpected val: ${val}`)
}
