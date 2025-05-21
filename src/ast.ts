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

import { mapValues } from "lodash"

// ===== Embedded Types =====
type ScalarType = boolean | number | string | null
type Type = ScalarType | {[col: string]: Type} | Array<Type>

// ===== Expression Types =====
type AnyExpr<T extends Type> = 
  | { type: 'literal'; literal: T }
  | { type: 'field'; source: Expr<{[col: string]: T}>; field: string }
  | { type: 'first'; source: Expr<Array<T>> }
  | { type: 'row'; source: Expr<Array<T>> }

type ScalarWindowOperator = "max" | "min"
type ScalarExpr<T extends Type> = 
  | { type: 'scalar_window'; op: ScalarWindowOperator; source: Expr<Array<T>> }

type MathOperator = 'plus' | 'minus'
type NumberWindowOperator = "max" | "min"
type NumberExpr = 
  | { type: "math_op", op: MathOperator, left: Expr<number>, right: Expr<number> }
  | { type: 'number_window'; op: NumberWindowOperator; source: Expr<Array<number>> }

type LogicalOperator = 'and' | 'or';
type ComparisonOperator = 'gt' | 'lt' | 'gte' | 'lte';
type BooleanExpr = 
  | { type: 'not'; expr: Expr<boolean> }
  | { type: "eq", left: Expr<Type>, right: Expr<Type> }
  | { type: 'comparison_op'; op: ComparisonOperator; left: Expr<number>; right: Expr<number> }
  | { type: 'logical_op'; op: LogicalOperator; left: Expr<boolean>, right: Expr<boolean> }

type ArrayExpr<T extends Type> = 
  | { type: 'table'; name: string; schema: Schema<T> }
  | { type: 'filter'; source: Expr<Array<T>>; filter: Expr<boolean> }
  | { type: 'sort'; source: Expr<Array<T>>; sort: Expr<Type> }
  | { type: 'limit'; source: Expr<Array<T>>; limit: number }
  | { type: 'offset'; source: Expr<Array<T>>; offset: number }
  | { type: 'set_op'; op: 'union' | 'intersect' | 'difference'; left: Expr<Array<T>>; right: Expr<Array<T>> }
  | { type: 'map'; source: Expr<Array<Type>>, map: Expr<T> }
  | { type: 'flat_map'; source: Expr<Array<Type>>, flatMap: Expr<Array<T>> }
  | { type: 'group_by'; source: Expr<Array<Type>>; key: Expr<Type> };

export type Expr<T extends Type> = { __elemType?: T } & (
  | (T extends Type ? AnyExpr<T>: never)
  | (T extends ScalarType ? ScalarExpr<T> : never)
  | (T extends boolean ? BooleanExpr : never)
  | (T extends number ? NumberExpr : never)
  | (T extends Array<infer ElemT> ? ElemT extends Type ? ArrayExpr<ElemT> : never : never)
)

type Schema<T> = T extends null 
  ? {type: "null"}
  : T extends string
  ? {type: "string"}
  : T extends number
  ? {type: "number"}
  : T extends Array<infer ElemT>
  ? {type: "array", el: Schema<ElemT>}
  : T extends Record<string, unknown>
  ? {type: "record", schema: { [K in keyof T]: Schema<T[K]>}}
  : never;

export type ExprBuilder<T extends Type> =  {__brand?: T} & (
  T extends null ?
     NullBuilder
  : T extends boolean ?
     BooleanBuilder
  : T extends number ?
    NumberBuilder
  : T extends string ?
    StringBuilder
  : T extends Array<infer ElemT> ?
     ElemT extends Type 
       ? ArrayBuilder<ElemT>
       : never
  : T extends Record<string, Type> ?
    { [Key in keyof T]: ExprBuilder<T[Key]> }
  : never
)

class ArrayBuilder<T extends Type> {
  constructor(public node: Expr<Array<T>>) {}

  map<R extends Type>(fn: (val: ExprBuilder<T>) => ExprBuilder<R>): ArrayBuilder<R> {
    const map = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "map", source: this.node, map })
  }

  filter(fn: (val: ExprBuilder<T>) => ExprBuilder<boolean>): ArrayBuilder<T> {
    const filter = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "filter", source: this.node, filter })
  }

  sort(fn: (val: ExprBuilder<T>) => ExprBuilder<Type>): ArrayBuilder<T> {
    const sort = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "sort", source: this.node, sort })
  }

  limit(fn: (val: ExprBuilder<T>) => ExprBuilder<Type>): ArrayBuilder<T> {
    const sort = withRowBuilder(this.node, fn)
    return new ArrayBuilder({ type: "sort", source: this.node, sort })
  }
}

function withRowBuilder<A extends Type, B extends Type>(source: Expr<Array<A>>, fn: (builder: ExprBuilder<A>) => ExprBuilder<B>): Expr<B> {
  const row: Expr<A> = { type: "row", source }
  // TODO
  return row as any;
}

function toExpr<T extends Type>(val: T | Expr<T>) {
  // TODO
  return val as any
}

class NumberBuilder {
  constructor(public node: Expr<number>) {}

  eq(value: Expr<Type> | Type): BooleanBuilder {
    return new BooleanBuilder({type: "eq", left: this.node, right: toExpr(value)})
  }

  gt(value: Expr<number> | number): BooleanBuilder {
    return new BooleanBuilder({type: "comparison_op", op: "gt", left: this.node, right: toExpr(value)})
  }

  lt(value: Expr<number> | number): BooleanBuilder {
    return new BooleanBuilder({type: "comparison_op", op: "lt", left: this.node, right: toExpr(value)})
  }

  minus(value: Expr<number> | number): NumberBuilder {
    return new NumberBuilder({type: "math_op", op: "plus", left: this.node, right: toExpr(value)})
  }

  plus(value: Expr<number> | number): NumberBuilder {
    return new NumberBuilder({type: "math_op", op: "minus", left: this.node, right: toExpr(value)})
  }
}

class StringBuilder {
  constructor(public node: Expr<string>) {}

  eq(value: Expr<string> | string): BooleanBuilder {
    return new BooleanBuilder({type: "eq", left: this.node, right: toExpr(value)})
  }
}

class BooleanBuilder {
  constructor(public node: Expr<boolean>) {}

  eq(value: Expr<Type> | Type): BooleanBuilder {
    return new BooleanBuilder({type: "eq", left: this.node, right: toExpr(value)})
  }

  and(value: Expr<boolean> | boolean): BooleanBuilder {
    return new BooleanBuilder({type: "logical_op", op: "and", left: this.node, right: toExpr(value)})
  }

  or(value: Expr<boolean> | boolean): BooleanBuilder {
    return new BooleanBuilder({type: "logical_op", op: "or", left: this.node, right: toExpr(value)})
  }

  not(): BooleanBuilder {
    return new BooleanBuilder({type: "not", expr: this.node})
  }
}

class NullBuilder {
  constructor(public node: Expr<boolean>) {}

  eq(value: Expr<Type> | Type): BooleanBuilder {
    return new BooleanBuilder({type: "eq", left: this.node, right: toExpr(value)})
  }
}

type InferSchema<S extends Schema<any>> = S extends {type: "null"}
  ? null
  : S extends { type: "string" }
  ? string
  : S extends { type: "number" }
  ? number
  : S extends { type: "record", schema: infer S }
     ? S extends {[key: string]: Schema<any>}
      ? {[Key in keyof S]: InferSchema<S[Key]> }
      : never
  : S extends { type: "array", el: infer S}
     ? S extends Schema<any>
      ? InferSchema<S>
      : never
  : never

export function Table<S extends Schema<any>>(name: string, schema: S): ArrayBuilder<InferSchema<S>> {
  return new ArrayBuilder({type: "table", name, schema})
}
