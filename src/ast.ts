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

// ===== Core Scalar Types =====
type Scalar = string | number | boolean | null;

// ===== Expression Types =====
type ComparisonOperator = 'ne' | 'gt' | 'lt' | 'gte' | 'lte';
type MathOperator = 'plus' | 'minus'
type BinaryOperator = ComparisonOperator | MathOperator
type LogicalOperator = 'and' | 'or';
type AggregateOperator = "count" | "average" | "max" | "min"

type Expr =
  | { type: 'expr_column'; source: Query<Schema>; column: string }
  | { type: 'value'; value: Scalar }
  | { type: 'eq'; left: Expr; right: Expr }
  | { type: 'binary_op'; op: BinaryOperator; left: Expr; right: Expr }
  | { type: 'logical_op'; op: LogicalOperator; args: Array<Expr>}
  | { type: 'not'; expr: Expr }
  | { type: 'agg'; op: AggregateOperator; source: Query<Schema> }

// ===== Query Nodes =====
type Query<S extends Schema> =
  | { type: 'query_column'; source: Query<Schema>; column: string }
  | { type: 'table'; name: string; schema: S }
  | { type: 'filter'; source: Query<S>; filter: Expr }
  | { type: 'map'; source: Query<Schema>, mapped: Projection<S> }
  | { type: 'order_by'; source: Query<S>; orderings: OrderSpec[] }
  | { type: 'limit'; source: Query<S>; limit: number }
  | { type: 'offset'; source: Query<S>; offset: number }
  | { type: 'set_op'; op: 'union' | 'intersect' | 'difference'; left: Query<S>; right: Query<S> }
  | { type: 'group_by'; source: Query<Schema>; key: Expr };

type OrderSpec = { expr: Expr; direction: 'asc' | 'desc' };

// ===== Schema Types =====
type ScalarType = 'uuid' | 'string' | 'number' | 'boolean' | 'unknown'

type Schema = {[columnName: string]: Schema | ScalarType};
type Projection<S extends Schema> = {[K in keyof S]: S[K] extends Schema ? Query<S[K]> : Expr}
type ColumnAccessors<S extends Schema> = {
  [K in keyof S]: S[K] extends Schema 
    ? QueryBuilder<S[K]> 
    : ExprBuilder
};

// ===== ColumnExprBuilder =====
class ExprBuilder {
  constructor(public node: Expr) {}

  eq(value: Expr | Scalar): ExprBuilder {
    return new ExprBuilder(eq(this.node, value))
  }

  gt(value: Expr | number): ExprBuilder {
    return new ExprBuilder(gt(this.node, value))
  }

  lt(value: Expr | number): ExprBuilder {
    return new ExprBuilder(lt(this.node, value))
  }

  minus(value: Expr | number): ExprBuilder {
    return new ExprBuilder(minus(this.node, value))
  }

  plus(value: Expr | number): ExprBuilder {
    return new ExprBuilder(plus(this.node, value))
  }

  and(expr: Expr): ExprBuilder {
    return new ExprBuilder(and(this.node, expr))
  }

  or(expr: Expr): ExprBuilder {
    return new ExprBuilder(or(this.node, expr))
  }

  not(): ExprBuilder {
    return new ExprBuilder(not(this.node))
  }

  asc(): OrderSpecBuilder {
    return new OrderSpecBuilder({ expr: this.node, direction: 'asc' })
  }

  desc(): OrderSpecBuilder {
    return new OrderSpecBuilder({ expr: this.node, direction: 'desc' });
  }
}

// ===== Functional Core =====
function val(value: Scalar): Expr {
  return { type: 'value', value };
}

function toExpr(value: any): Expr {
  return typeof value === 'object' && 'type' in value ? value : val(value);
}

function isScalarType(v: ScalarType | Schema): v is ScalarType {
  if (v === "uuid" || v === "string" || v === "number" || v === "unknown" || v === "boolean") {
    v satisfies ScalarType
    return true;
  } else  {
    return false;
  }
}

function isExpr(v: Expr | Query<Schema>): v is Expr {
  if (v.type === "expr_column" 
      || v.type === "value" 
        || v.type === "eq" 
          || v.type === "binary_op"
            || v.type === "logical_op"
              || v.type === "not"
                || v.type === "agg") {
                  v satisfies Expr
                  return true;
                }
                v satisfies Query<Schema>;
                return false;
}

function getSchema<S extends Schema>(source: Query<S>): S {
  if (source.type === "table") {
    return source.schema;
  } else if (source.type === "limit") {
    return getSchema(source.source);
  } else if (source.type === "filter") {
    return getSchema(source.source);
  } else if (source.type === "offset") {
    return getSchema(source.source);
  } else if (source.type === "order_by") {
    return getSchema(source.source);
  } else if (source.type === "set_op") {
    return getSchema(source.left);
  } else if (source.type === "map") {
    return mapValues(source.mapped, (v) => {
      if (isExpr(v)) {
        return "unknown";
      } else {
        return getSchema(v);
      }
    }) as S
  } else if (source.type === "group_by") {
    return {
      key: "unknown",
      values: getSchema(source.source),
    } as unknown as S
  } else if (source.type === "query_column") {
    const parentSchema = getSchema(source.source)
    return parentSchema[source.column] as S
  } else {
    return unreachable(source)
  }
}

function toColumnAccessors<S extends Schema>(source: Query<S>): ColumnAccessors<S> {
  const schema = getSchema(source);
    return mapValues(schema, (v, k) => {
      if (isScalarType(v)) {
        return new ExprBuilder({type: "expr_column", source, column: k})
      } else {
        return new QueryBuilder({type: "query_column", source, column: k})
      }
    }) as ColumnAccessors<S>
}

function toProjection<S extends Schema>(cols: ColumnAccessors<S>): Projection<S> {
  return mapValues(cols, v => v.node) as unknown as Projection<S>;
}

// ===== Functional Query Transformers =====
function map<S extends Schema, F extends Schema>(
  source: Query<S>,
  fn: (cols: ColumnAccessors<S>) => Projection<F>
): Query<F> {
  const cols = toColumnAccessors(source)
  return { type: 'map', source, mapped: fn(cols) }
}

function filter<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr): Query<S> {
  const cols = toColumnAccessors(source);
  return { type: 'filter', source, filter: fn(cols) }
}

function orderBy<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => OrderSpec[]): Query<S> {
  const cols = toColumnAccessors(source);
  return { type: 'order_by', source, orderings: fn(cols) };
}

function groupBy<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr): Query<{key: ScalarType, values: S}>   {
  const cols = toColumnAccessors(source)
  return { type: 'group_by', source, key: fn(cols) };
}

function limit<S extends Schema>(source: Query<S>, n: number): Query<S> {
  return { type: 'limit', source, limit: n };
}

function offset<S extends Schema>(source: Query<S>, n: number): Query<S> {
  return { type: 'offset', source, offset: n };
}

function union<S extends Schema>(left: Query<S>, right: Query<S>): Query<S> {
  return { type: 'set_op', op: "union", left, right };
}

function intersect<S extends Schema>(left: Query<S>, right: Query<S>): Query<S> {
  return { type: 'set_op', op: "intersect", left, right };
}

function difference<S extends Schema>(left: Query<S>, right: Query<S>): Query<S> {
  return { type: 'set_op', op: "difference", left, right };
}

function count(source: Query<Schema>): Expr {
  return { type: 'agg', op: 'count', source };
}

function max(source: Query<Schema>): Expr {
  return { type: 'agg', op: 'max', source };
}

function min(source: Query<Schema>): Expr {
  return { type: 'agg', op: 'min', source };
}

function average(source: Query<Schema>): Expr {
  return { type: 'agg', op: 'average', source };
}

// compound helpers
function averageOf<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr) {
  return average(map(source, (cols) => ({val: fn(cols)})))
}

function maxOf<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr) {
  return min(map(source, (cols) => ({val: fn(cols)})))
}

function minOf<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr) {
  return max(map(source, (cols) => ({val: fn(cols)})))
}

function any<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr): Expr {
  return gt(count(filter(source, fn)), 0)
}

function every<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr): Expr {
  const inverse = filter(source, (cols) => not(fn(cols)))
  return eq(count(inverse), 0)
}

// ===== Functional Expression Builders + ColumnExprBuilder =====

// ===== Functional Expression Builders =====
function eq(left: Expr, right: Expr | Scalar): Expr {
  return { type: 'eq', left, right: toExpr(right) };
}

function gt(left: Expr, right: Expr | Scalar): Expr {
  return { type: 'binary_op', op: 'gt', left, right: toExpr(right) };
}

function lt(left: Expr, right: Expr | Scalar): Expr {
  return { type: 'binary_op', op: 'lt', left, right: toExpr(right) };
}

function minus(left: Expr, right: Expr | Scalar): Expr {
  return { type: 'binary_op', op: 'minus', left, right: toExpr(right) };
}

function plus(left: Expr, right: Expr | Scalar): Expr {
  return { type: 'binary_op', op: 'plus', left, right: toExpr(right) };
}

function and(...args: Expr[]): Expr {
  return { type: 'logical_op', op: 'and', args };
}

function or(...args: Expr[]): Expr {
  return { type: 'logical_op', op: 'or', args };
}

function not(expr: Expr): Expr {
  return { type: 'not', expr };
}

class OrderSpecBuilder {
  constructor(private ref: OrderSpec) {}

  toAST(): OrderSpec {
    return this.ref;
  }
}

// ===== QueryBuilder Class (Fluent Wrapper) =====
class QueryBuilder<T extends Schema> {
  constructor(public node: Query<T>) {}
  toAST(): Query<T> {
    return this.node;
  }

  filter(fn: (cols: ColumnAccessors<T>) => Expr): QueryBuilder<T> {
    return new QueryBuilder(filter<T>(this.node, fn));
  }

  map<F extends Schema>(
    fn: (cols: ColumnAccessors<T>) => ColumnAccessors<F>
  ): QueryBuilder<F> {
    return new QueryBuilder(map<T, F>(this.node, (source) => toProjection(fn(source))));
  }

  orderBy(fn: (cols: ColumnAccessors<T>) => OrderSpec[]): QueryBuilder<T> {
    return new QueryBuilder(orderBy<T>(this.node, fn));
  }

  groupBy(fn: (cols: ColumnAccessors<T>) => Expr): QueryBuilder<{ key: ScalarType; values: T }> {
    return new QueryBuilder(groupBy<T>(this.node, fn));
  }

  limit(n: number): QueryBuilder<T> {
    return new QueryBuilder(limit(this.node, n));
  }

  offset(n: number): QueryBuilder<T> {
    return new QueryBuilder(offset(this.node, n));
  }

  union(other: QueryBuilder<T>): QueryBuilder<T> {
    return new QueryBuilder(union(this.node, other.node));
  }

  intersect(other: QueryBuilder<T>): QueryBuilder<T> {
    return new QueryBuilder(intersect(this.node, other.node));
  }

  difference(other: QueryBuilder<T>): QueryBuilder<T> {
    return new QueryBuilder(difference(this.node, other.node))
  }

  count(): ExprBuilder {
    return new ExprBuilder(count(this.node))
  }

  average(): ExprBuilder {
    return new ExprBuilder(average(this.node))
  }

  max(): ExprBuilder {
    return new ExprBuilder(max(this.node))
  }

  min(): ExprBuilder {
    return new ExprBuilder(min(this.node))
  }

  // helpers
  any(fn: (cols: ColumnAccessors<T>) => Expr): ExprBuilder {
    return new ExprBuilder(any<T>(this.node, fn))
  }

  every(fn: (cols: ColumnAccessors<T>) => Expr): ExprBuilder {
    return new ExprBuilder(every<T>(this.node, fn))
  }

  averageOf(fn: (cols: ColumnAccessors<T>) => Expr): ExprBuilder {
    return new ExprBuilder(averageOf(this.node, fn))
  }
  maxOf(fn: (cols: ColumnAccessors<T>) => Expr): ExprBuilder {
    return new ExprBuilder(maxOf(this.node, fn))
  }
  minOf(fn: (cols: ColumnAccessors<T>) => Expr): ExprBuilder {
    return new ExprBuilder(minOf(this.node, fn))
  }
}

function unreachable(v: never): never {
  throw new Error(`Expected ${v} to be unreachale`)
}
