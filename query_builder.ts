import { mapValues } from "lodash"

// ===== Core Scalar Types =====
type Scalar = string | number | boolean | null;

type ComparisonOperator = 'ne' | 'gt' | 'lt' | 'gte' | 'lte';
type LogicalOperator = 'and' | 'or';

// ===== Expression Types =====
type Expr =
  | { type: 'column'; table: string; column: string }
  | { type: 'column_ref_expr'; source: Query<Schema>; column: string }
  | { type: 'value'; value: Scalar }
  | { type: 'eq'; left: Expr; right: Expr }
  | { type: 'binary_op'; op: ComparisonOperator; left: Expr; right: Expr }
  | { type: 'logical_op'; op: LogicalOperator; args: Array<Expr>}
  | { type: 'not'; expr: Expr }
  | AggregateExpr

type AggregateExpr = {
  type: 'agg';
  op: 'count';
  source: Query<Schema>;
};

// ===== Query Nodes =====
type GroupBySourceSchema<S extends Schema> = S extends {key: Scalar, values: Query<infer ValueSchema>} 
  ? ValueSchema
  : never;
type Query<S extends Schema> =
  | { type: 'column_ref_query'; source: Query<Schema>; column: string }
  | { type: 'table'; name: string; schema: S }
  | { type: 'filter'; source: Query<S>; predicate: Expr }
  | { type: 'project'; fields: {[K in keyof S]: S[K] extends Schema ? Query<S[K]> : Expr}}
  | { type: 'order_by'; source: Query<S>; orderings: OrderSpec[] }
  | { type: 'limit'; source: Query<S>; limit: number }
  | { type: 'offset'; source: Query<S>; offset: number }
  | { type: 'set_op'; op: 'union' | 'intersect' | 'except'; left: Query<S>; right: Query<S> }
  | { type: 'group_by'; source: Query<GroupBySourceSchema<S>>; key: Expr };

type OrderSpec = { expr: Expr; direction: 'asc' | 'desc' };

// ===== Schema Types =====
type ScalarType = 'uuid' | 'string' | 'number' | 'boolean' | 'unknown'
type Schema = {[columnName: string]: Schema | ScalarType};

type ColumnAccessors<S extends Schema> = {
  [K in keyof S]: S[K] extends Schema 
    ? QueryBuilder<S[K]> 
    : ExprBuilder
};

type InferSchema<F> = {
  [K in keyof F]: 
    F[K] extends Query<infer R> ? R :
    F[K] extends Expr ? ScalarType : never;
};

// ===== ColumnExprBuilder =====
class ExprBuilder {
  constructor(private ref: Expr) {}

  eq(value: Expr | Scalar): ExprBuilder {
    return new ExprBuilder(eq(this.ref, value))
  }

  gt(value: Expr | number): ExprBuilder {
    return new ExprBuilder(gt(this.ref, value))
  }

  lt(value: Expr | number): ExprBuilder {
    return new ExprBuilder(lt(this.ref, value))
  }

  and(expr: Expr): ExprBuilder {
    return new ExprBuilder(and(this.ref, expr))
  }

  or(expr: Expr): ExprBuilder {
    return new ExprBuilder(or(this.ref, expr))
  }

  not(): ExprBuilder {
    return new ExprBuilder(not(this.ref))
  }

  asc(): OrderSpecBuilder {
    return new OrderSpecBuilder({ expr: this.ref, direction: 'asc' })
  }

  desc(): OrderSpecBuilder {
    return new OrderSpecBuilder({ expr: this.ref, direction: 'desc' });
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
  if (v.type === "column" 
    || v.type === "column_ref_expr" 
      || v.type === "value" 
        || v.type === "eq" 
          || v.type === "binary_op"
            || v.type === "logical_op"
              || v.type === "not"
              || v.type === "agg") {
                v satisfies Expr
                return true;
              }
              v satisfies Query<any>;
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
  } else if (source.type === "project") {
    return mapValues(source.fields, (v) => {
      if (isExpr(v)) {
        return "unknown";
      } else {
        return getSchema(v);
      }
    })
  } else if (source.type === "group_by") {
    return {
      key: source.key,
      values: source.source,
    }
  } else if (source.type === "column_ref_query") {
    const parentSchema = getSchema(source.source)
    return parentSchema[source.column] as S
  } else {
    return unreachable(source)
  }
}

function columnAccessors<S extends Schema>(source: Query<S>): ColumnAccessors<S> {
  const schema = getSchema(source);
    return mapValues(schema, (v, k) => {
      if (isScalarType(v)) {
        return new ExprBuilder({type: "columnRef", parent: source, column: k})
      } else {
        return new QueryBuilder({type: "columnRef", parent: source, column: k})
      }
    }) as ColumnAccessors<S>
}

// ===== Functional Query Transformers =====
function project<S extends Schema, F extends Schema>(
  source: Query<S>,
  fn: (cols: ColumnAccessors<S>) => F
): Query<F> {
  const cols = columnAccessors(source)
  return { type: 'project', source, fields: mapValues(fn(cols), (v) => v.node) }
}

function orderBy<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => OrderSpec[]): Query<S> {
  const cols = columnAccessors(source);
  return { type: 'order_by', source, orderings: fn(cols) };
}

function groupBy<S extends Schema>(source: Query<S>, fn: (cols: ColumnAccessors<S>) => Expr): Query<{key: 'unknown', values: S}>   {
  const cols = columnAccessors(source)
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

function count(source: Query<Schema>): AggregateExpr {
  return { type: 'agg', op: 'count', source };
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

  where(fn: (cols: ColumnAccessors<T>) => Expr): QueryBuilder<T> {
    return new QueryBuilder(select<T>(this.node, fn));
  }

  project<F extends Record<string, Expr | QueryBuilder<any>>>(
    fn: (cols: ColumnAccessors<T>) => F
  ): QueryBuilder<InferSchema<F>> {
    return new QueryBuilder(project<T, F>(this.node, fn));
  }

  orderBy(fn: (cols: ColumnAccessors<T>) => OrderSpec[]): QueryBuilder<T> {
    return new QueryBuilder(orderBy<T>(this.node, fn));
  }

  groupBy(fn: (cols: ColumnAccessors<T>) => Expr): QueryBuilder<{ key: 'scalar'; values: T }> {
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

  toAST(): Query<T> {
    return this.node;
  }
}

function unreachable(v: never): never {
  throw new Error(`Expected ${v} to be unreachale`)
}
