// Core scalar types
type Scalar = string | number | boolean | null;

// ===== Expression Types =====
type Expr =
  | { type: 'column'; table: string; column: string }
  | { type: 'value'; value: Scalar }
  | { type: 'binary_op'; op: ComparisonOperator; left: Expr; right: Expr }
  | { type: 'logical_op'; op: LogicalOperator; args: Expr[] }
  | { type: 'not'; expr: Expr };

type ComparisonOperator = 'eq' | 'ne' | 'gt' | 'lt' | 'gte' | 'lte';
type LogicalOperator = 'and' | 'or';

// ===== Query Nodes =====
type QueryNode =
  | { type: 'table'; name: string }
  | { type: 'select'; source: QueryNode; predicate: Expr }
  | { type: 'project'; source: QueryNode; fields: Record<string, Expr | QueryNode> }
  | { type: 'order_by'; source: QueryNode; orderings: OrderSpec[] }
  | { type: 'limit'; source: QueryNode; limit: number }
  | { type: 'offset'; source: QueryNode; offset: number }
  | { type: 'set_op'; op: 'union' | 'intersect' | 'except'; left: QueryNode; right: QueryNode }
  | { type: 'group_by'; source: QueryNode; key: Expr };

type OrderSpec = { expr: Expr; direction: 'asc' | 'desc' };

// ===== Schema/Utility Types =====
type Schema = Record<string, 'uuid' | 'string' | 'number' | 'boolean'>;

type ColumnAccessors<T extends Schema> = {
  [K in keyof T]: ColumnExprBuilder;
};

type InferSchema<F> = {
  [K in keyof F]: F[K] extends TableBuilder<infer R> ? R : 'scalar';
};

// ===== Column Expression Builder =====
class ColumnExprBuilder {
  constructor(private ref: { type: 'column'; table: string; column: string }) {}

  eq(value: Expr | Scalar) {
    return { type: 'binary_op', op: 'eq', left: this.ref, right: toExpr(value) } as const;
  }

  gt(value: Expr | Scalar) {
    return { type: 'binary_op', op: 'gt', left: this.ref, right: toExpr(value) } as const;
  }

  lt(value: Expr | Scalar) {
    return { type: 'binary_op', op: 'lt', left: this.ref, right: toExpr(value) } as const;
  }

  and(expr: Expr) {
    return { type: 'logical_op', op: 'and', args: [this.ref, expr] } as const;
  }

  asc(): OrderSpec {
    return { expr: this.ref, direction: 'asc' };
  }

  desc(): OrderSpec {
    return { expr: this.ref, direction: 'desc' };
  }
}

// ===== Utility Functions =====
function val(value: Scalar): Expr {
  return { type: 'value', value };
}

function toExpr(value: any): Expr {
  return typeof value === 'object' && 'type' in value ? value : val(value);
}

function columnAccessors<T extends Schema>(table: string, schema: T): ColumnAccessors<T> {
  const acc = {} as any;
  for (const key of Object.keys(schema)) {
    acc[key] = new ColumnExprBuilder({ type: 'column', table, column: key });
  }
  return acc;
}

function isBuilder(x: any): x is TableBuilder<any> {
  return typeof x === 'object' && x.node;
}

// ===== Aggregates =====
type AggregateExpr = {
  type: 'agg';
  op: 'count';
  source: QueryNode;
  expr?: Expr;
};

type ExtendedExpr = Expr | AggregateExpr;

function count(source: TableBuilder<any>): AggregateExpr {
  return {
    type: 'agg',
    op: 'count',
    source: source.node,
  };
}

// ===== TableBuilder =====
type TableBuilder<T extends Schema> = {
  schema: T;
  tableName: string;
  node: QueryNode;

  where(predicate: (cols: ColumnAccessors<T>) => Expr): TableBuilder<T>;

  project<F extends Record<string, Expr | TableBuilder<any>>>(
    fn: (cols: ColumnAccessors<T>) => F
  ): TableBuilder<InferSchema<F>>;

  orderBy(fn: (cols: ColumnAccessors<T>) => OrderSpec[]): TableBuilder<T>;

  groupBy(fn: (cols: ColumnAccessors<T>) => Expr): TableBuilder<{ key: 'scalar'; values: T }>;

  limit(n: number): TableBuilder<T>;
  offset(n: number): TableBuilder<T>;

  unionBy(other: TableBuilder<T>): TableBuilder<T>;
  intersectBy(other: TableBuilder<T>): TableBuilder<T>;
  differenceBy(other: TableBuilder<T>): TableBuilder<T>;

  toAST(): QueryNode;
};

function Table<T extends Schema>(name: string, schema: T): TableBuilder<T> {
  return {
    schema,
    tableName: name,
    node: { type: 'table', name },

    where(fn) {
      const predicate = fn(columnAccessors(name, schema));
      return {
        ...this,
        node: { type: 'select', source: this.node, predicate },
      };
    },

    project(fn) {
      const fields = fn(columnAccessors(this.tableName, this.schema));
      const rewritten: Record<string, Expr | QueryNode> = {};
      for (const [k, v] of Object.entries(fields)) {
        rewritten[k] = isBuilder(v) ? v.node : v;
      }
      return {
        ...this,
        node: { type: 'project', source: this.node, fields: rewritten },
      };
    },

    orderBy(fn) {
      const specs = fn(columnAccessors(this.tableName, this.schema));
      return {
        ...this,
        node: {
          type: 'order_by',
          source: this.node,
          orderings: specs,
        },
      };
    },

    groupBy(fn) {
      const keyExpr = fn(columnAccessors(this.tableName, this.schema));
      return {
        schema: { key: 'scalar', values: this.schema },
        tableName: this.tableName,
        node: {
          type: 'group_by',
          source: this.node,
          key: keyExpr,
        },
      };
    },

    limit(n) {
      return {
        ...this,
        node: { type: 'limit', source: this.node, limit: n },
      };
    },

    offset(n) {
      return {
        ...this,
        node: { type: 'offset', source: this.node, offset: n },
      };
    },

    unionBy(other) {
      return {
        ...this,
        node: {
          type: 'set_op',
          op: 'union',
          left: this.node,
          right: other.node,
        },
      };
    },

    intersectBy(other) {
      return {
        ...this,
        node: {
          type: 'set_op',
          op: 'intersect',
          left: this.node,
          right: other.node,
        },
      };
    },

    differenceBy(other) {
      return {
        ...this,
        node: {
          type: 'set_op',
          op: 'except',
          left: this.node,
          right: other.node,
        },
      };
    },

    toAST() {
      return this.node;
    },
  };
}
