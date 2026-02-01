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
type NumberType = {
    type: "number";
};
type StringType = {
    type: "string";
};
type NullType = {
    type: "null";
};
type BoolType = {
    type: "bool";
};
type ScalarType = NumberType | StringType | NullType | BoolType;
type ArrayType = {
    type: "array";
    el: Type;
};
type RecordType = {
    type: "record";
    fields: {
        [col: string]: Type;
    };
};
type Type = ScalarType | ArrayType | RecordType;
type ArrayTypeOf<T extends Readonly<Type>> = {
    type: "array";
    el: T;
};
type RecordTypeOf<T extends Record<string, Type>> = {
    type: "record";
    fields: {
        [K in keyof T]: T[K];
    };
};
export type Schema = RecordType;
export type Query<S extends Schema> = Expr<ArrayTypeOf<S>>;
type AnyExpr<T extends Type> = {
    type: 'field';
    source: Expr<RecordTypeOf<Record<string, T>>>;
    field: string;
} | {
    type: 'first';
    source: Expr<ArrayTypeOf<T>>;
} | {
    type: 'row';
    source: Expr<ArrayTypeOf<T>>;
} | {
    type: 'record';
    fields: Record<string, Expr<any>>;
};
type ScalarWindowOperator = "max" | "min";
type ScalarExpr<T extends Type> = never | {
    type: 'scalar_window';
    op: ScalarWindowOperator;
    source: Expr<ArrayTypeOf<T>>;
};
type MathOperator = 'plus' | 'minus';
type NumberWindowOperator = "max" | "min" | "average";
type NumberExpr = {
    type: 'number';
    number: number;
} | {
    type: "math_op";
    op: MathOperator;
    left: Expr<NumberType>;
    right: Expr<NumberType>;
} | {
    type: 'number_window';
    op: NumberWindowOperator;
    source: Expr<ArrayTypeOf<NumberType>>;
} | {
    type: 'count';
    source: Expr<ArrayType>;
};
type LogicalOperator = 'and' | 'or';
type ComparisonOperator = 'gt' | 'lt' | 'gte' | 'lte';
type BooleanExpr = {
    type: 'boolean';
    boolean: boolean;
} | {
    type: 'not';
    expr: Expr<BoolType>;
} | {
    type: "eq";
    left: Expr<ScalarType>;
    right: Expr<ScalarType>;
} | {
    type: 'comparison_op';
    op: ComparisonOperator;
    left: Expr<NumberType>;
    right: Expr<NumberType>;
} | {
    type: 'logical_op';
    op: LogicalOperator;
    left: Expr<BoolType>;
    right: Expr<BoolType>;
};
type StringExpr = {
    type: "string";
    string: string;
};
type NullExpr = {
    type: "null";
};
type ArrayExpr<T extends Type> = never | {
    type: 'array';
    array: Array<any>;
} | {
    type: 'table';
    name: string;
    schema: T;
} | {
    type: 'filter';
    source: Expr<ArrayTypeOf<T>>;
    filter: Expr<BoolType>;
} | {
    type: 'sort';
    source: Expr<ArrayTypeOf<T>>;
    sort: Expr<Type>;
} | {
    type: 'limit';
    source: Expr<ArrayTypeOf<T>>;
    limit: Expr<NumberType>;
} | {
    type: 'offset';
    source: Expr<ArrayTypeOf<T>>;
    offset: Expr<NumberType>;
} | {
    type: 'set_op';
    op: 'union' | 'intersect' | 'difference';
    left: Expr<ArrayTypeOf<T>>;
    right: Expr<ArrayTypeOf<T>>;
} | {
    type: 'map';
    source: Expr<ArrayTypeOf<Type>>;
    map: Expr<T>;
} | {
    type: 'flat_map';
    source: Expr<ArrayTypeOf<Type>>;
    flatMap: Expr<ArrayTypeOf<T>>;
} | {
    type: 'group_by';
    source: Expr<ArrayTypeOf<Type>>;
    key: Expr<Type>;
};
export type Expr<T extends Type> = {
    __brand?: T;
} & ((AnyExpr<T>) | (T extends ScalarType ? ScalarExpr<T> : never) | (T extends BoolType ? BooleanExpr : never) | (T extends NumberType ? NumberExpr : never) | (T extends StringType ? StringExpr : never) | (T extends NullType ? NullExpr : never) | (T extends ArrayTypeOf<infer ElemT> ? ElemT extends Type ? ArrayExpr<ElemT> : never : never));
type LiteralOf<T extends Type> = T extends {
    type: "null";
} ? null : T extends {
    type: "string";
} ? string : T extends {
    type: "bool";
} ? boolean : T extends {
    type: "number";
} ? number : T extends {
    type: "array";
    el: Type;
} ? Array<LiteralOf<T["el"]>> : T extends {
    type: "record";
    fields: {
        [col: string]: Type;
    };
} ? {
    [K in keyof T["fields"]]: LiteralOf<T["fields"][K]>;
} : never;
export type ExprBuilder<T extends Type> = {
    __brand?: T;
} & (T extends NullType ? NullBuilder : T extends BoolType ? BooleanBuilder : T extends NumberType ? NumberBuilder : T extends StringType ? StringBuilder : T extends ArrayTypeOf<infer ElemT> ? ElemT extends Type ? ArrayBuilder<ElemT> : never : T extends RecordType ? {
    [Key in keyof T["fields"]]: ExprBuilder<T["fields"][Key]>;
} : never);
declare class ArrayBuilder<T extends Type> {
    node: Expr<ArrayTypeOf<T>>;
    constructor(node: Expr<ArrayTypeOf<T>>);
    filter(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): ArrayBuilder<T>;
    sort(fn: (val: ExprBuilder<T>) => ValueOf<Type>): ArrayBuilder<T>;
    groupBy<K extends Type>(fn: (val: ExprBuilder<T>) => ValueOf<K>): ArrayBuilder<RecordTypeOf<{
        key: K;
        values: ArrayTypeOf<T>;
    }>>;
    map<R extends Type>(fn: (val: ExprBuilder<T>) => ValueOf<R>): ArrayBuilder<R>;
    limit(value: ValueOf<NumberType>): ArrayBuilder<T>;
    offset(value: ValueOf<NumberType>): ArrayBuilder<T>;
    union(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T>;
    intersection(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T>;
    difference(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T>;
    count(): NumberBuilder;
    average(): NumberBuilder;
    max(): ExprBuilder<T>;
    min(): ExprBuilder<T>;
    any(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): BooleanBuilder;
    every(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): BooleanBuilder;
}
type ValueOf<T extends Type> = LiteralOf<T> | Expr<T> | ExprBuilder<T>;
declare class NumberBuilder {
    node: Expr<NumberType>;
    constructor(node: Expr<NumberType>);
    eq(value: ValueOf<NumberType>): BooleanBuilder;
    gt(value: ValueOf<NumberType>): BooleanBuilder;
    lt(value: ValueOf<NumberType>): BooleanBuilder;
    minus(value: ValueOf<NumberType>): NumberBuilder;
    plus(value: ValueOf<NumberType>): NumberBuilder;
}
declare class StringBuilder {
    node: Expr<StringType>;
    constructor(node: Expr<StringType>);
    eq(value: ValueOf<StringType>): BooleanBuilder;
}
declare class BooleanBuilder {
    node: Expr<BoolType>;
    constructor(node: Expr<BoolType>);
    eq(value: ValueOf<BoolType>): BooleanBuilder;
    and(value: ValueOf<BoolType>): BooleanBuilder;
    or(value: ValueOf<BoolType>): BooleanBuilder;
    not(): BooleanBuilder;
}
declare class NullBuilder {
    node: Expr<NullType>;
    constructor(node: Expr<NullType>);
    or(value: ValueOf<NullType>): BooleanBuilder;
}
export declare function unreachable(val: never): never;
type TypeName = 'string' | 'number' | 'bool' | 'null' | 'uuid';
type SchemaSpec = Record<string, TypeName>;
type TypeFromName<N extends TypeName> = N extends 'string' ? StringType : N extends 'uuid' ? StringType : N extends 'number' ? NumberType : N extends 'bool' ? BoolType : N extends 'null' ? NullType : never;
type SchemaFromSpec<S extends SchemaSpec> = RecordTypeOf<{
    [K in keyof S]: TypeFromName<S[K]>;
}>;
export declare function Table<S extends SchemaSpec>(name: string, schema: S): ArrayBuilder<SchemaFromSpec<S>>;
export {};
