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
    __kind: "number";
};
type StringType = {
    __kind: "string";
};
type NullType = {
    __kind: "null";
};
type BoolType = {
    __kind: "bool";
};
type ScalarType = NumberType | StringType | NullType | BoolType;
type ArrayType = {
    __kind: "array";
    __el: Type;
};
type RecordType = {
    __kind: "record";
    __fields: {
        [col: string]: Type;
    };
};
type Type = ScalarType | ArrayType | RecordType;
type ArrayTypeOf<T extends Readonly<Type>> = {
    __kind: "array";
    __el: T;
};
type RecordTypeOf<T extends Record<string, Type>> = {
    __kind: "record";
    __fields: {
        [K in keyof T]: T[K];
    };
};
export type Schema = RecordType;
export type Query<S extends Schema> = Expr<ArrayTypeOf<S>>;
type AnyExpr<T extends Type> = {
    __type: 'field';
    __source: Expr<RecordTypeOf<Record<string, T>>>;
    __field: string;
} | {
    __type: 'first';
    __source: Expr<ArrayTypeOf<T>>;
} | {
    __type: 'row';
    __source: Expr<ArrayTypeOf<T>>;
} | {
    __type: 'record';
    __fields: Record<string, Expr<any>>;
};
type ScalarWindowOperator = "max" | "min";
type ScalarExpr<T extends Type> = never | {
    __type: 'scalar_window';
    __op: ScalarWindowOperator;
    __source: Expr<ArrayTypeOf<T>>;
};
type MathOperator = 'plus' | 'minus';
type NumberWindowOperator = "max" | "min" | "average";
type NumberExpr = {
    __type: 'number';
    __number: number;
} | {
    __type: "math_op";
    __op: MathOperator;
    __left: Expr<NumberType>;
    __right: Expr<NumberType>;
} | {
    __type: 'number_window';
    __op: NumberWindowOperator;
    __source: Expr<ArrayTypeOf<NumberType>>;
} | {
    __type: 'count';
    __source: Expr<ArrayType>;
};
type LogicalOperator = 'and' | 'or';
type ComparisonOperator = 'gt' | 'lt' | 'gte' | 'lte';
type BooleanExpr = {
    __type: 'boolean';
    __boolean: boolean;
} | {
    __type: 'not';
    __expr: Expr<BoolType>;
} | {
    __type: "eq";
    __left: Expr<ScalarType>;
    __right: Expr<ScalarType>;
} | {
    __type: 'comparison_op';
    __op: ComparisonOperator;
    __left: Expr<NumberType>;
    __right: Expr<NumberType>;
} | {
    __type: 'logical_op';
    __op: LogicalOperator;
    __left: Expr<BoolType>;
    __right: Expr<BoolType>;
};
type StringExpr = {
    __type: "string";
    __string: string;
};
type NullExpr = {
    __type: "null";
};
type ArrayExpr<T extends Type> = never | {
    __type: 'array';
    __array: Array<any>;
} | {
    __type: 'table';
    __name: string;
    __schema: T;
} | {
    __type: 'filter';
    __source: Expr<ArrayTypeOf<T>>;
    __filter: Expr<BoolType>;
} | {
    __type: 'sort';
    __source: Expr<ArrayTypeOf<T>>;
    __sort: Expr<Type>;
} | {
    __type: 'limit';
    __source: Expr<ArrayTypeOf<T>>;
    __limit: Expr<NumberType>;
} | {
    __type: 'offset';
    __source: Expr<ArrayTypeOf<T>>;
    __offset: Expr<NumberType>;
} | {
    __type: 'set_op';
    __op: 'union' | 'intersect' | 'difference';
    __left: Expr<ArrayTypeOf<T>>;
    __right: Expr<ArrayTypeOf<T>>;
} | {
    __type: 'map';
    __source: Expr<ArrayTypeOf<Type>>;
    __map: Expr<T>;
} | {
    __type: 'flat_map';
    __source: Expr<ArrayTypeOf<Type>>;
    __flatMap: Expr<ArrayTypeOf<T>>;
} | {
    __type: 'group_by';
    __source: Expr<ArrayTypeOf<Type>>;
    __key: Expr<Type>;
};
export type Expr<T extends Type> = {
    __brand?: T;
} & ((AnyExpr<T>) | (T extends ScalarType ? ScalarExpr<T> : never) | (T extends BoolType ? BooleanExpr : never) | (T extends NumberType ? NumberExpr : never) | (T extends StringType ? StringExpr : never) | (T extends NullType ? NullExpr : never) | (T extends ArrayTypeOf<infer ElemT> ? ElemT extends Type ? ArrayExpr<ElemT> : never : never));
export declare function exprType(e: Expr<any>): string;
type LiteralOf<T extends Type> = T extends {
    __kind: "null";
} ? null : T extends {
    __kind: "string";
} ? string : T extends {
    __kind: "bool";
} ? boolean : T extends {
    __kind: "number";
} ? number : T extends {
    __kind: "array";
    __el: Type;
} ? Array<LiteralOf<T["__el"]>> : T extends {
    __kind: "record";
    __fields: {
        [col: string]: Type;
    };
} ? {
    [K in keyof T["__fields"]]: LiteralOf<T["__fields"][K]>;
} : never;
export type ExprBuilder<T extends Type> = {
    __brand?: T;
} & (T extends NullType ? NullBuilder : T extends BoolType ? BooleanBuilder : T extends NumberType ? NumberBuilder : T extends StringType ? StringBuilder : T extends ArrayTypeOf<infer ElemT> ? ElemT extends Type ? ArrayBuilder<ElemT> : never : T extends RecordType ? {
    [Key in keyof T["__fields"]]: ExprBuilder<T["__fields"][Key]>;
} : never);
declare class ArrayBuilder<T extends Type> {
    __node: Expr<ArrayTypeOf<T>>;
    constructor(__node: Expr<ArrayTypeOf<T>>);
    filter(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): ArrayBuilder<T>;
    sort(fn: (val: ExprBuilder<T>) => ValueOf<Type>): ArrayBuilder<T>;
    groupBy<K extends Type>(fn: (val: ExprBuilder<T>) => ValueOf<K>): ArrayBuilder<RecordTypeOf<{
        key: K;
        values: ArrayTypeOf<T>;
    }>>;
    map<R>(fn: (val: ExprBuilder<T>) => R): ArrayBuilder<InferType<R>>;
    limit(value: ValueOf<NumberType>): ArrayBuilder<T>;
    offset(value: ValueOf<NumberType>): ArrayBuilder<T>;
    union(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T>;
    intersection(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T>;
    difference(other: ArrayBuilder<T> | Expr<ArrayTypeOf<T>>): ArrayBuilder<T>;
    count(): NumberBuilder;
    average(): NumberBuilder;
    max(): ExprBuilder<T>;
    min(): ExprBuilder<T>;
    first(): ArrayBuilder<T>;
    any(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): BooleanBuilder;
    every(fn: (val: ExprBuilder<T>) => ValueOf<BoolType>): BooleanBuilder;
}
type BuilderValue = ArrayBuilder<any> | NumberBuilder | StringBuilder | BooleanBuilder | NullBuilder;
type RecordObject = {
    [key: string]: BuilderValue | RecordObject | Expr<any> | string | number | boolean | null;
};
type ValueOf<T extends Type> = LiteralOf<T> | Expr<T> | ExprBuilder<T> | RecordObject;
type InferType<V> = V extends ArrayBuilder<infer E> ? ArrayTypeOf<E> : V extends NumberBuilder ? NumberType : V extends StringBuilder ? StringType : V extends BooleanBuilder ? BoolType : V extends NullBuilder ? NullType : V extends Expr<infer T> ? T : V extends string ? StringType : V extends number ? NumberType : V extends boolean ? BoolType : V extends null ? NullType : V extends {
    [key: string]: any;
} ? RecordTypeOf<{
    [K in keyof V]: InferType<V[K]>;
}> : Type;
declare class NumberBuilder {
    __node: Expr<NumberType>;
    constructor(__node: Expr<NumberType>);
    eq(value: ValueOf<NumberType> | null): BooleanBuilder;
    gt(value: ValueOf<NumberType>): BooleanBuilder;
    lt(value: ValueOf<NumberType>): BooleanBuilder;
    minus(value: ValueOf<NumberType>): NumberBuilder;
    plus(value: ValueOf<NumberType>): NumberBuilder;
}
declare class StringBuilder {
    __node: Expr<StringType>;
    constructor(__node: Expr<StringType>);
    eq(value: ValueOf<StringType> | null): BooleanBuilder;
}
declare class BooleanBuilder {
    __node: Expr<BoolType>;
    constructor(__node: Expr<BoolType>);
    eq(value: ValueOf<BoolType> | null): BooleanBuilder;
    and(value: ValueOf<BoolType>): BooleanBuilder;
    or(value: ValueOf<BoolType>): BooleanBuilder;
    not(): BooleanBuilder;
}
declare class NullBuilder {
    __node: Expr<NullType>;
    constructor(__node: Expr<NullType>);
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
