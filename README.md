<img src="https://github.com/user-attachments/assets/35982f58-7769-4630-9e20-b4d2bde57ded" width="300" />

# YARRQL: Yet Another Recursive Relational Query Language

Let's face it, SQL sucks. It's the only vestige (for most of us) of the COBOL era of languages, and despite its persistence, it's every bit as unwieldy as its contemporaries.
Nested arrays had _just been invented_ when SQL first came out, and it shows.
In place of the suspiciously new-fangled nested composite data (eg, columns where the _value_ of the column is itself a table), we get the morass that is `join`s and `group by` and `having`.

## Enter YARRQL

Well, what if we revisit that decision to not have table-valued columns? Then things get a lot simpler. 
Then a table looks pretty much like any array of composite values in any modern programming language, 
and we can manipulate it pretty much the same way, with `map` and `filter`. 

Want to `map` over a nested value? Sure, why not! Want to `filter` over a nested value? _by_ a nested value? Sure, why not!

And that allows us to express queries much more more intuitively than in SQL.

## Examples

Imagine we have the following tables:
```
 table Student {
   id: UUID,
   name: string,
   age: number
 }
 
 table Teacher {
   id: UUID
 }
 
 table Class {
   id: UUID
   name: string,
   teacher_id: UUID,
 }
 
 table Enrollment {
   id: UUID
   student_id: UUID
   class_id: UUID
   grade: undefined | number
 }
 
 ```
 You can specify queries as follows:
 ```
 const Teachers = Table({...schema from above})
 const Students = Table({...schema from above})
 const Enrollments = Table({...schema from above})
 const Classes = Table({...schema from above})
 const teachersWithStudents = Teachers.map(t => ({
     ...t, 
     students: Students.filter(s => {
        return Enrollment.any(e => {
            return Class.any(c => c.teacher_id.eq(t.id).and(c.id.eq(e.class_id)).and(e.student_id.eq(s.id))
         })
    })
 })
 ```
 and generate the logically equivalent SQL
 ```
 console.log(teachersWithStudents.toSql())
 ```

 While that's not terribly complicated to express in SQL, the following are much more so (at least, if you're as bad at SQL as I am)

 ```
 const teachersWithMatureClasses = Teachers.map(t => ({
   id: t.id
   matureClasses: Classes.filter(c => c.teacher_id.eq(t.id))
     .map(c => ({
       averageStudentAge: Students.filter(s => 
         Enrollments.some(e => e.class_id.eq(c.id).and(e.student_id.eq(s.id)))
       ).map(s => s.age()).average()
     }))
     .filter(c => c.averageStudentAge.gt(20))
 }));
 ```
 
 ```
 const studentsWithPendingGrades = Students.map(s => ({
   id: s.id,
   pendingClasses: Classes.filter(c => 
     Enrollments.any(e => 
       e.student_id.eq(s.id)
         .and(e.class_id.eq(c.id))
         .and(e.grade.eq(undefined))
     )
   )
 })).filter(s => s.pendingClasses.count().gt(0))
 ```
 
 ```
 const classesWithWideGradeRange = Classes.map(c => ({
  ...c,
   grades:  Enrollments
     .filter(e => e.class_id.eq(c.id).and(e.grade.neq(undefined)))
     .map(e => e.grade)
 })).filter(c => c.grades.max().minus(c.grades.min()).gte(20));
 ```

## What this is not

YARRQL is _not_ an ORM like ActiveRecord or DjangoORM or Sequelize. It's a query language, what you do with the data returned by it (using it directly or to hydrate objects) is up to you

YARRQL is also _not_ a DSL for constructing SQL queries in javascript like Knex, Drizzle, or Kysely. It's a different _query paradigm_ like JS is to Java (that's to say, not revolutionary, but still better :wink: )

## Don't LLMs make this pointless? Can't we just ask them to generate SQL?

LLMs can generate SQL, but it's hard to verify if that SQL is _correct_. I claim that it's much easier to see if a given YARRQL query does actually have the behavior you want.

LLMs can _also_ generate YARRQL, with the following prompt:

<details>
**YARRQL is a TypeScript-embedded query language for relational algebra, supporting nested (table-valued) columns and composable transformations. YARRQL queries are written using `.map`, `.filter`, `.any`, `.every`, and aggregate methods on table objects representing schema. Scalar columns map to values or expressions; table-valued columns map to subqueries (including nested `.filter`/`.map`). Filters use boolean-returning expressions (e.g., `s.age.gt(20)`), and aggregates like `.count()`, `.avg()` are called on subqueries. Logical connectives: `.and()`, `.or()`, `.not()`. Output is always a projection (`.map`), not direct selection of columns. Example:**

```typescript
const Teachers = Table({id: 'uuid', name: 'string'})
const Students = Table({id: 'uuid', name: 'string', age: 'number'})
const Enrollments = Table({student_id: 'uuid', class_id: 'uuid', grade: 'number'})
const Classes = Table({id: 'uuid', name: 'string', teacher_id: 'uuid'})

// Query for teachers with their students
const teachersWithStudents = Teachers.map(t => ({
  ...t,
  students: Students.filter(s =>
    Enrollments.any(e =>
      Classes.any(c =>
        c.teacher_id.eq(t.id).and(c.id.eq(e.class_id)).and(e.student_id.eq(s.id))
      )
    )
  )
}))

// Query for students with pending classes
const studentsWithPendingClasses = Students.map(s => ({
  id: s.id,
  pendingClasses: Classes.filter(c =>
    Enrollments.any(e =>
      e.student_id.eq(s.id).and(e.class_id.eq(c.id)).and(e.grade.eq(undefined))
    )
  )
})).filter(s => s.pendingClasses.count().gt(0))
```
API:
```typescript
class QueryBuilder<T extends Schema> {
  constructor(public node: Query<T>) {}
  toAST(): Query<T>

  filter(fn: (cols: ColumnAccessors<T>) => Expr): QueryBuilder<T>
  map<F extends Schema>(
    fn: (cols: ColumnAccessors<T>) => ColumnAccessors<F>
  ): QueryBuilder<F>
  orderBy(fn: (cols: ColumnAccessors<T>) => OrderSpec[]): QueryBuilder<T>
  groupBy(
    fn: (cols: ColumnAccessors<T>) => Expr
  ): QueryBuilder<{ key: ScalarType; values: T }>

  limit(n: number): QueryBuilder<T>
  offset(n: number): QueryBuilder<T>

  union(other: QueryBuilder<T>): QueryBuilder<T>
  intersect(other: QueryBuilder<T>): QueryBuilder<T>
  difference(other: QueryBuilder<T>): QueryBuilder<T>

  count(): ExprBuilder
  average(): ExprBuilder
  max(): ExprBuilder
  min(): ExprBuilder
  any(fn: (cols: ColumnAccessors<T>) => Expr): ExprBuilder
  every(fn: (cols: ColumnAccessors<T>) => Expr): ExprBuilder
}

class ExprBuilder {
  constructor(public node: Expr) {}

  eq(value: Expr | Scalar): ExprBuilder
  gt(value: Expr | number): ExprBuilder
  lt(value: Expr | number): ExprBuilder
  minus(value: Expr | number): ExprBuilder
  plus(value: Expr | number): ExprBuilder
  and(expr: Expr): ExprBuilder
  or(expr: Expr): ExprBuilder
  not(): ExprBuilder
  asc(): OrderSpecBuilder
  desc(): OrderSpecBuilder
}
```

**Generate queries in this style. Table-valued columns use nested subqueries via `.filter`/`.map`. Scalar columns are projected as values or aggregate expressions. Output is a `.map` projection (not SQL).**

</details>


## ToDo

As the long list here suggests, this is pre-pre-pre-pre-alpha. Not fit for actual production use anywhere

1. Add support for "simple" arrays (eg, if a table is typed `Array<{...fields}>`, constructs like `Array<Scalar>`
2. Add support for nested objects (eg, if a table is typed `Array{...fields}>`, constructs like `{..fields}` and `{field: {...subFields}}`
3. "pluck"-ing values (`.first`, `.nth`, `.last`)
3. SQL-ese aliases (`where` => `filter`, `map` => `select`)
4. Allow querying on tables without specifying their schema (at the cost of type safety)
5. A Chrome plugin to generate SQL from YARRQL on the fly
6. Support for more than just Postgres dialect SQL
7. (one day?) A native language format, rather than hosted in TS/JS
