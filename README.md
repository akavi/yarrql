<img src="https://github.com/user-attachments/assets/35982f58-7769-4630-9e20-b4d2bde57ded" width="100" />
# YARRQL: Yet Another Recursive Relational Query Language

Let's face it, SQL sucks. It's the only vestige (for most of us) of the COBOL era of languages, and it shows its age.
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
       ).averageOf(s => {age: s.age})
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
 const classesWithWideGradeRange = Classes.project(c => ({
   id: c.id
   highestGrade: grades(c).max()
   lowestGrade: grades(c).min()
 })).where(c => c.highestGrade.minus(c.lowestGrade).gte(20));
 ```

## What this is not

YARRQL is _not_ an ORM like ActiveRecord or DjangoORM or Sequelize. It's a query language, what you do with the data returned by it (using it directly or to hydrate objects) is up to you

YARRQL is also _not_ a DSL for constructing SQL queries in javascript like Knex, Drizzle, or Kysely. It's a different _query paradigm_ like JS is to Java (that's to say, not revolutionary, but still better :wink: )

## ToDo

As the long list here suggests, this is pre-pre-pre-pre-alpha. Not fit for actual production use anywhere

1. Add support for "simple" arrays (eg, if a table is typed `Array<{...fields}>`, constructs like `Array<Scalar>`
2. Add support for nested objects (eg, if a table is typed `Array{...fields}>`, constructs like `{..fields}` and `{field: {...subFields}}`
3. Allow querying on tables without specifying their schema (at the cost of type safety)
4. A Chrome plugin to generate SQL from YARRQL on the fly
5. Support for more than just Postgres dialect SQL
6. (one day?) A native language format, rather than hosted in TS/JS
