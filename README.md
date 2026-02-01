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
 console.log(toSql(teachersWithStudents))
 ```

 While that's not terribly complicated to express in SQL, the following are much more so (at least, if you're as bad at SQL as I am)

 ```
 const teachersWithMatureClasses = Teachers.map(t => ({
   id: t.id
   matureClasses: Classes.filter(c => {
      const students = Students.filter(s => 
        Enrollments.any(e => e.student_id.eq(s.id).and(e.class_id.eq(c.id)))
      )
      return c.teacher_id.eq(t.id).and(students.map(s => s.age).average().gt(25))
   }),
 }))
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

LLMs can _also_ generate YARRQL. Include the following in your prompt:

<details>
<summary>LLM Prompt for YARRQL Generation</summary>

### YARRQL Query Language

YARRQL is a TypeScript query builder that treats tables as arrays you can `map` and `filter` over. It supports nested subqueries as column values.

#### Defining Tables

```typescript
const Students = Table("student", { id: "uuid", name: "string", age: "number" })
const Teachers = Table("teacher", { id: "uuid", name: "string" })
const Classes = Table("class", { id: "uuid", name: "string", teacher_id: "uuid" })
const Enrollments = Table("enrollment", { student_id: "uuid", class_id: "uuid", grade: "number" })
```

#### Core Pattern

All queries follow this pattern: start with a table, chain operations, access fields via callback parameters.

```typescript
// Filter: keep rows matching condition
Students.filter(s => s.age.gt(20))

// Map: transform each row (ALWAYS use map for output shape)
Students.map(s => ({ name: s.name, age: s.age }))

// Chain operations
Students.filter(s => s.age.gt(20)).map(s => ({ name: s.name }))
```

#### Field Access & Comparisons

Fields are accessed via the callback parameter. Comparisons return boolean builders:

```typescript
s.age.gt(20)           // age > 20
s.age.gte(20)          // age >= 20
s.age.lt(20)           // age < 20
s.age.lte(20)          // age <= 20
s.age.eq(20)           // age = 20
s.age.neq(20)          // age != 20
s.name.eq("Alice")     // name = 'Alice'
s.grade.eq(null)       // grade IS NULL
```

#### Boolean Logic

Chain with `.and()`, `.or()`, negate with `.not()`:

```typescript
s.age.gt(20).and(s.age.lt(30))
s.name.eq("Alice").or(s.name.eq("Bob"))
s.active.not()
```

#### String Operations

```typescript
s.name.contains("ali")      // LIKE '%ali%'
s.name.startsWith("A")      // LIKE 'A%'
s.name.endsWith("ce")       // LIKE '%ce'
s.name.like("A%e")          // LIKE 'A%e'
s.name.toLowerCase()        // LOWER(name)
s.name.toUpperCase()        // UPPER(name)
s.name.concat(" Jr.")       // name || ' Jr.'
s.name.length()             // LENGTH(name) -> number
```

#### Arithmetic

```typescript
s.age.plus(1)
s.age.minus(5)
s.score.plus(s.bonus)
```

#### Aggregations (on arrays)

```typescript
Students.count()                        // count all
Students.filter(s => s.age.gt(20)).count()  // count filtered
Students.map(s => s.age).average()      // AVG(age)
Students.map(s => s.age).sum()          // SUM(age)
Students.map(s => s.age).max()          // MAX(age)
Students.map(s => s.age).min()          // MIN(age)
```

#### Existence Checks

```typescript
// any: true if at least one row matches
Enrollments.any(e => e.student_id.eq(s.id))

// every: true if all rows match
Enrollments.every(e => e.grade.gte(60))
```

#### Sorting

```typescript
Students.sort(s => s.age)              // ascending
Students.sort(s => s.age.desc())       // descending
Students.sort(s => s.name.desc())      // strings too
```

#### Pagination

```typescript
Students.sort(s => s.age).limit(10)
Students.sort(s => s.age).offset(20).limit(10)
```

#### First Element

```typescript
// .first() returns a single row, not an array
const oldest = Students.sort(s => s.age.desc()).first()
// Access fields directly on result
oldest.name
oldest.age
```

#### Nested Subqueries (Key Feature!)

Embed related data as nested arrays:

```typescript
// Teachers with their classes
Teachers.map(t => ({
  id: t.id,
  name: t.name,
  classes: Classes.filter(c => c.teacher_id.eq(t.id))
}))

// Teachers with classes AND students in each class
Teachers.map(t => ({
  id: t.id,
  name: t.name,
  classes: Classes.filter(c => c.teacher_id.eq(t.id)).map(c => ({
    id: c.id,
    name: c.name,
    students: Students.filter(s =>
      Enrollments.any(e => e.class_id.eq(c.id).and(e.student_id.eq(s.id)))
    )
  }))
}))
```

#### Filtering by Nested Properties

```typescript
// Teachers who have at least one class
Teachers.map(t => ({
  id: t.id,
  classes: Classes.filter(c => c.teacher_id.eq(t.id))
})).filter(t => t.classes.count().gt(0))

// Classes with average student age > 21
Classes.map(c => ({
  id: c.id,
  name: c.name,
  avgAge: Students.filter(s =>
    Enrollments.any(e => e.class_id.eq(c.id).and(e.student_id.eq(s.id)))
  ).map(s => s.age).average()
})).filter(c => c.avgAge.gt(21))
```

#### Complete Examples

```typescript
// Students with pending grades (grade IS NULL)
Students.map(s => ({
  id: s.id,
  name: s.name,
  pendingClasses: Classes.filter(c =>
    Enrollments.any(e =>
      e.student_id.eq(s.id).and(e.class_id.eq(c.id)).and(e.grade.eq(null))
    )
  )
})).filter(s => s.pendingClasses.count().gt(0))

// Top 5 students by total grade points
Students.map(s => ({
  id: s.id,
  name: s.name,
  totalPoints: Enrollments.filter(e => e.student_id.eq(s.id)).map(e => e.grade).sum()
})).sort(s => s.totalPoints.desc()).limit(5)

// Teachers ranked by their largest class size
Teachers.map(t => ({
  id: t.id,
  name: t.name,
  largestClassSize: Classes
    .filter(c => c.teacher_id.eq(t.id))
    .map(c => Enrollments.filter(e => e.class_id.eq(c.id)).count())
    .max()
})).sort(t => t.largestClassSize.desc())
```

#### Key Rules

1. **Always use `.map()` to define output shape** - don't just return raw tables
2. **Field access requires the callback parameter** - write `s.age`, not `age`
3. **Comparisons are method calls** - write `s.age.gt(20)`, not `s.age > 20`
4. **Null checks use `.eq(null)`** - not `=== null` or `.isNull()`
5. **Nested queries go inside `.map()` callbacks** - they can reference outer scope
6. **Use `.any()` for "exists" checks** - `OtherTable.any(x => condition)`
7. **Chain `.filter()` before `.map()`** when you want to filter then transform

</details>


## ToDo

As the long list here suggests, this is pre-pre-alpha. Not fit for actual production use anywhere

1. Allow querying on tables without specifying their schema (at the cost of type safety)
2. A Chrome plugin to generate SQL from YARRQL on the fly
3. Support for more than just Postgres dialect SQL
4. (one day?) A native language format, rather than hosted in TS/JS

## Name

Honestly I thought of the logo first, and then backronymed the name to fit it.
