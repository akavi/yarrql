/**
 * Test script to verify generated SQL against Postgres
 *
 * Usage:
 *   1. Start postgres: docker run --name yarrql-pg -e POSTGRES_PASSWORD=test -p 5432:5432 -d postgres
 *   2. Wait a few seconds for it to start
 *   3. Seed the database: docker exec -i yarrql-pg psql -U postgres < test/seed.sql
 *   4. Run tests: npx tsx test/run.ts
 */

import { wrap } from "lodash"
import { Table } from "../src/ast"
import { toSql } from "../src/postgres"
import { Client } from "pg"

const client = new Client({
  host: "localhost",
  port: 5432,
  user: "postgres",
  password: "test",
  database: "postgres",
})

async function main() {
  await client.connect()
  console.log("Connected to Postgres\n")

  // Define tables matching seed.sql (lowercase to match Postgres default behavior)
  const Teachers = Table("teacher", { id: "uuid", name: "string" })
  const Students = Table("student", { id: "uuid", name: "string", age: "number" })
  const Classes = Table("class", { id: "uuid", name: "string", teacher_id: "uuid" })
  const Enrollments = Table("enrollment", { id: "uuid", student_id: "uuid", class_id: "uuid", grade: "number" })

  // Test 1: Simple filter - students over 20
  console.log("=== Test 1: Students over 20 ===")
  const adultStudents = Students.filter(s => s.age.gt(20))
  const sql1 = toSql(adultStudents.__node)
  console.log("SQL:", sql1)
  const res1 = await client.query(`SELECT * FROM ${sql1}`)
  console.log("Results:", res1.rows)
  console.log()

  // Test 2: Simple filter - students with pending grades (grade IS NULL)
  console.log("=== Test 2: Enrollments with pending grades ===")
  const pendingEnrollments = Enrollments.filter(e => e.grade.eq(null))
  const sql2 = toSql(pendingEnrollments.__node)
  console.log("SQL:", sql2)
  // Note: eq(null) might not generate IS NULL correctly, let's see
  try {
    const res2 = await client.query(`SELECT * FROM ${sql2}`)
    console.log("Results:", res2.rows)
  } catch (err: any) {
    console.log("Error:", err.message)
  }
  console.log()

  // Test 3: Count
  console.log("=== Test 3: Count of students ===")
  const studentCount = Students.count()
  const sql3 = `SELECT ${toSql(studentCount.__node as any)} as count`
  console.log("SQL:", sql3)
  try {
    const res3 = await client.query(sql3)
    console.log("Results:", res3.rows)
  } catch (err: any) {
    console.log("Error:", err.message)
  }
  console.log()

  // Test 4: Filter with AND
  console.log("=== Test 4: Students over 20 AND under 25 ===")
  const middleAged = Students.filter(s => s.age.gt(20).and(s.age.lt(25)))
  const sql4 = toSql(middleAged.__node)
  console.log("SQL:", sql4)
  const res4 = await client.query(`SELECT * FROM ${sql4}`)
  console.log("Results:", res4.rows)
  console.log()

  // Test 5: Limit
  console.log("=== Test 5: First 2 students ===")
  const firstTwo = Students.limit(2)
  const sql5 = toSql(firstTwo.__node)
  console.log("SQL:", sql5)
  const res5 = await client.query(`SELECT * FROM ${sql5}`)
  console.log("Results:", res5.rows)
  console.log()

  // Test 6: Teachers with mature classes (complex nested query from README)
  console.log("=== Test 6: Teachers with mature classes ===")
  console.log("Average ages per class:")
  const classAges = Classes.map(c => {
    const students = Students.filter(s =>
      Enrollments.any(e => e.student_id.eq(s.id).and(e.class_id.eq(c.id)))
    )
    return {
      id: c.id,
      name: c.name,
      averageAge: students.map(s => s.age).average(),
    }
  })
  const sql6a = toSql(classAges.__node)
  console.log("SQL:", sql6a)
  try {
    const res6a = await client.query(`SELECT * FROM ${sql6a}`)
    console.log("Results:", JSON.stringify(res6a.rows, null, 2))
  } catch (err: any) {
    console.log("Error:", err.message)
  }
  const teachersWithMatureClasses = Teachers.map(t => ({
    id: t.id,
    name: t.name,
    matureClasses: Classes.filter(c => {
      const students = Students.filter(s =>
        Enrollments.any(e => e.student_id.eq(s.id).and(e.class_id.eq(c.id)))
      )
      return c.teacher_id.eq(t.id).and(students.map(s => s.age).average().gt(21))
    }),
  })).filter(t => t.matureClasses.count().gt(0))
  const sql6 = toSql(teachersWithMatureClasses.__node)
  console.log("SQL:", sql6)
  try {
    const res6 = await client.query(`SELECT * FROM ${sql6}`)
    console.log("Results:", JSON.stringify(res6.rows, null, 2))
  } catch (err: any) {
    console.log("Error:", err.message)
  }
  console.log()

  // Test 7: Deeply nested query - 3 levels of nesting
  // Teachers -> Classes -> Students, then filter on nested counts
  // This tests whether we can access fields within JSON array elements
  console.log("=== Test 7: Deeply nested query ===")
  const teachersWithClassDetails = Teachers.map(t => ({
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

  // First, just test the mapping works
  console.log("--- Part A: Just the nested map ---")
  const sql7a = toSql(teachersWithClassDetails.__node)
  console.log("SQL:", sql7a)
  try {
    const res7a = await client.query(`SELECT * FROM ${sql7a}`)
    console.log("Results:", JSON.stringify(res7a.rows, null, 2))
  } catch (err: any) {
    console.log("Error:", err.message)
  }

  // Now filter: teachers who have at least one class with more than 1 student
  // This requires accessing c.students.count() where c is inside t.classes (a JSON array)
  console.log("--- Part B: Filter on deeply nested count ---")
  const teachersWithPopularClasses = teachersWithClassDetails
    .filter(t => t.classes.filter(c => c.students.count().gt(1)).count().gt(0))

  const sql7b = toSql(teachersWithPopularClasses.__node)
  console.log("SQL:", sql7b)
  try {
    const res7b = await client.query(`SELECT * FROM ${sql7b}`)
    console.log("Results:", JSON.stringify(res7b.rows, null, 2))
  } catch (err: any) {
    console.log("Error:", err.message)
  }
  console.log()

  // Test 8 Deeply nested query - 3 levels of nesting
  // Teachers -> Classes -> Students, then filter on nested counts
  // This tests whether we can access fields within JSON array elements
  console.log("=== Test 8: Sorting by nested counts ===")
  const sortedTeachers = Teachers.sort(t => {
    const classSizes = Classes
      .filter(c => c.teacher_id.eq(t.id))
      .map(c => Students.filter(s =>
        Enrollments.any(e => e.class_id.eq(c.id).and(e.student_id.eq(s.id)))
      ).count())
    return classSizes.max().desc()
  })

  const sql18 = toSql(sortedTeachers.__node)
  console.log("SQL:", sql18)
  try {
    const res8 = await client.query(`SELECT * FROM ${sql18}`)
    console.log("Results:", JSON.stringify(res8.rows, null, 2))
  } catch (err: any) {
    console.log("Error:", err.message)
  }

  const teachersLargestClassSize = sortedTeachers.map(t => {
    const largestClass = Classes
      .filter(c => c.teacher_id.eq(t.id))
      .map(c => ({
        count: Students.filter(s => Enrollments.any(e => e.class_id.eq(c.id).and(e.student_id.eq(s.id)))).count(),
        name: c.name,
      })).sort(c => c.count.desc()).first()
    return {
      id: t.id,
      name: t.name,
       largestClassName: largestClass.name,
      largestClassSize: largestClass.count,
    }
  })
  const sql19 = toSql(teachersLargestClassSize.__node)
  console.log("SQL:", sql19)
  try {
    const res9 = await client.query(`SELECT * FROM ${sql19}`)
    console.log("Results:", JSON.stringify(res9.rows, null, 2))
  } catch (err: any) {
    console.log("Error:", err.message)
  }

  await client.end()
  console.log("Done!")
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
