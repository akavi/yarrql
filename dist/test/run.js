"use strict";
/**
 * Test script to verify generated SQL against Postgres
 *
 * Usage:
 *   1. Start postgres: docker run --name yarrql-pg -e POSTGRES_PASSWORD=test -p 5432:5432 -d postgres
 *   2. Wait a few seconds for it to start
 *   3. Seed the database: docker exec -i yarrql-pg psql -U postgres < test/seed.sql
 *   4. Run tests: npx tsx test/run.ts
 */
Object.defineProperty(exports, "__esModule", { value: true });
const ast_1 = require("../src/ast");
const postgres_1 = require("../src/postgres");
const pg_1 = require("pg");
const client = new pg_1.Client({
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "test",
    database: "postgres",
});
async function main() {
    await client.connect();
    console.log("Connected to Postgres\n");
    // Define tables matching seed.sql (lowercase to match Postgres default behavior)
    const Teachers = (0, ast_1.Table)("teacher", { id: "uuid", name: "string" });
    const Students = (0, ast_1.Table)("student", { id: "uuid", name: "string", age: "number" });
    const Classes = (0, ast_1.Table)("class", { id: "uuid", name: "string", teacher_id: "uuid" });
    const Enrollments = (0, ast_1.Table)("enrollment", { id: "uuid", student_id: "uuid", class_id: "uuid", grade: "number" });
    // Test 1: Simple filter - students over 20
    console.log("=== Test 1: Students over 20 ===");
    const adultStudents = Students.filter(s => s.age.gt(20));
    const sql1 = (0, postgres_1.toSql)(adultStudents.node);
    console.log("SQL:", sql1);
    const res1 = await client.query(`SELECT * FROM ${sql1}`);
    console.log("Results:", res1.rows);
    console.log();
    // Test 2: Simple filter - students with pending grades (grade IS NULL)
    console.log("=== Test 2: Enrollments with pending grades ===");
    const pendingEnrollments = Enrollments.filter(e => e.grade.eq(null));
    const sql2 = (0, postgres_1.toSql)(pendingEnrollments.node);
    console.log("SQL:", sql2);
    // Note: eq(null) might not generate IS NULL correctly, let's see
    try {
        const res2 = await client.query(`SELECT * FROM ${sql2}`);
        console.log("Results:", res2.rows);
    }
    catch (err) {
        console.log("Error:", err.message);
    }
    console.log();
    // Test 3: Count
    console.log("=== Test 3: Count of students ===");
    const studentCount = Students.count();
    const sql3 = `SELECT ${(0, postgres_1.toSql)(studentCount.node)} as count`;
    console.log("SQL:", sql3);
    try {
        const res3 = await client.query(sql3);
        console.log("Results:", res3.rows);
    }
    catch (err) {
        console.log("Error:", err.message);
    }
    console.log();
    // Test 4: Filter with AND
    console.log("=== Test 4: Students over 20 AND under 25 ===");
    const middleAged = Students.filter(s => s.age.gt(20).and(s.age.lt(25)));
    const sql4 = (0, postgres_1.toSql)(middleAged.node);
    console.log("SQL:", sql4);
    const res4 = await client.query(`SELECT * FROM ${sql4}`);
    console.log("Results:", res4.rows);
    console.log();
    // Test 5: Limit
    console.log("=== Test 5: First 2 students ===");
    const firstTwo = Students.limit(2);
    const sql5 = (0, postgres_1.toSql)(firstTwo.node);
    console.log("SQL:", sql5);
    const res5 = await client.query(`SELECT * FROM ${sql5}`);
    console.log("Results:", res5.rows);
    console.log();
    // Test 6: Teachers with mature classes (complex nested query from README)
    console.log("=== Test 6: Teachers with mature classes ===");
    const teachersWithMatureClasses = Teachers.map(t => ({
        id: t.id,
        matureClasses: Classes.filter(c => {
            const students = Students.filter(s => Enrollments.any(e => e.student_id.eq(s.id).and(e.class_id.eq(c.id))));
            return c.teacher_id.eq(t.id).and(students.map(s => s.age).average().gt(20));
        }),
    }));
    const sql6 = (0, postgres_1.toSql)(teachersWithMatureClasses.node);
    console.log("SQL:", sql6);
    try {
        const res6 = await client.query(`SELECT * FROM ${sql6}`);
        console.log("Results:", JSON.stringify(res6.rows, null, 2));
    }
    catch (err) {
        console.log("Error:", err.message);
    }
    console.log();
    await client.end();
    console.log("Done!");
}
main().catch(err => {
    console.error(err);
    process.exit(1);
});
