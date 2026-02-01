/**
 * Test script to verify generated SQL against Postgres
 *
 * Usage:
 *   1. Start postgres: docker run --name yarrql-pg -e POSTGRES_PASSWORD=test -p 5432:5432 -d postgres
 *   2. Wait a few seconds for it to start
 *   3. Seed the database: docker exec -i yarrql-pg psql -U postgres < test/seed.sql
 *   4. Run tests: npx tsx test/run.ts
 */
export {};
