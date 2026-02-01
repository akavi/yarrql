/**
 * Verify README examples compile and generate SQL
 * Run with: npx tsx src/examples.ts
 */
import { toSql } from "./postgres"

// We need to export Table from ast.ts - let me check what's available
// For now, we'll work directly with the ArrayBuilder

// First, let's check if the basic types work by importing them
import type { Schema, Query } from "./ast"

console.log("✓ Imports work")

// To properly test the README examples, we need a Table() function exported from ast.ts
// Let's check what's currently exported and create a minimal test

console.log("\nNote: To fully test README examples, ast.ts needs to export a Table() constructor.")
console.log("Currently only types (Schema, Query, Expr) and unreachable() are exported.")
