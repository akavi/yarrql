import { toSql } from './postgres';
import { Table } from './ast';
import type { Query, Schema } from './ast';

describe('postgres.ts - SQL Generation', () => {
  // Define test schemas
  const studentSchema = {
    id: 'uuid' as const,
    name: 'string' as const,
    age: 'number' as const,
  };

  const teacherSchema = {
    id: 'uuid' as const,
    name: 'string' as const,
  };

  const classSchema = {
    id: 'uuid' as const,
    name: 'string' as const,
    teacher_id: 'uuid' as const,
  };

  const enrollmentSchema = {
    id: 'uuid' as const,
    student_id: 'uuid' as const,
    class_id: 'uuid' as const,
    grade: 'number' as const,
  };

  describe('Table queries', () => {
    it('should generate SQL for a simple table query', () => {
      const Students = Table('students', studentSchema);
      const sql = toSql(Students.toAST());

      expect(sql).toContain('SELECT "id", "name", "age" FROM "students"');
      expect(sql).toContain('AS _t0');
    });

    it('should generate SQL for multiple columns', () => {
      const Teachers = Table('teachers', teacherSchema);
      const sql = toSql(Teachers.toAST());

      expect(sql).toContain('SELECT "id", "name" FROM "teachers"');
      expect(sql).toContain('AS _t0');
    });
  });

  describe('Filter queries', () => {
    it('should generate SQL for a simple filter with equality', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.age.eq(20).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('WHERE');
      expect(sql).toContain('= 20');
    });

    it('should generate SQL for filter with greater than', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.age.gt(18).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('WHERE');
      expect(sql).toContain('> 18');
    });

    it('should generate SQL for filter with less than', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.age.lt(25).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('WHERE');
      expect(sql).toContain('< 25');
    });

    it('should generate SQL for filter with AND logic', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s =>
        s.age.gt(18).and(s.age.lt(30).node).node
      );
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('WHERE');
      expect(sql).toContain('AND');
      expect(sql).toContain('> 18');
      expect(sql).toContain('< 30');
    });

    it('should generate SQL for filter with OR logic', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s =>
        s.age.lt(18).or(s.age.gt(65).node).node
      );
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('WHERE');
      expect(sql).toContain('OR');
      expect(sql).toContain('< 18');
      expect(sql).toContain('> 65');
    });

    it('should generate SQL for filter with NOT logic', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.age.eq(20).not().node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('WHERE');
      expect(sql).toContain('NOT');
    });

    it('should handle string values with proper escaping', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.name.eq("John").node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain("'John'");
    });

    it('should handle string values with single quotes (escaping)', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.name.eq("O'Brien").node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain("'O''Brien'");
    });

    it('should handle boolean values', () => {
      const schema = { id: 'uuid' as const, active: 'boolean' as const };
      const Users = Table('users', schema);
      const filtered = Users.filter(u => u.active.eq(true).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('TRUE');
    });

    it('should handle null values', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.name.eq(null).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('NULL');
    });
  });

  describe('Map/Projection queries', () => {
    it('should generate SQL for simple map projection', () => {
      const Students = Table('students', studentSchema);
      const mapped = Students.map(s => ({ studentId: s.id, studentName: s.name }));
      const sql = toSql(mapped.toAST());

      expect(sql).toContain('AS "studentId"');
      expect(sql).toContain('AS "studentName"');
    });

    it('should generate SQL for map with all columns', () => {
      const Students = Table('students', studentSchema);
      const mapped = Students.map(s => s);
      const sql = toSql(mapped.toAST());

      expect(sql).toContain('"id"');
      expect(sql).toContain('"name"');
      expect(sql).toContain('"age"');
    });

    it('should generate SQL for map with computed expressions', () => {
      const Students = Table('students', studentSchema);
      const mapped = Students.map(s => ({
        id: s.id,
        ageNextYear: s.age.plus(1)
      }));
      const sql = toSql(mapped.toAST());

      expect(sql).toContain('AS "ageNextYear"');
      expect(sql).toContain('+ 1');
    });

    it('should handle map with table-valued columns', () => {
      const Students = Table('students', studentSchema);
      const Classes = Table('classes', classSchema);

      // Using any type to bypass strict typing for testing the SQL generation
      const query: any = {
        type: 'map',
        source: Students.toAST(),
        mapped: {
          id: { type: 'expr_column', source: Students.toAST(), column: 'id' },
          classes: Classes.toAST()
        }
      };

      const sql = toSql(query);
      expect(sql).toContain('json_agg');
      expect(sql).toContain('COALESCE');
    });
  });

  describe('OrderBy queries', () => {
    it('should generate SQL for ascending order', () => {
      const Students = Table('students', studentSchema);
      const ordered = Students.orderBy(s => [s.age.asc().toAST()]);
      const sql = toSql(ordered.toAST());

      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('asc');
    });

    it('should generate SQL for descending order', () => {
      const Students = Table('students', studentSchema);
      const ordered = Students.orderBy(s => [s.age.desc().toAST()]);
      const sql = toSql(ordered.toAST());

      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('desc');
    });

    it('should generate SQL for multiple order specifications', () => {
      const Students = Table('students', studentSchema);
      const ordered = Students.orderBy(s => [
        s.age.desc().toAST(),
        s.name.asc().toAST()
      ]);
      const sql = toSql(ordered.toAST());

      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('desc');
      expect(sql).toContain('asc');
      expect(sql).toContain(',');
    });
  });

  describe('Limit queries', () => {
    it('should generate SQL for limit', () => {
      const Students = Table('students', studentSchema);
      const limited = Students.limit(10);
      const sql = toSql(limited.toAST());

      expect(sql).toContain('LIMIT 10');
    });

    it('should generate SQL for limit with filter', () => {
      const Students = Table('students', studentSchema);
      const query = Students.filter(s => s.age.gt(18).node).limit(5);
      const sql = toSql(query.toAST());

      expect(sql).toContain('LIMIT 5');
      expect(sql).toContain('WHERE');
    });
  });

  describe('Offset queries', () => {
    it('should generate SQL for offset', () => {
      const Students = Table('students', studentSchema);
      const offsetQuery = Students.offset(10);
      const sql = toSql(offsetQuery.toAST());

      expect(sql).toContain('OFFSET 10');
    });

    it('should generate SQL for offset with limit', () => {
      const Students = Table('students', studentSchema);
      const query = Students.limit(10).offset(20);
      const sql = toSql(query.toAST());

      expect(sql).toContain('LIMIT 10');
      expect(sql).toContain('OFFSET 20');
    });
  });

  describe('Set operations', () => {
    it('should generate SQL for UNION', () => {
      const Students1 = Table('students1', studentSchema);
      const Students2 = Table('students2', studentSchema);
      const unioned = Students1.union(Students2);
      const sql = toSql(unioned.toAST());

      expect(sql).toContain('UNION');
      expect(sql).toContain('students1');
      expect(sql).toContain('students2');
    });

    it('should generate SQL for INTERSECT', () => {
      const Students1 = Table('students1', studentSchema);
      const Students2 = Table('students2', studentSchema);
      const intersected = Students1.intersect(Students2);
      const sql = toSql(intersected.toAST());

      expect(sql).toContain('INTERSECT');
    });

    it('should generate SQL for DIFFERENCE', () => {
      const Students1 = Table('students1', studentSchema);
      const Students2 = Table('students2', studentSchema);
      const differenced = Students1.difference(Students2);
      const sql = toSql(differenced.toAST());

      expect(sql).toContain('DIFFERENCE');
    });
  });

  describe('GroupBy queries', () => {
    it('should generate SQL for group by', () => {
      const Students = Table('students', studentSchema);
      const grouped = Students.groupBy(s => s.age.node);
      const sql = toSql(grouped.toAST());

      expect(sql).toContain('GROUP BY');
      expect(sql).toContain('json_agg');
      expect(sql).toContain('AS key');
      expect(sql).toContain('AS values');
    });
  });

  describe('Aggregate functions', () => {
    it('should generate SQL for count', () => {
      const Students = Table('students', studentSchema);
      const query = Students.map(s => ({
        totalStudents: Students.count()
      }));
      const sql = toSql(query.toAST());

      expect(sql).toContain('COUNT(*)');
    });

    it('should generate SQL for average', () => {
      const Students = Table('students', studentSchema);
      const query = Students.map(s => ({
        avgAge: Students.average()
      }));
      const sql = toSql(query.toAST());

      expect(sql).toContain('AVG');
    });

    it('should generate SQL for max', () => {
      const Students = Table('students', studentSchema);
      const query = Students.map(s => ({
        maxAge: Students.max()
      }));
      const sql = toSql(query.toAST());

      expect(sql).toContain('MAX');
    });

    it('should generate SQL for min', () => {
      const Students = Table('students', studentSchema);
      const query = Students.map(s => ({
        minAge: Students.min()
      }));
      const sql = toSql(query.toAST());

      expect(sql).toContain('MIN');
    });

    it('should generate SQL for any (existential quantification)', () => {
      const Students = Table('students', studentSchema);
      const Enrollments = Table('enrollments', enrollmentSchema);

      const hasEnrollments = Students.filter(s =>
        Enrollments.any(e => e.student_id.eq(s.id.node).node).node
      );
      const sql = toSql(hasEnrollments.toAST());

      expect(sql).toContain('COUNT(*)');
      expect(sql).toContain('> 0');
    });

    it('should generate SQL for every (universal quantification)', () => {
      const Students = Table('students', studentSchema);
      const Enrollments = Table('enrollments', enrollmentSchema);

      const allEnrolled = Students.filter(s =>
        Enrollments.every(e => e.grade.gt(50).node).node
      );
      const sql = toSql(allEnrolled.toAST());

      expect(sql).toContain('COUNT(*)');
      expect(sql).toContain('= 0');
      expect(sql).toContain('NOT');
    });
  });

  describe('Binary operations', () => {
    it('should generate SQL for plus operation', () => {
      const Students = Table('students', studentSchema);
      const query = Students.map(s => ({
        ageInFiveYears: s.age.plus(5)
      }));
      const sql = toSql(query.toAST());

      expect(sql).toContain('+ 5');
    });

    it('should generate SQL for minus operation', () => {
      const Students = Table('students', studentSchema);
      const query = Students.map(s => ({
        ageFiveYearsAgo: s.age.minus(5)
      }));
      const sql = toSql(query.toAST());

      expect(sql).toContain('- 5');
    });

    it('should generate SQL for greater than or equal', () => {
      const schema = { id: 'uuid' as const, score: 'number' as const };
      const Scores = Table('scores', schema);

      // Direct construction to test gte
      const directQuery: Query<typeof schema> = {
        type: 'filter',
        source: Scores.toAST(),
        filter: {
          type: 'binary_op',
          op: 'gte',
          left: { type: 'expr_column', source: Scores.toAST(), column: 'score' },
          right: { type: 'value', value: 50 }
        }
      };
      const sql = toSql(directQuery);

      expect(sql).toContain('>=');
    });

    it('should generate SQL for less than or equal', () => {
      const schema = { id: 'uuid' as const, score: 'number' as const };
      const Scores = Table('scores', schema);

      const directQuery: Query<typeof schema> = {
        type: 'filter',
        source: Scores.toAST(),
        filter: {
          type: 'binary_op',
          op: 'lte',
          left: { type: 'expr_column', source: Scores.toAST(), column: 'score' },
          right: { type: 'value', value: 100 }
        }
      };
      const sql = toSql(directQuery);

      expect(sql).toContain('<=');
    });

    it('should generate SQL for not equal', () => {
      const Students = Table('students', studentSchema);

      const directQuery: Query<typeof studentSchema> = {
        type: 'filter',
        source: Students.toAST(),
        filter: {
          type: 'binary_op',
          op: 'ne',
          left: { type: 'expr_column', source: Students.toAST(), column: 'age' },
          right: { type: 'value', value: 25 }
        }
      };
      const sql = toSql(directQuery);

      expect(sql).toContain('<>');
    });
  });

  describe('Complex nested queries', () => {
    it('should handle deeply nested filters with subqueries', () => {
      const Students = Table('students', studentSchema);
      const Classes = Table('classes', classSchema);
      const Enrollments = Table('enrollments', enrollmentSchema);

      const studentsInMathClasses = Students.filter(s =>
        Classes.any(c =>
          c.name.eq("Math").and(
            Enrollments.any(e =>
              e.student_id.eq(s.id.node).and(e.class_id.eq(c.id.node).node).node
            ).node
          ).node
        ).node
      );

      const sql = toSql(studentsInMathClasses.toAST());
      expect(sql).toContain('WHERE');
      expect(sql).toContain('COUNT(*)');
      expect(sql).toContain("'Math'");
    });

    it('should handle map with multiple nested subqueries', () => {
      const Students = Table('students', studentSchema);
      const Enrollments = Table('enrollments', enrollmentSchema);

      const studentsWithStats = Students.map(s => ({
        id: s.id,
        name: s.name,
        totalEnrollments: Enrollments.filter(e => e.student_id.eq(s.id.node).node).count(),
        averageGrade: Enrollments.filter(e => e.student_id.eq(s.id.node).node).averageOf(e => e.grade.node)
      }));

      const sql = toSql(studentsWithStats.toAST());
      expect(sql).toContain('COUNT(*)');
      expect(sql).toContain('AVG');
    });
  });

  describe('Edge cases', () => {
    it('should handle empty logical operations (AND)', () => {
      const directQuery: Query<typeof studentSchema> = {
        type: 'filter',
        source: Table('students', studentSchema).toAST(),
        filter: { type: 'logical_op', op: 'and', args: [] }
      };
      const sql = toSql(directQuery);

      expect(sql).toContain('TRUE');
    });

    it('should handle empty logical operations (OR)', () => {
      const directQuery: Query<typeof studentSchema> = {
        type: 'filter',
        source: Table('students', studentSchema).toAST(),
        filter: { type: 'logical_op', op: 'or', args: [] }
      };
      const sql = toSql(directQuery);

      expect(sql).toContain('FALSE');
    });

    it('should handle query_column type', () => {
      const nestedSchema = {
        id: 'uuid' as const,
        details: {
          name: 'string' as const,
          age: 'number' as const
        }
      };

      const Users = Table('users', nestedSchema);
      const query: Query<{ name: 'string'; age: 'number' }> = {
        type: 'query_column',
        source: Users.toAST(),
        column: 'details'
      };

      const sql = toSql(query);
      expect(sql).toContain('SELECT "details"');
    });

    it('should generate unique aliases for nested queries', () => {
      const Students = Table('students', studentSchema);
      const filtered1 = Students.filter(s => s.age.gt(18).node);
      const filtered2 = filtered1.filter(s => s.age.lt(30).node);
      const filtered3 = filtered2.filter(s => s.name.eq("John").node);

      const sql = toSql(filtered3.toAST());

      // Should have multiple table aliases
      expect(sql).toContain('_t0');
      // Nested filters should create multiple SELECT statements
      const selectCount = (sql.match(/SELECT/g) || []).length;
      expect(selectCount).toBeGreaterThan(1);
    });
  });

  describe('Value type handling', () => {
    it('should handle number values', () => {
      const Students = Table('students', studentSchema);
      const filtered = Students.filter(s => s.age.eq(42).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('= 42');
      expect(sql).not.toContain("'42'"); // Should not be quoted
    });

    it('should handle boolean true', () => {
      const schema = { id: 'uuid' as const, active: 'boolean' as const };
      const Users = Table('users', schema);
      const filtered = Users.filter(u => u.active.eq(true).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('TRUE');
    });

    it('should handle boolean false', () => {
      const schema = { id: 'uuid' as const, active: 'boolean' as const };
      const Users = Table('users', schema);
      const filtered = Users.filter(u => u.active.eq(false).node);
      const sql = toSql(filtered.toAST());

      expect(sql).toContain('FALSE');
    });
  });
});
