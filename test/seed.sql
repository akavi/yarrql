-- Schema
CREATE TABLE teacher (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL
);

CREATE TABLE student (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  age INT NOT NULL
);

CREATE TABLE Class (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  teacher_id TEXT REFERENCES teacher(id)
);

CREATE TABLE enrollment (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
  student_id TEXT REFERENCES student(id),
  class_id TEXT REFERENCES Class(id),
  grade INT
);

-- Seed data
INSERT INTO teacher (id, name) VALUES
  ('t1', 'Ms. Smith'),
  ('t2', 'Mr. Jones');

INSERT INTO student (id, name, age) VALUES
  ('s1', 'Alice', 22),
  ('s2', 'Bob', 19),
  ('s3', 'Charlie', 25),
  ('s4', 'Diana', 20);

INSERT INTO Class (id, name, teacher_id) VALUES
  ('c1', 'Math 101', 't1'),
  ('c2', 'English 101', 't2'),
  ('c3', 'Physics 101', 't1');

INSERT INTO enrollment (id, student_id, class_id, grade) VALUES
  ('e1', 's1', 'c1', 85),
  ('e2', 's1', 'c2', 92),
  ('e3', 's2', 'c1', NULL),  -- pending grade
  ('e4', 's3', 'c1', 95),
  ('e5', 's3', 'c3', 70),
  ('e6', 's4', 'c2', NULL);  -- pending grade
