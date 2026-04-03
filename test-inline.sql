CREATE TABLE IF NOT EXISTS test (a Int32) ENGINE = Memory;
INSERT INTO test VALUES (10);
SELECT * FROM (
  SELECT * FROM test
  UNION ALL
  SELECT * FROM format(JSONEachRow, 'a Int32', '{"a": 20}\n{"a": 30}')
);
