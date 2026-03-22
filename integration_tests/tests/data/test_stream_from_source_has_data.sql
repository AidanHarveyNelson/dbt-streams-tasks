-- Test that the table_from_ref table has data after initial creation.
-- The prep_structure macro inserts data into testing_source before dbt run.
-- CREATE TABLE AS SELECT should populate the target with that data.
-- This test FAILS (returns rows) if the table is empty.

WITH row_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ target.database }}.{{ target.schema }}.table_from_source
)
SELECT cnt
FROM row_count
WHERE cnt = 0
