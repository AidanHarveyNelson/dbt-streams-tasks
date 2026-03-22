-- Test that the table_from_ref table has data after initial creation.
-- The prep_structure macro inserts data into testing_ref before dbt run.
-- view_on_source reads from testing_ref, and table_from_ref reads from view_on_source.
-- This test FAILS (returns rows) if the table is empty.

WITH row_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ target.database }}.{{ target.schema }}.table_from_ref
)
SELECT cnt
FROM row_count
WHERE cnt = 0
