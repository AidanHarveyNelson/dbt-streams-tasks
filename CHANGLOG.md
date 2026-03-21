# Changelog

## v0.0.2 (2026-09-03)

- Feature: Rewrite stream_task.sql materialization with proper step ordering: temp view → target table → stream → task → resume task
- Feature: Simplify stream.sql to always set src_table config and return the source relation for column discovery
- Feature: Fix create_table.sql get_missing_columns argument order
- Feature: Rewrite create_task.sql with clean MERGE formatting and proper parameter handling
- Feature: Add 8 integration tests covering table/stream/task existence and data validation
- Feature: Add prep_structure, insert_data, and cleanup helper macros
- Feature: Update sample models to use correct config structure
- Feature: Update run_test.sh with --full-refresh and clear step logging
- Feature: Remove non-Snowflake adapter dependencies from pyproject.toml
- Feature: Update README with usage guide and config reference
- Feature: Expanded integration tests to cover all use cases


## v0.0.1 (2025-09-03)

- Feature: Initial implementation logic
