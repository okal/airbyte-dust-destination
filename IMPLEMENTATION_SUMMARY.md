# Tabular Data Support Implementation Summary

## Overview

Successfully implemented tabular data support for the Dust destination connector, allowing users to choose between document-based and table-based data storage formats.

## Changes Made

### 1. Configuration (`spec.json`)
- Added `data_format` field: `"documents"` (default) or `"tables"`
- Added `table_id_prefix` field: Prefix for table names (default: `"airbyte_"`)

### 2. Client Extensions (`client.py`)
- Added `_tables_base` URL construction (uses `data_sources` with underscores, not hyphens)
- Extended `check_connection()` to accept `data_format` parameter
- Added `upsert_table()` method: Creates/updates table definitions
- Added `upsert_rows()` method: Batch upserts rows into tables

### 3. Destination Logic (`destination.py`)
- Refactored `write()` to branch on `data_format`
- Added `_write_documents()`: Original document-based logic (unchanged)
- Added `_write_tables()`: New table-based logic with batching
- Added helper methods:
  - `_build_table_id()`: Sanitizes table IDs
  - `_infer_column_type()`: Maps Python types to Dust column types
  - `_update_schema()`: Builds schema from records
  - `_flatten_record()`: Converts nested objects/arrays to JSON strings
  - `_ensure_table_exists()`: Creates table with inferred schema
  - `_flush_table_batches()`: Flushes pending rows

### 4. Features
- **Batching**: Rows are batched (500 per request) for efficiency
- **Schema Inference**: Automatically infers column types from data
- **State Handling**: Flushes pending rows before yielding STATE messages
- **Backward Compatibility**: Defaults to "documents" mode if not specified

### 5. Tests (`test_destination.py`)
- Added tests for table ID building
- Added tests for schema inference
- Added tests for record flattening
- Added tests for tables mode write operations
- Updated check connection tests to verify data_format parameter

## API Assumptions

Since exact API specifications weren't available, the implementation makes reasonable assumptions:

1. **Table Creation** (`POST /tables`):
   ```json
   {
     "id": "table_id",
     "name": "table_name",
     "description": "optional",
     "columns": [{"name": "col", "type": "string"}]
   }
   ```

2. **Row Upsert** (`POST /tables/{tId}/rows`):
   ```json
   {
     "rows": [{"col1": "value1", "col2": 42}]
   }
   ```

3. **Column Types**: Uses `"string"`, `"number"`, `"boolean"`, `"json"`

## Usage

### Documents Mode (Default)
```json
{
  "api_key": "sk-...",
  "workspace_id": "w1",
  "space_id": "s1",
  "data_source_id": "ds1"
}
```

### Tables Mode
```json
{
  "api_key": "sk-...",
  "workspace_id": "w1",
  "space_id": "s1",
  "data_source_id": "ds1",
  "data_format": "tables",
  "table_id_prefix": "airbyte_"
}
```

## Next Steps

1. **API Validation**: Test against actual Dust API to verify request/response formats
2. **Primary Keys**: Investigate how Dust handles primary keys in tables (may need API updates)
3. **Schema Evolution**: Handle schema changes gracefully (add new columns, handle missing columns)
4. **Error Handling**: Refine error handling for batch failures
5. **Performance Tuning**: Adjust batch size based on API limits and performance

## Notes

- Nested objects and arrays are serialized to JSON strings in table rows
- Table names are sanitized to be URL-safe
- Schema is inferred from the first records seen for each stream
- All rows for a stream are flushed before STATE messages are yielded
