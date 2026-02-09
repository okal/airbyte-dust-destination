# Plan: Tabular Data Support for Dust Destination Connector

## Executive Summary

This document outlines a plan to add support for writing Airbyte records as **tabular data** (tables/rows) to Dust, in addition to the existing document-based approach. The key question is whether this should be implemented as:
1. **Same connector** with a configuration option (recommended)
2. **Separate connector** (alternative)

**Recommendation: Same connector with a configuration option** (`data_format: "documents" | "tables"`)

## Current Architecture

### Document-Based Approach (Current)
- Each Airbyte record → One Dust document
- Endpoint: `POST /api/v1/w/{wId}/spaces/{spaceId}/data-sources/{dsId}/documents/{documentId}`
- Data stored as JSON text in document body
- Good for: Unstructured data, text-heavy records, semantic search

### Tabular Approach (Proposed)
- Each Airbyte stream → One Dust table
- Each Airbyte record → One row in the table
- Endpoints:
  - `POST /api/v1/w/{wId}/spaces/{spaceId}/data-sources/{dsId}/tables` (create/upsert table)
  - `POST /api/v1/w/{wId}/spaces/{spaceId}/data-sources/{dsId}/tables/{tId}/rows` (upsert rows)
- Good for: Structured data, database tables, CSV-like data, analytics

## API Analysis

### Tables API Endpoints

1. **Upsert Table** (`POST /tables`)
   - Creates or updates a table definition
   - Requires: Table name, schema (columns)
   - Returns: Table ID (`tId`)

2. **Upsert Rows** (`POST /tables/{tId}/rows`)
   - Upserts rows into an existing table
   - Requires: Array of row objects
   - Supports batch operations (multiple rows per request)

### Key Differences from Documents API

| Aspect | Documents | Tables |
|--------|-----------|--------|
| **Data Model** | One record = one document | One stream = one table, one record = one row |
| **Schema** | Flexible JSON | Fixed columns (defined at table creation) |
| **Batching** | One-by-one | Can batch multiple rows |
| **Query Model** | Semantic search | SQL-like queries |
| **Use Case** | Unstructured content | Structured relational data |

## Implementation Plan

### Option 1: Same Connector with Configuration (Recommended)

**Pros:**
- Single codebase to maintain
- Users can choose format per use case
- Shared authentication/connection logic
- Easier to support both formats in future

**Cons:**
- More complex code paths
- Need to handle both formats in tests

**Configuration Changes:**

```json
{
  "api_key": "...",
  "workspace_id": "...",
  "space_id": "...",
  "data_source_id": "...",
  "base_url": "https://dust.tt",
  "data_format": "documents" | "tables",  // NEW
  "table_id_prefix": "airbyte_"  // Optional, for tables mode
}
```

**Implementation Steps:**

1. **Extend `spec.json`**
   - Add `data_format` enum field (default: "documents" for backward compatibility)
   - Add optional `table_id_prefix` field

2. **Extend `DustClient`**
   - Add `upsert_table()` method
   - Add `upsert_rows()` method (with batching support)
   - Modify `check_connection()` to validate based on format

3. **Modify `DestinationDust.write()`**
   - Branch on `config["data_format"]`
   - **Documents path**: Existing logic (unchanged)
   - **Tables path**: New logic:
     - Group records by stream
     - For each stream:
       - Infer schema from first record (or use stream schema if available)
       - Upsert table definition
       - Batch rows and upsert in chunks

4. **Schema Inference**
   - Extract column names from record keys
   - Map Airbyte types to Dust column types
   - Handle nested objects (flatten or JSON column?)

5. **Row Batching**
   - Collect rows per stream
   - Batch size: 100-1000 rows (configurable?)
   - Upsert batches, yield STATE after each batch

6. **Table ID Strategy**
   - Use stream name: `{table_id_prefix}{stream_name}`
   - Sanitize for URL safety

### Option 2: Separate Connector

**Pros:**
- Cleaner separation of concerns
- Simpler codebase per connector
- Can optimize each for its use case

**Cons:**
- Code duplication (auth, retry logic, etc.)
- Two connectors to maintain
- Users need to choose connector upfront

**If choosing this option:**
- Create `airbyte-dust-tables-destination` repository
- Share common utilities via a shared package (if needed)
- Similar structure but focused on tables API

## Technical Considerations

### Schema Evolution
- **Problem**: Airbyte streams may add/remove columns over time
- **Solution**: 
  - On table upsert, merge new columns into existing schema
  - Handle missing columns in rows (null values)
  - Document limitations in README

### Primary Keys
- **Documents**: Used for document ID
- **Tables**: Should be used for row ID/upsert key
- Dust tables API likely supports primary key specification

### Incremental Syncs
- **Documents**: Each document upserted independently
- **Tables**: Need to track which rows were updated
- May need to use primary key for upsert semantics

### Error Handling
- **Documents**: Fail one record = fail sync (current behavior)
- **Tables**: Batch failures - partial success possible?
- Need to decide: fail-fast vs. continue-on-error

### Performance
- **Documents**: One API call per record
- **Tables**: Batch multiple rows per API call (more efficient)
- Consider rate limits: tables API may have different limits

## Testing Strategy

1. **Unit Tests**
   - Test schema inference from records
   - Test table creation/upsert logic
   - Test row batching logic
   - Test backward compatibility (documents mode)

2. **Integration Tests**
   - Test full sync with tables format
   - Test schema evolution scenarios
   - Test error handling

3. **Backward Compatibility**
   - Ensure existing configs (without `data_format`) default to documents
   - Test that document mode still works

## Migration Path

1. **Phase 1**: Implement tables support alongside documents (same connector)
2. **Phase 2**: Add configuration UI in Airbyte Cloud/OSS
3. **Phase 3**: Document use cases and when to use each format
4. **Phase 4**: Consider deprecating document mode? (Unlikely - both have value)

## Open Questions

1. **API Details Needed:**
   - What's the exact request body for `POST /tables`?
   - What's the exact request body for `POST /tables/{tId}/rows`?
   - How are primary keys specified in table schema?
   - What are the column type mappings?
   - What's the batch size limit for rows?

2. **Design Decisions:**
   - How to handle nested objects in records? (Flatten? JSON column?)
   - How to handle arrays? (JSON column? Separate table?)
   - Should we support schema evolution automatically?
   - What's the batching strategy? (Fixed size? Time-based?)

3. **User Experience:**
   - Should `data_format` be per-stream or global?
   - How to handle mixed formats in one sync? (Probably not supported initially)

## Recommendation

**Implement as same connector with `data_format` configuration option** because:
1. Both formats serve different use cases and users may want both
2. Shared infrastructure (auth, retry, connection checking) reduces maintenance
3. Easier to add features that work for both formats
4. Backward compatible (default to documents)

**Next Steps:**
1. Get API documentation/details for tables endpoints
2. Prototype table creation and row upsert
3. Implement schema inference
4. Add configuration option
5. Test with real Airbyte sources
