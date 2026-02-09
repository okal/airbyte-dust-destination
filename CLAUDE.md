# Airbyte Destination Connector for Dust

## Project Overview

Python Airbyte destination connector that pushes data into Dust data sources
as documents. Each Airbyte record becomes a Dust document, upserted via the
Dust REST API.

## Architecture

### File Structure

- `main.py` - Entry point. Instantiates DestinationDust and runs it.
- `destination_dust/destination.py` - Core connector class implementing
  spec(), check(), write().
- `destination_dust/client.py` - HTTP client for the Dust API with retry logic.
- `destination_dust/spec.json` - Connector configuration schema.

### Key Patterns

- The Destination base class comes from `airbyte_cdk.destinations`.
- `spec()` loads from spec.json automatically (base class behavior).
- `check()` returns AirbyteConnectionStatus(SUCCEEDED or FAILED).
- `write()` is a generator that yields AirbyteMessage(type=STATE).
- Records are upserted one at a time (no batching).
- STATE messages are yielded back immediately since upserts are synchronous.

### Dust API

- Base URL: `https://dust.tt` (US) or `https://eu.dust.tt` (EU)
- Auth: Bearer token in Authorization header
- Upsert: `POST /api/v1/w/{wId}/spaces/{spaceId}/data-sources/{dsId}/documents/{documentId}`
- List docs: `GET /api/v1/w/{wId}/spaces/{spaceId}/data-sources/{dsId}/documents`
- URL paths use **hyphens** (`data-sources`), not underscores

#### Upsert Request Body

```json
{
  "title": "string",
  "mime_type": "application/json",
  "text": "string (JSON-serialized record data)",
  "source_url": "string",
  "tags": ["string"],
  "timestamp": 1736365559000,
  "light_document_output": true,
  "async": false
}
```

### Document ID Strategy

1. If the stream has a primary_key: `{stream_name}-{pk_value1}-{pk_value2}`
2. No primary key: `{stream_name}-{sha256_hash_first_16_chars}`
3. Sanitized to `[a-zA-Z0-9_-]` for URL safety

## Development

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest pytest-mock requests-mock  # dev deps
```

### Run Locally

```bash
# Print connector spec
python main.py spec

# Check connection
python main.py check --config secrets/config.json

# Run a sync (pipe messages from stdin)
cat messages.jsonl | python main.py write --config secrets/config.json --catalog catalog.json
```

### secrets/config.json Format

```json
{
  "api_key": "sk-...",
  "workspace_id": "your-workspace-id",
  "space_id": "your-space-id",
  "data_source_id": "your-datasource-id",
  "base_url": "https://dust.tt"
}
```

### Test

```bash
.venv/bin/pytest unit_tests/ -v
```

### Build Docker Image

```bash
docker build -t airbyte/destination-dust:dev .
```

## Configuration

Defined in `destination_dust/spec.json`:

| Field | Required | Description |
|-------|----------|-------------|
| `api_key` | Yes | Dust API Bearer token (secret) |
| `workspace_id` | Yes | Dust workspace ID |
| `space_id` | Yes | Dust space ID |
| `data_source_id` | Yes | Dust data source ID |
| `base_url` | No | API base URL (default: `https://dust.tt`) |

## Error Handling

- HTTP retries: 3 attempts with exponential backoff for 429/5xx
- Failed upserts raise RuntimeError, failing the sync
- Airbyte resumes from last checkpointed STATE on retry
- No silent record dropping — fail loud, retry the sync

## Dependencies

- `airbyte-cdk >=6.0`
- `requests >=2.31,<3.0`
