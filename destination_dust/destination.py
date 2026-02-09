import hashlib
import json
import logging
import re
from collections import defaultdict
from typing import Any, Iterable, List, Mapping

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    Type,
)

from destination_dust.client import DustClient

logger = logging.getLogger("airbyte")

# Batch size for table row upserts
TABLE_BATCH_SIZE = 500


class DestinationDust(Destination):
    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        return super().spec(*args, **kwargs)

    def check(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> AirbyteConnectionStatus:
        try:
            client = DustClient(config)
            data_format = config.get("data_format", "documents")
            client.check_connection(data_format=data_format)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Connection check failed: {str(e)}",
            )

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        client = DustClient(config)
        data_format = config.get("data_format", "documents")

        if data_format == "tables":
            yield from self._write_tables(client, config, configured_catalog, input_messages)
        else:
            yield from self._write_documents(client, configured_catalog, input_messages)

    def _write_documents(
        self,
        client: DustClient,
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """Write records as documents (original behavior)."""
        streams = {
            stream.stream.name: stream for stream in configured_catalog.streams
        }

        for message in input_messages:
            if message.type == Type.STATE:
                yield message

            elif message.type == Type.RECORD:
                record = message.record
                stream_name = record.stream
                data = record.data

                configured_stream = streams.get(stream_name)
                document_id = self._build_document_id(
                    stream_name, data, configured_stream
                )
                title = self._build_title(stream_name, data)
                text = json.dumps(data, indent=2, default=str)
                tags = [f"airbyte:stream:{stream_name}"]
                timestamp = record.emitted_at

                client.upsert_document(
                    document_id=document_id,
                    title=title,
                    text=text,
                    tags=tags,
                    timestamp=timestamp,
                )

    def _write_tables(
        self,
        client: DustClient,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """Write records as table rows with batching."""
        streams = {
            stream.stream.name: stream for stream in configured_catalog.streams
        }
        table_id_prefix = config.get("table_id_prefix", "airbyte_")

        # Collect records by stream
        stream_rows: dict[str, List[dict[str, Any]]] = defaultdict(list)
        stream_schemas: dict[str, dict[str, str]] = {}  # stream_name -> {column: type}
        table_ids: dict[str, str] = {}  # stream_name -> table_id

        for message in input_messages:
            if message.type == Type.STATE:
                # Flush any pending rows before yielding state
                yield from self._flush_table_batches(
                    client, stream_rows, stream_schemas, table_ids, streams, table_id_prefix
                )
                stream_rows.clear()
                yield message

            elif message.type == Type.RECORD:
                record = message.record
                stream_name = record.stream
                data = record.data

                # Update schema with this record's fields
                if stream_name not in stream_schemas:
                    stream_schemas[stream_name] = {}
                self._update_schema(stream_schemas[stream_name], data)

                # Flatten nested objects to JSON strings for now
                flattened_data = self._flatten_record(data)
                stream_rows[stream_name].append(flattened_data)

                # Batch and flush when batch size reached
                if len(stream_rows[stream_name]) >= TABLE_BATCH_SIZE:
                    if stream_name not in table_ids:
                        table_ids[stream_name] = self._build_table_id(
                            stream_name, table_id_prefix
                        )
                        self._ensure_table_exists(
                            client, stream_name, table_ids[stream_name],
                            stream_schemas[stream_name], streams.get(stream_name)
                        )

                    rows_batch = stream_rows[stream_name][:TABLE_BATCH_SIZE]
                    client.upsert_rows(table_ids[stream_name], rows_batch)
                    stream_rows[stream_name] = stream_rows[stream_name][TABLE_BATCH_SIZE:]

        # Flush remaining rows
        yield from self._flush_table_batches(
            client, stream_rows, stream_schemas, table_ids, streams, table_id_prefix
        )

    def _flush_table_batches(
        self,
        client: DustClient,
        stream_rows: dict[str, List[dict[str, Any]]],
        stream_schemas: dict[str, dict[str, str]],
        table_ids: dict[str, str],
        streams: dict[str, Any],
        table_id_prefix: str,
    ) -> Iterable[AirbyteMessage]:
        """Flush all pending rows for all streams."""
        for stream_name, rows in stream_rows.items():
            if not rows:
                continue

            if stream_name not in table_ids:
                table_ids[stream_name] = self._build_table_id(
                    stream_name, table_id_prefix
                )
                self._ensure_table_exists(
                    client, stream_name, table_ids[stream_name],
                    stream_schemas[stream_name], streams.get(stream_name)
                )

            # Flush in batches
            for i in range(0, len(rows), TABLE_BATCH_SIZE):
                batch = rows[i:i + TABLE_BATCH_SIZE]
                client.upsert_rows(table_ids[stream_name], batch)

    def _ensure_table_exists(
        self,
        client: DustClient,
        stream_name: str,
        table_id: str,
        schema: dict[str, str],
        configured_stream: Any,
    ) -> None:
        """Ensure table exists with correct schema."""
        columns = []
        for col_name, col_type in schema.items():
            columns.append({
                "name": col_name,
                "type": col_type,
            })

        # Note: Primary keys are handled by Dust API based on row data
        # The API may infer primary keys from the table schema or row structure
        client.upsert_table(
            table_id=table_id,
            name=stream_name,
            description=f"Airbyte stream: {stream_name}",
            columns=columns,
        )

    @staticmethod
    def _build_document_id(
        stream_name: str,
        data: Mapping[str, Any],
        configured_stream: Any,
    ) -> str:
        """
        Build a deterministic document ID from stream name and primary key.

        Falls back to a SHA-256 hash prefix of the record data when no
        primary key is defined.
        """
        pk_parts: list[str] = []
        if (
            configured_stream
            and configured_stream.primary_key
            and len(configured_stream.primary_key) > 0
        ):
            for key_path in configured_stream.primary_key:
                value: Any = data
                for key in key_path:
                    if isinstance(value, dict):
                        value = value.get(key, "")
                    else:
                        value = ""
                        break
                pk_parts.append(str(value))

        if pk_parts:
            raw_id = f"{stream_name}-{'-'.join(pk_parts)}"
        else:
            data_str = json.dumps(data, sort_keys=True, default=str)
            data_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
            raw_id = f"{stream_name}-{data_hash}"

        return re.sub(r"[^a-zA-Z0-9_\-]", "_", raw_id)

    @staticmethod
    def _build_title(stream_name: str, data: Mapping[str, Any]) -> str:
        """
        Extract a human-readable title from the record data.

        Checks common title-like field names; falls back to stream name.
        """
        for candidate in ("title", "name", "subject", "headline", "label"):
            if candidate in data and data[candidate]:
                return str(data[candidate])
        return f"{stream_name} record"

    @staticmethod
    def _build_table_id(stream_name: str, prefix: str) -> str:
        """Build a table ID from stream name and prefix."""
        raw_id = f"{prefix}{stream_name}"
        return re.sub(r"[^a-zA-Z0-9_\-]", "_", raw_id)

    @staticmethod
    def _update_schema(schema: dict[str, str], data: Mapping[str, Any]) -> None:
        """
        Update schema dictionary with types inferred from data.

        Maps Python/JSON types to Dust column types.
        """
        for key, value in data.items():
            if key not in schema:
                schema[key] = DestinationDust._infer_column_type(value)

    @staticmethod
    def _infer_column_type(value: Any) -> str:
        """
        Infer Dust column type from a Python value.

        Returns a Dust column type string. Common types:
        - "string" for text
        - "number" for integers/floats
        - "boolean" for booleans
        - "json" for complex objects/arrays
        """
        if value is None:
            return "string"  # Default for nullable columns
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "number"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, (dict, list)):
            return "json"  # Store complex types as JSON
        else:
            return "string"  # Fallback

    @staticmethod
    def _flatten_record(data: Mapping[str, Any]) -> dict[str, Any]:
        """
        Flatten a record for table storage.

        Nested objects and arrays are serialized to JSON strings.
        """
        flattened = {}
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                flattened[key] = json.dumps(value, default=str)
            else:
                flattened[key] = value
        return flattened
