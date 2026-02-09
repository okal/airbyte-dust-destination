import json
from unittest.mock import MagicMock, patch

import pytest

from destination_dust.destination import DestinationDust


class TestBuildDocumentId:
    def test_with_single_primary_key(self):
        stream = MagicMock()
        stream.primary_key = [["id"]]
        data = {"id": 42, "name": "Alice"}

        result = DestinationDust._build_document_id("users", data, stream)
        assert result == "users-42"

    def test_with_composite_primary_key(self):
        stream = MagicMock()
        stream.primary_key = [["org_id"], ["user_id"]]
        data = {"org_id": "acme", "user_id": 7}

        result = DestinationDust._build_document_id("members", data, stream)
        assert result == "members-acme-7"

    def test_with_nested_primary_key(self):
        stream = MagicMock()
        stream.primary_key = [["meta", "id"]]
        data = {"meta": {"id": "abc-123"}, "value": "x"}

        result = DestinationDust._build_document_id("items", data, stream)
        # The hyphen in "abc-123" is preserved (it's a safe char)
        assert result == "items-abc-123"

    def test_without_primary_key(self):
        stream = MagicMock()
        stream.primary_key = []
        data = {"foo": "bar", "num": 1}

        result = DestinationDust._build_document_id("events", data, stream)
        assert result.startswith("events-")
        assert len(result) == len("events-") + 16  # 16-char hash

    def test_without_primary_key_is_deterministic(self):
        stream = MagicMock()
        stream.primary_key = []
        data = {"a": 1, "b": 2}

        id1 = DestinationDust._build_document_id("s", data, stream)
        id2 = DestinationDust._build_document_id("s", data, stream)
        assert id1 == id2

    def test_without_configured_stream(self):
        data = {"x": "y"}
        result = DestinationDust._build_document_id("stream", data, None)
        assert result.startswith("stream-")

    def test_sanitizes_special_characters(self):
        stream = MagicMock()
        stream.primary_key = [["id"]]
        data = {"id": "hello world/foo@bar"}

        result = DestinationDust._build_document_id("s", data, stream)
        assert result == "s-hello_world_foo_bar"

    def test_missing_pk_field_in_data(self):
        stream = MagicMock()
        stream.primary_key = [["missing_field"]]
        data = {"other": "value"}

        result = DestinationDust._build_document_id("s", data, stream)
        assert result == "s-"


class TestBuildTitle:
    def test_uses_title_field(self):
        assert DestinationDust._build_title("s", {"title": "My Doc"}) == "My Doc"

    def test_uses_name_field(self):
        assert DestinationDust._build_title("s", {"name": "Alice"}) == "Alice"

    def test_uses_subject_field(self):
        assert DestinationDust._build_title("s", {"subject": "Re: Hello"}) == "Re: Hello"

    def test_prefers_title_over_name(self):
        data = {"title": "T", "name": "N"}
        assert DestinationDust._build_title("s", data) == "T"

    def test_fallback_to_stream_name(self):
        assert DestinationDust._build_title("users", {"id": 1}) == "users record"

    def test_skips_empty_title(self):
        assert DestinationDust._build_title("s", {"title": "", "name": "N"}) == "N"

    def test_skips_none_title(self):
        assert DestinationDust._build_title("s", {"title": None, "name": "N"}) == "N"


class TestCheck:
    @patch("destination_dust.destination.DustClient")
    def test_check_success(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        dest = DestinationDust()
        config = {
            "api_key": "sk-test",
            "workspace_id": "w1",
            "space_id": "s1",
            "data_source_id": "ds1",
        }
        result = dest.check(MagicMock(), config)

        assert result.status.value == "SUCCEEDED"
        mock_client.check_connection.assert_called_once_with(data_format="documents")

    @patch("destination_dust.destination.DustClient")
    def test_check_failure(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.check_connection.side_effect = ConnectionError("bad key")
        mock_client_cls.return_value = mock_client

        dest = DestinationDust()
        config = {
            "api_key": "bad",
            "workspace_id": "w1",
            "space_id": "s1",
            "data_source_id": "ds1",
        }
        result = dest.check(MagicMock(), config)

        assert result.status.value == "FAILED"
        assert "bad key" in result.message


class TestWrite:
    @patch("destination_dust.destination.DustClient")
    def test_yields_state_messages(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        dest = DestinationDust()
        config = {
            "api_key": "sk-test",
            "workspace_id": "w1",
            "space_id": "s1",
            "data_source_id": "ds1",
        }

        state_msg = MagicMock()
        state_msg.type = MagicMock()
        state_msg.type.__eq__ = lambda self, other: other.name == "STATE"
        # Use the actual Type enum
        from airbyte_cdk.models import Type
        state_msg.type = Type.STATE

        catalog = MagicMock()
        catalog.streams = []

        results = list(dest.write(config, catalog, [state_msg]))
        assert len(results) == 1
        assert results[0] is state_msg

    @patch("destination_dust.destination.DustClient")
    def test_upserts_record_messages(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        from airbyte_cdk.models import Type

        record_msg = MagicMock()
        record_msg.type = Type.RECORD
        record_msg.record.stream = "users"
        record_msg.record.data = {"id": 1, "name": "Alice"}
        record_msg.record.emitted_at = 1700000000000

        stream = MagicMock()
        stream.stream.name = "users"
        stream.primary_key = [["id"]]

        catalog = MagicMock()
        catalog.streams = [stream]

        dest = DestinationDust()
        config = {
            "api_key": "sk-test",
            "workspace_id": "w1",
            "space_id": "s1",
            "data_source_id": "ds1",
        }

        results = list(dest.write(config, catalog, [record_msg]))

        assert len(results) == 0  # No state messages to yield
        mock_client.upsert_document.assert_called_once()

        call_kwargs = mock_client.upsert_document.call_args
        assert call_kwargs.kwargs["document_id"] == "users-1"
        assert call_kwargs.kwargs["title"] == "Alice"
        assert call_kwargs.kwargs["tags"] == ["airbyte:stream:users"]
        assert json.loads(call_kwargs.kwargs["text"]) == {"id": 1, "name": "Alice"}

    @patch("destination_dust.destination.DustClient")
    def test_writes_tables_mode(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        from airbyte_cdk.models import Type

        record_msg = MagicMock()
        record_msg.type = Type.RECORD
        record_msg.record.stream = "users"
        record_msg.record.data = {"id": 1, "name": "Alice", "age": 30}
        record_msg.record.emitted_at = 1700000000000

        stream = MagicMock()
        stream.stream.name = "users"
        stream.primary_key = [["id"]]

        catalog = MagicMock()
        catalog.streams = [stream]

        dest = DestinationDust()
        config = {
            "api_key": "sk-test",
            "workspace_id": "w1",
            "space_id": "s1",
            "data_source_id": "ds1",
            "data_format": "tables",
            "table_id_prefix": "airbyte_",
        }

        results = list(dest.write(config, catalog, [record_msg]))

        assert len(results) == 0  # No state messages to yield
        mock_client.upsert_table.assert_called_once()
        mock_client.upsert_rows.assert_called_once()

        # Check table creation
        table_call = mock_client.upsert_table.call_args
        assert table_call.kwargs["table_id"] == "airbyte_users"
        assert table_call.kwargs["name"] == "users"
        assert len(table_call.kwargs["columns"]) == 3  # id, name, age

        # Check row upsert
        rows_call = mock_client.upsert_rows.call_args
        assert rows_call.kwargs["table_id"] == "airbyte_users"
        assert len(rows_call.kwargs["rows"]) == 1
        assert rows_call.kwargs["rows"][0] == {"id": 1, "name": "Alice", "age": 30}


class TestBuildTableId:
    def test_basic_table_id(self):
        result = DestinationDust._build_table_id("users", "airbyte_")
        assert result == "airbyte_users"

    def test_sanitizes_special_characters(self):
        result = DestinationDust._build_table_id("my-stream", "prefix_")
        assert result == "prefix_my-stream"

    def test_sanitizes_unsafe_characters(self):
        result = DestinationDust._build_table_id("stream@name", "p_")
        assert result == "p_stream_name"


class TestInferColumnType:
    def test_string_type(self):
        assert DestinationDust._infer_column_type("hello") == "string"
        assert DestinationDust._infer_column_type(None) == "string"

    def test_number_type(self):
        assert DestinationDust._infer_column_type(42) == "number"
        assert DestinationDust._infer_column_type(3.14) == "number"

    def test_boolean_type(self):
        assert DestinationDust._infer_column_type(True) == "boolean"
        assert DestinationDust._infer_column_type(False) == "boolean"

    def test_json_type(self):
        assert DestinationDust._infer_column_type({"key": "value"}) == "json"
        assert DestinationDust._infer_column_type([1, 2, 3]) == "json"


class TestFlattenRecord:
    def test_flattens_nested_objects(self):
        data = {"id": 1, "meta": {"key": "value"}}
        result = DestinationDust._flatten_record(data)
        assert result["id"] == 1
        assert result["meta"] == '{"key": "value"}'

    def test_flattens_arrays(self):
        data = {"id": 1, "tags": ["a", "b"]}
        result = DestinationDust._flatten_record(data)
        assert result["id"] == 1
        assert isinstance(result["tags"], str)
        assert json.loads(result["tags"]) == ["a", "b"]

    def test_preserves_primitive_types(self):
        data = {"id": 1, "name": "Alice", "active": True, "score": 95.5}
        result = DestinationDust._flatten_record(data)
        assert result == data


class TestUpdateSchema:
    def test_updates_schema_from_record(self):
        schema = {}
        data = {"id": 1, "name": "Alice", "active": True}
        DestinationDust._update_schema(schema, data)
        assert schema["id"] == "number"
        assert schema["name"] == "string"
        assert schema["active"] == "boolean"

    def test_handles_nested_objects(self):
        schema = {}
        data = {"id": 1, "meta": {"key": "value"}}
        DestinationDust._update_schema(schema, data)
        assert schema["id"] == "number"
        assert schema["meta"] == "json"


class TestCheckTables:
    @patch("destination_dust.destination.DustClient")
    def test_check_with_tables_format(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        dest = DestinationDust()
        config = {
            "api_key": "sk-test",
            "workspace_id": "w1",
            "space_id": "s1",
            "data_source_id": "ds1",
            "data_format": "tables",
        }
        result = dest.check(MagicMock(), config)

        assert result.status.value == "SUCCEEDED"
        mock_client.check_connection.assert_called_once_with(data_format="tables")
