"""Tests for Avro round-trip serialization / deserialization."""
import json
import pytest
import fastavro
from io import BytesIO
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "ad_event_update.avsc"


@pytest.fixture
def avro_schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)


@pytest.fixture
def sample_event():
    return {
        "event_id": "abc-123",
        "event_time": "2026-03-28T12:00:00+00:00",
        "user_id": "user-001",
        "campaign_id": 42,
        "ad_id": 99,
        "device": "mobile",
        "country": "US",
        "event_type": "click",
        "ad_creative_id": "creative-001",
    }


@pytest.fixture
def sample_event_null_creative():
    return {
        "event_id": "abc-456",
        "event_time": "2026-03-28T12:00:00+00:00",
        "user_id": "user-002",
        "campaign_id": 10,
        "ad_id": 200,
        "device": "desktop",
        "country": "IN",
        "event_type": "view",
        "ad_creative_id": None,
    }


class TestAvroRoundTrip:
    """Serialize with fastavro, then deserialize, and verify equality."""

    def _serialize(self, schema, event):
        buf = BytesIO()
        fastavro.schemaless_writer(buf, schema, event)
        return buf.getvalue()

    def _deserialize(self, schema, data):
        return fastavro.schemaless_reader(BytesIO(data), schema)

    def test_round_trip(self, avro_schema, sample_event):
        raw = self._serialize(avro_schema, sample_event)
        result = self._deserialize(avro_schema, raw)
        assert result["event_id"] == "abc-123"
        assert result["campaign_id"] == 42
        assert result["ad_creative_id"] == "creative-001"

    def test_round_trip_null_creative(self, avro_schema, sample_event_null_creative):
        raw = self._serialize(avro_schema, sample_event_null_creative)
        result = self._deserialize(avro_schema, raw)
        assert result["ad_creative_id"] is None

    def test_schema_file_exists(self):
        assert SCHEMA_PATH.exists(), f"Schema file not found: {SCHEMA_PATH}"

    def test_schema_has_required_fields(self, avro_schema):
        field_names = {f["name"] for f in avro_schema["fields"]}
        expected = {
            "event_id", "event_time", "user_id", "campaign_id",
            "ad_id", "device", "country", "event_type", "ad_creative_id",
        }
        assert expected == field_names


