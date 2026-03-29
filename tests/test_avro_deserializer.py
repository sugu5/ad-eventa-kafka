import json
from pathlib import Path

from consumer import avro_deserializer as deser


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "ad_event_update.avsc"


def test_load_avro_schema_local_returns_string(monkeypatch):
    # Force registry lookup to fail to ensure local fallback is used
    monkeypatch.setattr(deser, "get_avro_schema_from_registry", lambda u, s: None)
    schema_str, source = deser.load_avro_schema("http://no-registry", "topic", SCHEMA_PATH)
    assert source == "local"
    assert isinstance(schema_str, str)
    assert "fields" in deser.json.loads(schema_str)


def test_make_from_avro_column_returns_callable_or_none():
    with open(SCHEMA_PATH) as f:
        schema_str = f.read()

    result = deser.make_from_avro_column(schema_str)
    # In environments without pyspark.avro support this will be None.
    assert (result is None) or callable(result)
