"""
Avro deserialization helpers for the Spark consumer.
"""
import json
import threading
from typing import Optional
from ad_stream_producer.logger import get_logger


def get_avro_schema_from_registry(schema_registry_url: str, subject: str) -> Optional[str]:
    """
    Fetch the latest schema for *subject* from Schema Registry.
    Returns the raw schema JSON string, or None on failure.
    """
    import requests

    url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get("schema")
        return None
    except requests.RequestException:
        return None

logger = get_logger("avro_deserializer")

_schema_cache = {}
_schema_cache_lock = threading.Lock()
_error_counts = {}
_MAX_LOGS_PER_ERROR_KEY = 5


def load_avro_schema(schema_registry_url: str, topic: str, local_schema_path):
    """
    Try to fetch the Avro schema from Schema Registry.
    Falls back to a local .avsc file if the registry is unreachable.
    """
    avro_schema_str = get_avro_schema_from_registry(
        schema_registry_url, f"{topic}-value"
    )
    if avro_schema_str:
        return json.loads(avro_schema_str), "registry"

    with open(local_schema_path) as f:
        return json.load(f), "local"


def get_avro_schema_by_id(schema_registry_url: str, schema_id: int):
    """
    Fetch a schema by numeric schema ID from Schema Registry and cache it.
    Returns parsed schema dict or None on failure.
    """
    import requests
    import fastavro

    with _schema_cache_lock:
        cached = _schema_cache.get(schema_id)
    if cached is not None:
        return cached

    url = f"{schema_registry_url}/schemas/ids/{schema_id}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None

        schema_str = response.json().get("schema")
        if not schema_str:
            return None

        parsed_schema = fastavro.parse_schema(json.loads(schema_str))
        with _schema_cache_lock:
            _schema_cache[schema_id] = parsed_schema
        return parsed_schema
    except requests.RequestException:
        return None


def _log_deserialization_error(error_key: str, message: str):
    """
    Avoid flooding stdout with identical tracebacks for malformed messages.
    """
    count = _error_counts.get(error_key, 0) + 1
    _error_counts[error_key] = count

    if count <= _MAX_LOGS_PER_ERROR_KEY:
        logger.warning(message)
    elif count == _MAX_LOGS_PER_ERROR_KEY + 1:
        logger.warning(f"Further deserialization errors for '{error_key}' will be suppressed")


def make_deserialize_udf(schema_registry_url: str, fallback_schema_broadcast):
    """
    Return a PySpark UDF that deserializes Confluent-wire-format Avro bytes
    into a JSON string.

    Confluent wire format:
      Byte  0     : magic byte (0x00)
      Bytes 1–4   : schema ID (big-endian int32)
      Bytes 5+    : Avro binary payload
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    def _deserialize(data):
        if data is None or len(data) < 5:
            return None
        try:
            import fastavro
            from io import BytesIO
            import json as _json

            if data[0] != 0:
                _log_deserialization_error(
                    "invalid_magic_byte",
                    f"Skipping message with invalid Confluent magic byte: {data[0]}",
                )
                return None

            schema_id = int.from_bytes(data[1:5], byteorder="big", signed=False)
            schema = get_avro_schema_by_id(schema_registry_url, schema_id)
            if schema is None:
                schema = fallback_schema_broadcast.value

            record = fastavro.schemaless_reader(BytesIO(data[5:]), schema)
            return _json.dumps(record, default=str)
        except Exception as e:
            error_key = type(e).__name__
            payload_length = len(data) if data is not None else 0
            _log_deserialization_error(
                error_key,
                f"Deserialization error ({error_key}) for payload length={payload_length}",
            )
            return None

    return udf(_deserialize, StringType())
