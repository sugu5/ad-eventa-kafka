"""
Avro deserialization helpers for the Spark consumer.
"""
import json
import traceback
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


def make_deserialize_udf(schema_broadcast):
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

            schema = schema_broadcast.value
            record = fastavro.schemaless_reader(BytesIO(data[5:]), schema)
            return _json.dumps(record, default=str)
        except Exception as e:
            logger.error(f"Deserialization error: {e}", exc_info=True)
            return None

    return udf(_deserialize, StringType())
