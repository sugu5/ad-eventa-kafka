"""
Avro deserialization helpers for the Spark consumer.
"""
import json
from typing import Optional


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



def load_avro_schema(schema_registry_url: str, topic: str, local_schema_path):
    """
    Try to fetch the Avro schema from Schema Registry.
    Falls back to a local .avsc file if the registry is unreachable.

    Returns the schema as a JSON string and a source tag: (schema_str, "registry"|"local").
    """
    avro_schema_str = get_avro_schema_from_registry(
        schema_registry_url, f"{topic}-value"
    )
    if avro_schema_str:
        return avro_schema_str, "registry"

    with open(local_schema_path) as f:
        return f.read(), "local"


def make_from_avro_column(avro_schema_str: str):
    """
    Return a function that converts a binary column containing Confluent
    wire-format Avro messages into a JSON string column using Spark
    `from_avro` + `to_json`.

    The Confluent wire format has a 5-byte header (magic + schema id). We
    remove the first 5 bytes using `substring` before calling `from_avro`.

    Example usage in `run_consumer.py`:
      deser = make_from_avro_column(avro_schema_str)
      df2 = df.select(deser(col("value")).alias("json_str"))
    """
    from pyspark.sql.functions import expr, to_json
    try:
        from pyspark.sql.avro.functions import from_avro
    except Exception:
        return None

    def _col_deser(col_expr):
        # substring in Spark is 1-based: start at 6 to skip first 5 bytes
        payload = expr("substring({}, 6, length({})-5)".format(col_expr._jc.toString(), col_expr._jc.toString()))
        return to_json(from_avro(payload, avro_schema_str))

    return _col_deser


