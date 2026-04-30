import os
from typing import Dict, List
from dotenv import load_dotenv

# Load .env file if present (no error if missing)
load_dotenv()


class Config:
    """
    Centralized configuration loaded from environment variables.
    Defaults match docker-compose.yaml for local development.

    Schema Evolution:
    - SCHEMA_VERSION: Controls which schema version to use
      * 1 = Original schema (ad_event_v1.avsc) - DEPRECATED
      * 2 = Schema with engagement_score (ad_event_v2.avsc) - SUPPORTED
      * 3 = Current schema with geo/segment data (ad_event_v3.avsc) - RECOMMENDED
    - SCHEMA_PATH: Explicit schema file path (overrides SCHEMA_VERSION)
    """

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: List[str] = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9095,localhost:9093,localhost:9094"
    ).split(",")
    TOPIC: str = os.getenv("KAFKA_TOPIC", "ads_events")

    # Schema Registry
    SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    
    # Schema version (1, 2, or 3)
    SCHEMA_VERSION: int = int(os.getenv("SCHEMA_VERSION", "3"))
    
    # Explicit schema path (if set, overrides SCHEMA_VERSION)
    # v1: ad_event_v1.avsc (original, deprecated)
    # v2: ad_event_v2.avsc (with engagement_score)
    # v3: ad_event_v3.avsc (with user_segment, conversion_value, geo_latitude)
    SCHEMA_PATH: str = os.getenv(
        "SCHEMA_PATH",
        {
            1: "ad_event_v1.avsc",
            2: "ad_event_v2.avsc",
            3: "ad_event_v3.avsc"
        }.get(SCHEMA_VERSION, "ad_event_v3.avsc")
    )

    # Iceberg warehouse path
    ICEBERG_WAREHOUSE: str = os.getenv("ICEBERG_WAREHOUSE", "output/iceberg_warehouse")
    ICEBERG_CATALOG: str = os.getenv("ICEBERG_CATALOG", "local")

    # Producer
    PRODUCER_RATE_PER_SEC: int = int(os.getenv("PRODUCER_RATE_PER_SEC", "10"))
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8001"))
