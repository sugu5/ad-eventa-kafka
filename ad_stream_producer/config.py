import os
from typing import Dict, List
from dotenv import load_dotenv

# Load .env file if present (no error if missing)
load_dotenv()


class Config:
    """
    Centralized configuration loaded from environment variables.
    Defaults match docker-compose.yaml for local development.
    """

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: List[str] = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9095,localhost:9093,localhost:9094"
    ).split(",")
    TOPIC: str = os.getenv("KAFKA_TOPIC", "ads_events")

    # Schema Registry
    SCHEMA_REGISTRY_URL: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    SCHEMA_PATH: str = os.getenv("SCHEMA_PATH", "ad_event_update.avsc")

    # PostgreSQL
    POSTGRES_URL: str = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/postgres")
    POSTGRES_PROPERTIES: Dict[str, str] = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "driver": os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver"),
    }
    POSTGRES_TABLE: str = os.getenv("POSTGRES_TABLE", "event_schema.ad_events")

    # Producer
    PRODUCER_RATE_PER_SEC: int = int(os.getenv("PRODUCER_RATE_PER_SEC", "10"))
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8001"))
