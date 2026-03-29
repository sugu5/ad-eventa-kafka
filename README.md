# 🚀 Kafka Ad-Stream Pipeline

A **real-time ad-event streaming pipeline** built with Apache Kafka, Confluent Schema Registry, PySpark Structured Streaming, and PostgreSQL.

Simulates an ad-tech data platform where millions of user interactions (views, clicks, purchases) are produced, validated, serialized with Avro, streamed through a 3-broker Kafka cluster, and consumed into a PostgreSQL data warehouse — all with end-to-end observability via Prometheus metrics.

---

## 📐 Architecture

```
┌──────────────┐     Avro / Schema      ┌──────────────────┐
│  Event       │     Registry            │  Schema Registry │
│  Generator   │◄───────────────────────►│  (Confluent)     │
│  (Faker)     │                         └──────────────────┘
└──────┬───────┘                                  ▲
       │ generate                                 │ register / fetch
       ▼                                          │
┌──────────────┐  produce   ┌─────────────────────┴───┐
│  Producer    │───────────►│  Kafka Cluster (3 brokers)│
│  Service     │  (Avro)    │  Topic: ads_events       │
│  + Pydantic  │            │  Partitions: 3           │
│    validation│            └─────────────┬────────────┘
└──────────────┘                          │ consume
       │                                  ▼
  Prometheus              ┌───────────────────────────┐
  metrics (:8001)         │  PySpark Structured        │
                          │  Streaming Consumer        │
                          │                            │
                          │  ┌─────────┐ ┌───────────┐│
                          │  │Good     │ │Corrupted  ││
                          │  │Records  │ │Records    ││
                          │  └────┬────┘ └─────┬─────┘│
                          └───────┼────────────┼──────┘
                                  │            │
                                  ▼            ▼
                          ┌────────────┐  ┌──────────┐
                          │ PostgreSQL │  │  Local    │
                          │ (JDBC)     │  │  JSON     │
                          └────────────┘  └──────────┘
```

---

## 🛠 Tech Stack

| Component | Technology |
|-----------|-----------|
| Message Broker | Apache Kafka 7.6 (3-broker cluster) |
| Schema Management | Confluent Schema Registry + Avro |
| Producer | confluent-kafka Python client |
| Data Validation | Pydantic v2 |
| Consumer | PySpark Structured Streaming |
| Sink | PostgreSQL 16 |
| Observability | Prometheus (producer metrics) |
| Data Generation | Faker |
| Monitoring UI | Kafka UI (Provectus) |
| Containerization | Docker Compose |

---

## 📂 Project Structure

```
kafka-develop/
├── ad_stream_producer/          # Kafka producer package
│   ├── config.py                # Env-var-driven configuration
│   ├── kafka_producer.py        # Async confluent-kafka Producer wrapper
│   ├── logger.py                # Structured logging
│   ├── producer_service.py      # Orchestrates generate → validate → serialize → produce
│   ├── run_producer.py          # CLI entry point
│   └── schema.py                # Pydantic AdEvent model
├── consumer/                    # Spark consumer package
│   ├── avro_deserializer.py     # Avro deser UDF + Schema Registry client
│   ├── sink.py                  # foreachBatch processor (Postgres + Parquet fallback)
│   └── run_consumer.py          # CLI entry point
├── data_generator/
│   └── event_generator.py       # Realistic synthetic event generator
├── metrics/
│   └── metrics.py               # Prometheus counters, gauges, histograms
├── schema/
│   └── ad_event_update.avsc     # Avro schema definition
├── tests/                       # pytest test suite
│   ├── test_event_generator.py
│   ├── test_avro_serde.py
│   └── test_producer_service.py
├── docker-compose.yaml          # Full infra: Kafka × 3, ZK, SR, Kafka UI, Postgres
├── init_db.sql                  # PostgreSQL table + indexes
├── .env.example                 # Environment variable template
├── pyproject.toml               # Python project config + dependencies
└── README.md
```

---

## ⚡ Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This brings up:
- **Zookeeper** — cluster coordination
- **Kafka × 3** — brokers on ports `9093`, `9094`, `9095`
- **Schema Registry** — port `8081`
- **Kafka UI** — port `8080` → [http://localhost:8080](http://localhost:8080)
- **PostgreSQL** — port `5432` (auto-creates `event_schema.ad_events` table)

### 2. Install Python Dependencies

```bash
pip install -e ".[dev]"
```

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env if your ports / credentials differ
```

### 4. Run the Producer

```bash
python -m ad_stream_producer.run_producer
```

The producer will:
- Generate synthetic ad events using Faker
- Validate each event with Pydantic
- Serialize with Avro (registered in Schema Registry)
- Produce to `ads_events` topic at configurable rate (default: 10/sec)
- Expose Prometheus metrics on `:8001`

### 5. Run the Consumer

```bash
python -m consumer.run_consumer
```

The consumer will:
- Read from `ads_events` topic using PySpark Structured Streaming
- Deserialize Avro messages (Confluent wire format)
- **Write valid records to PostgreSQL with UPSERT** (handles duplicates gracefully)
- Route corrupted records to local JSON files

### 6. Run Tests

```bash
pytest tests/ -v
```

---

## 📊 Event Schema

```json
{
  "type": "record",
  "name": "AdEvent",
  "namespace": "ads",
  "fields": [
    {"name": "event_id",       "type": "string"},
    {"name": "event_time",     "type": "string"},
    {"name": "user_id",        "type": "string"},
    {"name": "campaign_id",    "type": "int"},
    {"name": "ad_id",          "type": "int"},
    {"name": "device",         "type": "string"},
    {"name": "country",        "type": "string"},
    {"name": "event_type",     "type": "string"},
    {"name": "ad_creative_id", "type": ["null", "string"]}
  ]
}
```

Event types follow a realistic distribution: **views (55%) > clicks (33%) > purchases (11%)**

---

## 🔑 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **confluent-kafka** (not kafka-python) | Native C library — higher throughput, async produce with delivery callbacks |
| **Avro + Schema Registry** | Compact binary format, centralized schema evolution, backward compatibility |
| **Pydantic validation before serialization** | Catch malformed events at the producer, not the consumer |
| **3-broker cluster with replication factor 3** | Fault-tolerant — survives 1 broker failure |
| **PySpark Structured Streaming** | Exactly-once semantics with checkpointing, micro-batch processing |
| **PostgreSQL with Parquet fallback** | If DB is down, data is preserved locally and not lost |
| **Prometheus metrics** | Production-standard observability (events/sec, latency, failures) |
| **Fixed user pool (1000 users)** | Simulates realistic session continuity vs. random UUIDs |
| **Environment-variable config** | No secrets in source code, 12-factor app pattern |
| **PostgreSQL UPSERT** | Handles duplicate events gracefully with `ON CONFLICT DO UPDATE` |

---

## 📈 Monitoring

- **Kafka UI**: [http://localhost:8080](http://localhost:8080) — topics, partitions, consumer groups
- **Prometheus metrics**: [http://localhost:8001](http://localhost:8001) — producer throughput, latency, failures

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=ad_stream_producer --cov=data_generator -v
```

| Test File | What It Covers |
|-----------|---------------|
| `test_event_generator.py` | Event format, field ranges, distribution, user pool reuse, seed reproducibility |
| `test_avro_serde.py` | Avro round-trip serialization, nullable fields, schema validation |
| `test_producer_service.py` | Serialization calls, Kafka send, error handling, shutdown |

---

## 📝 What I Learned

- How Kafka **partitioning** and **replication** work in a multi-broker setup
- The **Confluent wire format** (magic byte + schema ID + Avro binary) and why Schema Registry exists
- Why **subject naming strategies** (`topic_subject_name_strategy`) require `SerializationContext`
- The difference between **sync vs. async producing** and the impact on throughput
- PySpark **Structured Streaming checkpoints** for exactly-once delivery
- Why **Pydantic validation at the producer** prevents downstream deserialization failures

---

## License

Personal project — feel free to reference for learning.
