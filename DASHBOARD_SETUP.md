# Beautiful Kafka Dashboard Setup Guide

## Overview
This setup provides a complete monitoring solution with:
- **Prometheus** - Metrics collection and storage
- **Grafana** - Beautiful dashboards
- **Kafka Exporter** - Exposes Kafka broker/topic/consumer metrics
- **Application Metrics** - Your producer/consumer metrics on port 8001

## Quick Start

### 1. Start All Services
From your project root, run:

```powershell
docker-compose up -d
```

This will start:
- Zookeeper
- 3x Kafka Brokers
- Schema Registry
- Kafka UI
- PostgreSQL
- **Kafka Exporter** (new)
- **Prometheus** (new)
- **Grafana** (new)

Wait ~30 seconds for all services to be healthy:

```powershell
docker-compose ps
```

All services should show `healthy` status.

### 2. Access Grafana Dashboard
Open in your browser:

**URL:** http://localhost:3000

**Credentials:**
- Username: `admin`
- Password: `admin`

The **Kafka Streaming Dashboard** will be automatically loaded in the "Kafka" folder.

### 3. Dashboard Panels

#### Overview (Top Row - Big Numbers)
- **Total Topics** - Count of all topics in your Kafka cluster
- **Total Partitions** - Sum of all partitions across topics
- **Producer Throughput** - Events per second (your app)
- **Failed Events** - Counter of failed deliveries

#### Producer Monitoring
- **Producer Offsets Table** - Latest offset per partition for `ads_events` topic
- **Producer Events Rate** - Sent vs Failed events per minute
- **Producer Latency Percentiles** - p50, p95, p99 latencies (1m avg)
- **Producer Queue Backlog** - Internal queue size over time

#### Consumer Monitoring
- **Consumer Lag Table** - Lag per partition (color coded: green=0, yellow>100, red>500)
- **Consumer Offsets Table** - Current offset per partition for each consumer group
- **Consumer Lag Trend** - Lag over time for all consumer groups

#### Cluster Overview
- **Topics Overview** - List all topics with partition count
- **Broker Metrics** - Topic/partition information

### 4. Verify Metrics Are Flowing

**Check Prometheus Targets:**
- URL: http://localhost:9090/targets
- You should see:
  - `kafka_exporter:9308` - UP
  - `app_metrics` (host.docker.internal:8001 or your app IP) - UP

**Check Available Metrics:**
- URL: http://localhost:9090
- Click Graph tab, type a metric name (e.g., `events_sent_total`, `kafka_partition_current_offset`, etc.)

### 5. Access Other Monitoring UIs

- **Kafka UI:** http://localhost:8080 (topics, brokers, consumer groups)
- **Schema Registry:** http://localhost:8081 (schemas)
- **Prometheus UI:** http://localhost:9090 (raw metrics)
- **Your App Metrics:** http://localhost:8001/metrics (raw Prometheus format)

## Dashboard Metric Queries

### Key Metric Names

**From Kafka Exporter:**
- `kafka_topic_partitions{topic="..."}` - Partition count per topic
- `kafka_partition_current_offset{topic="...", partition="..."}` - Latest offset
- `kafka_consumergroup_current_offset{group="...", topic="...", partition="..."}` - Consumer offset
- `kafka_consumergroup_lag{group="...", topic="...", partition="..."}` - Lag per partition

**From Your App:**
- `events_sent_total` - Counter of sent events
- `events_failed_total` - Counter of failed events
- `events_per_second` - Current throughput gauge
- `producer_latency_seconds` - Histogram of delivery latencies
- `producer_queue_size` - Current queue backlog

## Customizing the Dashboard

### Add New Panel
1. Click **+** button → **Add panel**
2. Select **Prometheus** datasource
3. Write your query (examples below)
4. Customize visualization type, colors, thresholds

### Example Queries

**Total messages in system:**
```
sum(rate(events_sent_total[1m]))
```

**Consumer lag by group:**
```
sum by (group) (kafka_consumergroup_lag)
```

**Producer latency 99th percentile:**
```
histogram_quantile(0.99, sum(rate(producer_latency_seconds_bucket[5m])) by (le))
```

**Topic throughput (messages/sec):**
```
sum by (topic) (rate(kafka_partition_current_offset[1m]))
```

**Failed event rate:**
```
rate(events_failed_total[1m])
```

## Troubleshooting

### Metrics Not Appearing in Prometheus

1. **Kafka Exporter Logs:**
   ```powershell
   docker logs kafka-exporter
   ```
   Check if it can connect to brokers.

2. **App Metrics Not Scraped:**
   - If your Python app runs on Windows host, ensure Prometheus can reach `host.docker.internal:8001`
   - If app runs in Docker container, verify container name/port in `prometheus.yml`

3. **Verify Metrics Endpoint:**
   ```powershell
   curl http://localhost:8001/metrics
   ```
   Should return Prometheus format metrics.

### Grafana Dashboard Not Loading

1. Check Prometheus datasource:
   - Grafana → Configuration → Data Sources
   - Verify Prometheus shows green checkmark (UP)

2. Manually import dashboard:
   - Go to Dashboards → New → Import
   - Upload `monitoring/grafana-dashboards/kafka-dashboard.json`

### Port Conflicts

If ports are already in use:
- Prometheus: 9090
- Grafana: 3000
- Kafka Exporter: 9308

Edit `docker-compose.yaml` to change port mappings.

## Configuration Files

- `monitoring/prometheus.yml` - Scrape config for Prometheus
- `monitoring/grafana-datasources.yml` - Prometheus datasource config
- `monitoring/grafana-dashboards-provision.yml` - Auto-load dashboards
- `monitoring/grafana-dashboards/kafka-dashboard.json` - The beautiful dashboard

## Metrics Flow Diagram

```
Your App (Producer/Consumer) 
    ↓ 
    Exposes metrics on port 8001
    ↓
Prometheus scrapes:
    - app_metrics (port 8001)
    - kafka_exporter (port 9308)
    ↓
Grafana queries Prometheus
    ↓
Beautiful Dashboard on port 3000
```

## Auto-Refresh and Time Range

The dashboard:
- **Refreshes every 10 seconds** by default
- **Shows last 1 hour** of data by default
- **Change time range:** Click time picker (top right)
- **Manual refresh:** Click refresh button

## Next Steps

1. **Start your producer:** `python -m ad_stream_producer.run_producer`
2. **Start your consumer:** `python -m consumer.run_consumer`
3. **Monitor on Grafana:** Watch metrics flow in real-time
4. **Adjust thresholds:** Customize alert thresholds for your SLOs

## Alert Setup (Optional)

To add alerts to dashboard panels:
1. Click panel → Edit
2. Scroll to "Alert" section
3. Define condition and notification channel
4. Test and save

Example: Alert if producer lag > 1000 messages or failed events rate > 0.

---

**Enjoy your beautiful Kafka monitoring dashboard! 🎉**

