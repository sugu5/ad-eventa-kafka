# 🎨 Beautiful Kafka Streaming Dashboard

Your Kafka cluster now has a **professional, beautiful monitoring dashboard** built with Prometheus and Grafana!

## 📊 What You Get

A complete monitoring solution with real-time visibility into:
- ✅ **Kafka Brokers** - Topics, partitions, broker health
- ✅ **Producer Metrics** - Events/sec, latency percentiles, queue backlog
- ✅ **Consumer Metrics** - Offsets, lag, consumption rate
- ✅ **System Health** - All metrics in beautiful charts and tables

---

## 🚀 Quick Start (3 Steps)

### Step 1: Start the Services
Run one of these commands from your project root:

**PowerShell:**
```powershell
.\start_dashboard.ps1
```

**Batch/CMD:**
```cmd
start_dashboard.bat
```

**Or manually:**
```bash
docker-compose up -d kafka-exporter prometheus grafana
```

Wait ~30 seconds for all services to become healthy.

### Step 2: Open Grafana Dashboard
Open your browser:
```
http://localhost:3000
```

**Default Credentials:**
- Username: `admin`
- Password: `admin`

The **Kafka Streaming Dashboard** will auto-load! 🎉

### Step 3: Start Your Producer & Consumer
```bash
python -m ad_stream_producer.run_producer
python -m consumer.run_consumer
```

Watch the metrics appear in real-time on your dashboard! 📈

---

## 📱 Dashboard Panels Overview

### 🏆 Top Row - Key Metrics (Big Numbers)
- **Total Topics** - Count of all topics in cluster
- **Total Partitions** - Sum of partitions across all topics  
- **Producer Throughput** - Events per second (your app)
- **Failed Events** - Total delivery failures

### 📤 Producer Section
- **Producer Offsets Table** - Latest offset per partition (ads_events)
- **Producer Events Rate** - Sent vs failed events over time
- **Producer Latency Percentiles** - p50, p95, p99 latencies
- **Producer Queue Backlog** - Internal queue size trend

### 📥 Consumer Section
- **Consumer Lag Table** - Lag per partition (color coded: green=healthy, yellow=warning, red=critical)
- **Consumer Offsets Table** - Current offset per partition per group
- **Consumer Lag Trend** - Lag over time for all consumer groups

### 📡 Cluster Overview
- **Topics Overview** - All topics with partition counts
- **Broker Status** - Broker health and metadata

---

## 🔗 All Available URLs

| Service | URL | Purpose |
|---------|-----|---------|
| **Grafana Dashboard** | http://localhost:3000 | Beautiful monitoring (admin/admin) |
| **Prometheus UI** | http://localhost:9090 | Raw metrics, PromQL queries |
| **Kafka UI** | http://localhost:8080 | Topic/broker/group management |
| **Kafka Exporter** | http://localhost:9308/metrics | Prometheus format Kafka metrics |
| **Your App Metrics** | http://localhost:8001/metrics | Your producer/consumer metrics |

---

## 🎯 Dashboard Features

### Auto-Refresh
- Dashboard refreshes every **10 seconds** automatically
- Change in top-right corner if needed

### Time Range
- Default view: **Last 1 hour**
- Click time picker (top-right) to change range
- Options: 5m, 15m, 1h, 6h, 24h, 7d, 30d

### Color Coding
- **Green** - Healthy (lag = 0, no failures)
- **Yellow** - Warning (lag > 100 messages)
- **Red** - Critical (lag > 500 messages)

### Interactive Tables
- Click row headers to sort
- Hover over values for tooltips
- Right-click for drill-down options

---

## 📊 Key Metrics Explained

### Producer Metrics
- **events_sent_total** - Cumulative count of successfully sent events
- **events_failed_total** - Cumulative count of delivery failures
- **events_per_second** - Current throughput rate
- **producer_latency_seconds** - Time from produce() to broker acknowledgment
- **producer_queue_size** - Messages waiting in local producer queue

### Consumer Metrics
- **kafka_partition_current_offset** - Latest broker offset
- **kafka_consumergroup_current_offset** - Consumer group's current position
- **kafka_consumergroup_lag** - Distance between consumer and producer

### Calculated Metrics
- **Producer Latency Percentiles** - p50, p95, p99 latencies (histogram_quantile)
- **Throughput Rate** - rate(events_sent_total[1m])
- **Total Consumer Lag** - sum(kafka_consumergroup_lag by group)

---

## 🛠️ Troubleshooting

### Metrics Not Appearing?

1. **Check Prometheus Targets:**
   - Go to http://localhost:9090/targets
   - All targets should show **UP** in green

2. **Verify Kafka Exporter:**
   ```bash
   docker logs kafka-develop-kafka-exporter-1
   ```
   Should see messages like "Brokers added to list"

3. **Check Your App Metrics:**
   ```bash
   curl http://localhost:8001/metrics
   ```
   Should return Prometheus format metrics

4. **Verify Prometheus Config:**
   - View at http://localhost:9090/config
   - Check all scrape_configs are present

### Grafana Dashboard Not Loading?

1. **Check Prometheus Datasource:**
   - Settings (gear icon) → Data Sources
   - Prometheus should show green checkmark

2. **Manually Import Dashboard:**
   - Dashboards → New → Import
   - Upload: `monitoring/grafana-dashboards/kafka-dashboard.json`

3. **Check Grafana Logs:**
   ```bash
   docker logs kafka-develop-grafana-1
   ```

### Port Conflicts?

If ports are already in use, edit `docker-compose.yaml`:
- Prometheus: Change `"9090:9090"` to `"9091:9090"`
- Grafana: Change `"3000:3000"` to `"3001:3000"`
- Kafka Exporter: Change `"9308:9308"` to `"9309:9308"`

Then update `prometheus.yml` scrape targets accordingly.

---

## 📝 Example PromQL Queries

Use these in Grafana or Prometheus for custom dashboards:

```promql
# Producer throughput
rate(events_sent_total[1m])

# Producer latency (95th percentile)
histogram_quantile(0.95, sum(rate(producer_latency_seconds_bucket[5m])) by (le))

# Consumer lag per group
sum by (group) (kafka_consumergroup_lag)

# Total messages in system
sum(kafka_partition_current_offset)

# Failed event rate
rate(events_failed_total[1m])

# Per-partition lag
kafka_consumergroup_lag{topic="ads_events"}
```

---

## 🎨 Customizing the Dashboard

### Add a New Panel
1. Click **+** button → **Add panel**
2. Select **Prometheus** datasource
3. Write a PromQL query (examples above)
4. Customize:
   - **Visualization Type** (Graph, Table, Stat, Gauge, etc.)
   - **Title & Description**
   - **Color scheme & thresholds**
   - **Legend & tooltip options**
5. Save

### Add Template Variables
Create dynamic dashboards that let you pick a topic or consumer group:

1. Dashboard Settings (gear icon)
2. Variables → New variable
3. Query type: **Prometheus**
4. Query: `label_values(kafka_topic_partitions, topic)`
5. Name: `topic`

Now add `$topic` to your queries: `kafka_partition_current_offset{topic="$topic"}`

### Add Alerts
1. Edit Panel
2. Scroll to "Alert" section
3. Set condition: e.g., "if `sum(kafka_consumergroup_lag) > 1000`"
4. Configure notification channel (email, Slack, etc.)
5. Save

---

## 📂 Configuration Files

| File | Purpose |
|------|---------|
| `monitoring/prometheus.yml` | Prometheus scrape config |
| `monitoring/grafana-datasources.yml` | Grafana datasource config |
| `monitoring/grafana-dashboards-provision.yml` | Dashboard auto-provisioning |
| `monitoring/grafana-dashboards/kafka-dashboard.json` | The main dashboard |

---

## 🔄 How It Works

```
Your Producer/Consumer (port 8001)
    ↓
Prometheus scrapes every 15s
    ├── Kafka Exporter (9308) → broker/topic/consumer metrics
    └── Your App (8001) → events_sent, latency, etc.
    ↓
Prometheus stores time-series data
    ↓
Grafana queries Prometheus every 10s
    ↓
Beautiful Dashboard refreshes in browser
```

---

## 🧹 Cleanup

To stop all monitoring services:
```bash
docker-compose down
```

To remove volumes (including prometheus data):
```bash
docker-compose down -v
```

To stop only monitoring services (keep Kafka):
```bash
docker-compose stop kafka-exporter prometheus grafana
```

---

## 📚 Resources

- **Prometheus Docs:** https://prometheus.io/docs/
- **Grafana Docs:** https://grafana.com/docs/grafana/latest/
- **PromQL Guide:** https://prometheus.io/docs/prometheus/latest/querying/basics/
- **Kafka Exporter:** https://github.com/danielqsj/kafka-exporter

---

## ✨ Next Steps

1. ✅ Dashboard running? Great!
2. ⏭️ Start producing/consuming data
3. 📊 Create custom dashboards for your metrics
4. 🔔 Set up alerts for SLOs
5. 📈 Track performance trends over time

---

**Enjoy your beautiful Kafka monitoring dashboard! 🚀**

Questions? Check `DASHBOARD_SETUP.md` for detailed setup instructions.

