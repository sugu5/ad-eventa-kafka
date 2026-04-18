-- SQL Schema for 5-Minute Event Statistics
-- This table stores aggregated event counts by event_type and device for the last 5 minutes

CREATE SCHEMA IF NOT EXISTS event_schema;

CREATE TABLE IF NOT EXISTS event_schema.event_type_device_stats (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    device VARCHAR(50) NOT NULL,
    event_count INT NOT NULL DEFAULT 0,
    aggregation_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(event_type, device, aggregation_time)
);

-- Index for faster queries on recent aggregations
CREATE INDEX IF NOT EXISTS idx_aggregation_time
ON event_schema.event_type_device_stats(aggregation_time DESC);

-- Index for filtering by event_type and device
CREATE INDEX IF NOT EXISTS idx_event_device
ON event_schema.event_type_device_stats(event_type, device);

-- Query to get last 5 minutes of statistics
-- SELECT event_type, device, event_count, aggregation_time
-- FROM event_schema.event_type_device_stats
-- WHERE aggregation_time >= NOW() - INTERVAL '5 minutes'
-- ORDER BY aggregation_time DESC, event_type, device;

-- Query to get statistics for specific event type
-- SELECT device, SUM(event_count) as total_count, aggregation_time
-- FROM event_schema.event_type_device_stats
-- WHERE event_type = 'view' AND aggregation_time >= NOW() - INTERVAL '5 minutes'
-- GROUP BY device, aggregation_time
-- ORDER BY aggregation_time DESC;

-- Query to get most popular combinations (last 5 minutes)
-- SELECT event_type, device, SUM(event_count) as total_count
-- FROM event_schema.event_type_device_stats
-- WHERE aggregation_time >= NOW() - INTERVAL '5 minutes'
-- GROUP BY event_type, device
-- ORDER BY total_count DESC;

