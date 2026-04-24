-- Query 1: On-time events summary
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT event_id) as unique_events,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT event_date) as event_days
FROM 'output/iceberg_warehouse/ad_events/data/*/*.parquet';

-- Query 2: Event distribution by type
SELECT 
    event_type, 
    device, 
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM 'output/iceberg_warehouse/ad_events/data/*/*.parquet'
GROUP BY event_type, device
ORDER BY event_count DESC
LIMIT 20;

-- Query 3: Events by country
SELECT 
    country, 
    COUNT(*) as count,
    COUNT(DISTINCT user_id) as unique_users
FROM 'output/iceberg_warehouse/ad_events/data/*/*.parquet'
GROUP BY country
ORDER BY count DESC;

-- Query 4: Campaign performance
SELECT 
    campaign_id, 
    COUNT(*) as impressions,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT device) as device_count
FROM 'output/iceberg_warehouse/ad_events/data/*/*.parquet'
GROUP BY campaign_id
ORDER BY impressions DESC
LIMIT 15;

-- Query 5: Late events analysis
SELECT 
    event_date,
    COUNT(*) as late_count
FROM 'output/iceberg_warehouse/ad_events_late/data/*/*.parquet'
GROUP BY event_date
ORDER BY event_date DESC;

-- Query 6: Aggregations over time
SELECT 
    window_end,
    event_type,
    SUM(event_count) as total_events
FROM 'output/iceberg_warehouse/ad_events_aggregation/data/*/*.parquet'
GROUP BY window_end, event_type
ORDER BY window_end DESC
LIMIT 20;

-- Query 7: Top performing devices
SELECT 
    device,
    COUNT(*) as events,
    COUNT(DISTINCT campaign_id) as campaigns,
    COUNT(DISTINCT user_id) as users
FROM 'output/iceberg_warehouse/ad_events/data/*/*.parquet'
GROUP BY device
ORDER BY events DESC;

-- Query 8: Event time distribution
SELECT 
    event_date,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT campaign_id) as campaigns
FROM 'output/iceberg_warehouse/ad_events/data/*/*.parquet'
GROUP BY event_date
ORDER BY event_date DESC
LIMIT 30;
