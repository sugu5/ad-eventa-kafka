-- init_db.sql — runs automatically on first PostgreSQL start
CREATE SCHEMA IF NOT EXISTS event_schema;

CREATE TABLE IF NOT EXISTS event_schema.ad_events (
    event_id      VARCHAR(36) PRIMARY KEY,
    event_time    TIMESTAMP   NOT NULL,
    user_id       VARCHAR(36) NOT NULL,
    campaign_id   INTEGER     NOT NULL,
    ad_id         INTEGER     NOT NULL,
    device        VARCHAR(20) NOT NULL,
    country       VARCHAR(10) NOT NULL,
    event_type    VARCHAR(20) NOT NULL,
    ad_creative_id VARCHAR(36)
);

-- Index for common analytical queries
CREATE INDEX IF NOT EXISTS idx_ad_events_event_time ON event_schema.ad_events (event_time);
CREATE INDEX IF NOT EXISTS idx_ad_events_user_id    ON event_schema.ad_events (user_id);
CREATE INDEX IF NOT EXISTS idx_ad_events_event_type ON event_schema.ad_events (event_type);

