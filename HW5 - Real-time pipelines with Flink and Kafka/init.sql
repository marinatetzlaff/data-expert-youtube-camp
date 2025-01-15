-- Create processed_events_aggregated_ip table to land the aggregated data
CREATE TABLE IF NOT EXISTS processed_events_aggregated_ip (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    ip VARCHAR,
    num_hits BIGINT
);