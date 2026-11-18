CREATE INDEX IF NOT EXISTS daily_level_idx ON energy_l3_lysenko.daily (level_band);

-- Cassandra MV predicates cannot contain water_level > threshold.
-- level_band is a deterministic write-time category; the view uses valid CQL.
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_l3_lysenko.by_level_band AS
 SELECT * FROM energy_l3_lysenko.daily
 WHERE level_band IS NOT NULL AND day IS NOT NULL
 AND device_id IS NOT NULL AND event_time IS NOT NULL
 PRIMARY KEY ((level_band,day),device_id,event_time)
 WITH CLUSTERING ORDER BY (device_id ASC,event_time DESC);

-- type_hour is one derived non-key column. All three base-key columns remain.
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_l3_lysenko.by_type_hour AS
 SELECT * FROM energy_l3_lysenko.daily
 WHERE type_hour IS NOT NULL AND day IS NOT NULL
 AND device_id IS NOT NULL AND event_time IS NOT NULL
 PRIMARY KEY ((type_hour),day,device_id,event_time)
 WITH CLUSTERING ORDER BY (day DESC,device_id ASC,event_time DESC);
