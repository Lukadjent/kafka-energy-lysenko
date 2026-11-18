SHOW VERSION;
SELECT cluster_name, data_center, release_version FROM system.local;
USE dnipro_hydro_lysenko;
SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'dnipro_hydro_lysenko';
DESCRIBE TABLES;

INSERT INTO turbine_readings (unit_id, day, ts, station_id, upstream_level_m, flow_m3s, head_m, power_mw)
VALUES ('DEMO', '2025-07-15', '2025-07-15T00:00:00Z', 'KYIV', 102.6, 115, 11.4, 11.6);

BEGIN UNLOGGED BATCH
INSERT INTO turbine_readings (unit_id, day, ts, station_id, upstream_level_m, flow_m3s, head_m, power_mw)
VALUES ('DEMO', '2025-07-15', '2025-07-15T00:15:00Z', 'KYIV', 102.6, 116, 11.4, 11.7);
INSERT INTO turbine_readings (unit_id, day, ts, station_id, upstream_level_m, flow_m3s, head_m, power_mw)
VALUES ('DEMO', '2025-07-15', '2025-07-15T00:30:00Z', 'KYIV', 102.7, 117, 11.4, 11.8);
APPLY BATCH;

SELECT unit_id, ts, power_mw FROM turbine_readings
WHERE unit_id = 'DEMO' AND day = '2025-07-15' LIMIT 3;

DELETE FROM turbine_readings WHERE unit_id = 'DEMO' AND day = '2025-07-15';
