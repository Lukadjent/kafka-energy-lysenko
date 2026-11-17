-- Idempotent landing key and five-minute aggregates for a warehouse.
CREATE TABLE IF NOT EXISTS hydro_landing (
  device_id TEXT NOT NULL,
  event_time TEXT NOT NULL,
  turbine_type TEXT NOT NULL,
  water_flow REAL NOT NULL,
  water_level REAL NOT NULL,
  power_output REAL NOT NULL,
  PRIMARY KEY (device_id, event_time)
);
SELECT datetime((unixepoch(event_time) / 300) * 300, 'unixepoch') AS window_start_utc,
       device_id, turbine_type, COUNT(*) AS readings,
       AVG(water_flow) AS mean_flow_m3s, AVG(water_level) AS mean_level_m,
       MIN(water_level) AS min_level_m, MAX(water_level) AS max_level_m,
       AVG(power_output) AS mean_power_mw
FROM hydro_landing
GROUP BY window_start_utc, device_id, turbine_type;

