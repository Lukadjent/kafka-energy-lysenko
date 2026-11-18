CREATE KEYSPACE IF NOT EXISTS energy_l3_lysenko
WITH replication = {'class':'NetworkTopologyStrategy','dc1':3};

CREATE TABLE IF NOT EXISTS energy_l3_lysenko.simple (
 device_id text, event_time timestamp, day date, hour timestamp, week date,
 station_id text, turbine_type text, water_flow double, water_level double,
 power_output double, efficiency double, level_band text, type_hour text,
 PRIMARY KEY (device_id,event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
AND compression = {'class':'LZ4Compressor','chunk_length_in_kb':16};

CREATE TABLE IF NOT EXISTS energy_l3_lysenko.hourly (
 device_id text, event_time timestamp, day date, hour timestamp, week date,
 station_id text, turbine_type text, water_flow double, water_level double,
 power_output double, efficiency double, level_band text, type_hour text,
 PRIMARY KEY ((device_id,hour),event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
AND compression = {'class':'LZ4Compressor','chunk_length_in_kb':16};

CREATE TABLE IF NOT EXISTS energy_l3_lysenko.daily (
 device_id text, event_time timestamp, day date, hour timestamp, week date,
 station_id text, turbine_type text, water_flow double, water_level double,
 power_output double, efficiency double, level_band text, type_hour text,
 PRIMARY KEY ((device_id,day),event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
AND compression = {'class':'LZ4Compressor','chunk_length_in_kb':16};

CREATE TABLE IF NOT EXISTS energy_l3_lysenko.weekly_rollup (
 station_id text, week date, cnt bigint, sum_flow double, min_level double,
 max_level double, energy_mwh double,
 PRIMARY KEY ((station_id),week)
) WITH CLUSTERING ORDER BY (week DESC);
