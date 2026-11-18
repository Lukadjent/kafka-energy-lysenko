CREATE KEYSPACE IF NOT EXISTS dnipro_hydro_lysenko
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

USE dnipro_hydro_lysenko;

CREATE TABLE IF NOT EXISTS turbine_readings (
    unit_id text,
    day date,
    ts timestamp,
    station_id text,
    upstream_level_m double,
    flow_m3s double,
    head_m double,
    power_mw double,
    PRIMARY KEY ((unit_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS reservoir_readings (
    station_id text,
    day date,
    ts timestamp,
    level_m double,
    inflow_m3s double,
    water_temp_c double,
    PRIMARY KEY ((station_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS station_daily (
    station_id text,
    year int,
    day date,
    energy_mwh double,
    mean_mw double,
    peak_mw double,
    PRIMARY KEY ((station_id, year), day)
) WITH CLUSTERING ORDER BY (day DESC);

CREATE TABLE IF NOT EXISTS seasonal_by_station (
    station_id text,
    season_year int,
    season text,
    days int,
    energy_mwh double,
    mean_mw double,
    peak_mw double,
    PRIMARY KEY (station_id, season_year, season)
) WITH CLUSTERING ORDER BY (season_year DESC, season ASC);

CREATE TABLE IF NOT EXISTS seasonal_by_season (
    season_year int,
    season text,
    station_id text,
    days int,
    energy_mwh double,
    mean_mw double,
    peak_mw double,
    PRIMARY KEY ((season_year, season), station_id)
) WITH CLUSTERING ORDER BY (station_id ASC);
