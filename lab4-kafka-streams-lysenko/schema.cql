CREATE KEYSPACE IF NOT EXISTS energy_l4_lysenko WITH replication={'class':'NetworkTopologyStrategy','dc1':3};
CREATE TABLE IF NOT EXISTS energy_l4_lysenko.cascade_flow_aggregates (run_id text, window_start timestamp, payload text, PRIMARY KEY ((run_id),window_start));
CREATE TABLE IF NOT EXISTS energy_l4_lysenko.station_coordination (run_id text, station text, window_start timestamp, predicted_level double, coordinated_flow double, PRIMARY KEY ((run_id,station),window_start));
CREATE TABLE IF NOT EXISTS energy_l4_lysenko.anomalies (run_id text, event_id text, payload text, PRIMARY KEY ((run_id),event_id));
CREATE TABLE IF NOT EXISTS energy_l4_lysenko.saga_state (saga_id text PRIMARY KEY, next_step int, status text, fail_lower boolean);
CREATE TABLE IF NOT EXISTS energy_l4_lysenko.saga_log (saga_id text, sequence int, action text, status text, PRIMARY KEY ((saga_id),sequence));
CREATE TABLE IF NOT EXISTS energy_l4_lysenko.participant (station_id text PRIMARY KEY, gate double, prepared boolean, version int, commands map<text,text>);
