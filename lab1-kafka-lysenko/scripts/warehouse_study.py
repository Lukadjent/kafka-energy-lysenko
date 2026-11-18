"""Execute the analytics-B warehouse/window study on seven synthetic days."""
import json
import sqlite3
import time
from common import ROOT, configure_stdout
from generate_data import hydro_record


def main():
    configure_stdout()
    connection = sqlite3.connect(ROOT / "data" / "warehouse.sqlite")
    sql = (ROOT / "config" / "warehouse.sql").read_text(encoding="utf-8")
    connection.executescript(sql[:sql.index("SELECT")])
    started = time.perf_counter()
    count = 15 * 2880 * 7
    batch = []
    for index in range(count):
        row = hydro_record(index)
        batch.append((row["device_id"], row["timestamp"], row["turbine_type"], row["water_flow"], row["water_level"], row["power_output"]))
        if len(batch) == 10000 or index == count - 1:
            connection.executemany("INSERT OR IGNORE INTO hydro_landing VALUES (?,?,?,?,?,?)", batch)
            connection.commit()
            batch = []
    actual = connection.execute("SELECT COUNT(*) FROM hydro_landing").fetchone()[0]
    assert actual == count
    results = []
    for seconds in (30, 300, 900, 3600):
        start = time.perf_counter()
        rows = connection.execute(f"SELECT (unixepoch(event_time)/{seconds})*{seconds}, device_id, turbine_type, COUNT(*), AVG(water_flow), AVG(water_level), MIN(water_level), MAX(water_level), AVG(power_output), SUM(water_flow), SUM(water_level), SUM(power_output), MIN(power_output), MAX(power_output) FROM hydro_landing GROUP BY 1,2,3").fetchall()
        elapsed = time.perf_counter() - start
        assert sum(row[3] for row in rows) == count
        mean_wait, max_wait = connection.execute(f"SELECT AVG((unixepoch(event_time)/{seconds})*{seconds}+{seconds}-unixepoch(event_time)), MAX((unixepoch(event_time)/{seconds})*{seconds}+{seconds}-unixepoch(event_time)) FROM hydro_landing").fetchone()
        results.append({"window_seconds": seconds, "input_records": count, "aggregate_rows": len(rows),
            "row_reduction_pct": 100 * (1 - len(rows) / count), "query_seconds": elapsed,
            "modeled_mean_wait_seconds": mean_wait, "modeled_max_wait_seconds": max_wait})
        if seconds == 300:
            names = ("window_start_epoch", "device_id", "turbine_type", "cnt", "mean_flow", "mean_level", "min_level", "max_level", "mean_power", "sum_flow", "sum_level", "sum_power", "min_power", "max_power")
            (ROOT / "evidence" / "warehouse-sample.json").write_text(json.dumps([dict(zip(names, row)) for row in rows[:6]], indent=2), encoding="utf-8")
            connection.execute("CREATE TABLE IF NOT EXISTS hydro_rollup_5m (window_start_epoch INTEGER, device_id TEXT, turbine_type TEXT, cnt INTEGER, mean_flow REAL, mean_level REAL, min_level REAL, max_level REAL, mean_power REAL, sum_flow REAL, sum_level REAL, sum_power REAL, min_power REAL, max_power REAL, PRIMARY KEY (device_id, window_start_epoch))")
            connection.executemany("INSERT OR REPLACE INTO hydro_rollup_5m VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            connection.commit()
    direct = connection.execute("SELECT substr(event_time,1,10), device_id, COUNT(*), SUM(power_output), MIN(power_output), MAX(power_output) FROM hydro_landing GROUP BY 1,2 ORDER BY 1,2").fetchall()
    rolled = connection.execute("SELECT date(window_start_epoch,'unixepoch'), device_id, SUM(cnt), SUM(sum_power), MIN(min_power), MAX(max_power) FROM hydro_rollup_5m GROUP BY 1,2 ORDER BY 1,2").fetchall()
    import math
    assert len(direct) == len(rolled) == 7 * 15
    assert all(a[:3] == b[:3] and all(math.isclose(x, y, rel_tol=1e-10, abs_tol=1e-7) for x, y in zip(a[3:], b[3:])) for a, b in zip(direct, rolled))
    connection.close()
    summary = {"engine": "SQLite", "historical_days": 7, "input_records": count,
        "database_bytes": (ROOT / "data" / "warehouse.sqlite").stat().st_size,
        "total_seconds": time.perf_counter() - started, "windows": results,
        "rollup_contract": {"stored_statistics": ["cnt", "sum", "min", "max"],
            "daily_groups_verified_against_raw": len(direct), "weighted_mean": "SUM(sum_power)/SUM(cnt)",
            "all_daily_count_sum_min_max_match": True}}
    (ROOT / "evidence" / "warehouse.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("WAREHOUSE PASS: 302400 records; all four window aggregations reconcile", flush=True)
    print("WAREHOUSE_RESULT " + json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
