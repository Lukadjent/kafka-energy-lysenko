"""Generate, load and query synthetic Dnipro cascade telemetry on Apache Cassandra."""

import argparse
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path
import platform
from random import Random
from statistics import fmean, median
from time import perf_counter

import cassandra
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent
from cassandra.query import BatchStatement, BatchType


ROOT = Path(__file__).resolve().parent
KEYSPACE = "dnipro_hydro_lysenko"
DAY = date(2025, 7, 15)
HISTORY_START = date(2022, 12, 1)
HISTORY_END = date(2025, 12, 1)
SEASONS = ("winter", "spring", "summer", "autumn")
INTERVAL = 15
STATIONS = {
    "KYIV": (102.6, 11.4, 265),
    "KANIV": (91.5, 12.1, 320),
    "DNIPRO": (51.4, 35.2, 355),
}
FIELDS = {
    "turbine_readings": "unit_id, day, ts, station_id, upstream_level_m, flow_m3s, head_m, power_mw",
    "reservoir_readings": "station_id, day, ts, level_m, inflow_m3s, water_temp_c",
    "station_daily": "station_id, year, day, energy_mwh, mean_mw, peak_mw",
    "seasonal_by_station": "station_id, season_year, season, days, energy_mwh, mean_mw, peak_mw",
    "seasonal_by_season": "station_id, season_year, season, days, energy_mwh, mean_mw, peak_mw",
}


def make_data():
    rng = Random(13)
    readings, reservoirs, daily, archives = [], [], [], []
    start = datetime.combine(DAY, time.min, tzinfo=timezone.utc)
    for index, (station, (level, head, base_flow)) in enumerate(STATIONS.items()):
        total_power = []
        for hour in range(24):
            cycle = math.sin(math.tau * (hour - 8) / 24)
            reservoirs.append((station, DAY, start + timedelta(hours=hour),
                               round(level + 0.18 * cycle, 3),
                               round(base_flow * 5 * (1.07 + 0.10 * cycle), 2),
                               round(22.4 + 1.1 * cycle + 0.4 * index, 2)))
        for minute in range(0, 24 * 60, INTERVAL):
            cycle = math.sin(math.tau * (minute - 480) / 1440)
            station_power = []
            for unit in range(5):
                upstream = round(level + 0.18 * cycle + rng.uniform(-0.02, 0.02), 3)
                flow = round(base_flow * (0.92 + 0.12 * cycle + 0.025 * unit
                                          + rng.uniform(-0.04, 0.04)), 2)
                net_head = round(head + 0.23 * cycle + rng.uniform(-0.12, 0.12), 3)
                power = round(0.00981 * 0.90 * flow * net_head, 3)
                id_ = f"HYDRO_DN_{index * 5 + unit + 1:03d}"
                readings.append((id_, DAY, start + timedelta(minutes=minute), station,
                                 upstream, flow, net_head, power))
                station_power.append(power)
            total_power.append(sum(station_power))

        grouped = defaultdict(list)
        for days in range((HISTORY_END - HISTORY_START).days):
            day = HISTORY_START + timedelta(days=days)
            if day == DAY:
                mean_mw, peak_mw = fmean(total_power), max(total_power)
            else:
                cycle = math.cos(math.tau * (day.timetuple().tm_yday - 105) / 365)
                mean_mw = 5 * 0.00981 * 0.90 * base_flow * head * (0.97 + 0.10 * cycle)
                mean_mw *= rng.uniform(0.91, 1.09)
                peak_mw = mean_mw * rng.uniform(1.10, 1.22)
            values = (round(mean_mw * 24, 4), round(mean_mw, 4), round(peak_mw, 4))
            daily.append((station, day.year, day, *values))
            season = SEASONS[(day.month % 12) // 3]
            grouped[day.year + (day.month == 12), season].append(values)
        for (year, season), values in grouped.items():
            energy = math.fsum(row[0] for row in values)
            archives.append((station, year, season, len(values), round(energy, 4),
                             round(energy / (24 * len(values)), 4),
                             max(row[2] for row in values)))
    return readings, reservoirs, daily, archives


def prepare(statement, session):
    prepared = session.prepare(statement)
    prepared.consistency_level = ConsistencyLevel.LOCAL_ONE
    return prepared


def write_table(session, table, rows, batch=False):
    fields = FIELDS[table]
    markers = ", ".join("?" for _ in fields.split(","))
    insert = prepare(f"INSERT INTO {table} ({fields}) VALUES ({markers})", session)
    if batch:
        # Only the two telemetry tables use this path. Their partition key is fields 0 and 1.
        partitions = defaultdict(list)
        for row in rows:
            partitions[row[:2]].append(row)
        jobs = []
        for partition in partitions.values():
            for offset in range(0, len(partition), 12):
                statement = BatchStatement(batch_type=BatchType.UNLOGGED,
                                           consistency_level=ConsistencyLevel.LOCAL_ONE)
                for row in partition[offset:offset + 12]:
                    statement.add(insert, row)
                jobs.append((statement, None))
    else:
        jobs = [(insert, row) for row in rows]
    start = perf_counter()
    execute_concurrent(session, jobs, concurrency=16)
    duration = perf_counter() - start
    return {"rows": len(rows), "requests": len(jobs), "seconds": duration,
            "rows_per_s": len(rows) / duration, "mode": "UNLOGGED" if batch else "prepared"}


def compare_archive_keys(session, archive):
    fields = FIELDS["seasonal_by_station"]
    base = f"SELECT {fields} FROM "
    station_year = prepare(base + "seasonal_by_station WHERE station_id = ? AND season_year = ?", session)
    station_point = prepare(
        base + "seasonal_by_station WHERE station_id = ? AND season_year = ? AND season = ?", session)
    season_all = prepare(base + "seasonal_by_season WHERE season_year = ? AND season = ?", session)
    season_point = prepare(
        base + "seasonal_by_season WHERE season_year = ? AND season = ? AND station_id = ?", session)
    layouts = ("by_station", "by_season")
    results = {}
    for scenario in ("station_year", "cascade_season"):
        times = {name: [] for name in layouts}
        for trial in range(70):
            year = 2023 + trial % 3
            if scenario == "station_year":
                station = tuple(STATIONS)[(trial // 3) % len(STATIONS)]
                queries = {
                    "by_station": [(station_year, (station, year))],
                    "by_season": [(season_point, (year, season, station)) for season in SEASONS],
                }
                expected = sorted(row for row in archive if row[:2] == (station, year))
            else:
                season = SEASONS[(trial // 3) % len(SEASONS)]
                queries = {
                    "by_station": [(station_point, (station, year, season)) for station in STATIONS],
                    "by_season": [(season_all, (year, season))],
                }
                expected = sorted(row for row in archive if row[1:3] == (year, season))
            for layout in layouts[::1 if trial % 2 == 0 else -1]:
                start = perf_counter()
                rows = [tuple(row) for statement, parameters in queries[layout]
                        for row in session.execute(statement, parameters)]
                elapsed_ms = (perf_counter() - start) * 1000
                if sorted(rows) != expected:
                    raise RuntimeError(f"Archive contents differ: {scenario}, {layout}, trial {trial}")
                if trial >= 10:
                    times[layout].append(elapsed_ms)
        results[scenario] = {
            layout: {"requests": len(queries[layout]), "rows": len(expected),
                     "p50_ms": median(samples),
                     "p95_ms": sorted(samples)[math.ceil(0.95 * len(samples)) - 1],
                     "samples_ms": samples}
            for layout, samples in times.items()
        }
    return results


def run(session):
    node = session.execute("SELECT cluster_name, data_center, release_version FROM system.local").one()
    print(f"Cassandra {node.release_version} | {node.cluster_name} | DC {node.data_center}")
    print(f"Python {platform.python_version()} | cassandra-driver {cassandra.__version__} | LOCAL_ONE")
    for statement in (ROOT / "schema.cql").read_text(encoding="utf-8").split(";"):
        if statement.strip():
            session.execute(statement.strip())
    print(f"Keyspace: {KEYSPACE} | generated UTC day: {DAY}")
    readings, reservoirs, daily, archive = make_data()
    sources = (readings, reservoirs, daily, archive, archive)
    writes = {}
    for table, rows in zip(FIELDS, sources, strict=True):
        result = write_table(session, table, rows, batch=table.endswith("_readings"))
        actual = session.execute(f"SELECT count(*) FROM {table}").one().count
        if actual != len(rows):
            raise RuntimeError(f"{table}: Cassandra returned {actual} rows, expected {len(rows)}")
        writes[table] = result
        print(f"{table:<22} {actual:>4} rows | {result['requests']:>4} requests | "
              f"{result['seconds']:.3f} s | {result['rows_per_s']:.0f} rows/s")

    query = prepare(
        "SELECT ts, power_mw FROM turbine_readings WHERE unit_id = ? AND day = ?", session)
    from_db = defaultdict(lambda: defaultdict(list))
    latest = []
    for station_index, station in enumerate(STATIONS):
        for unit in range(1, 6):
            unit_id = f"HYDRO_DN_{station_index * 5 + unit:03d}"
            values = list(session.execute(query, (unit_id, DAY)))
            if len(values) != 96 or values[0].ts <= values[-1].ts:
                raise RuntimeError(f"Wrong telemetry count or clustering order for {unit_id}")
            for row in values:
                from_db[station][row.ts].append(row.power_mw)
            if not latest:
                latest = [{"ts": row.ts.replace(tzinfo=timezone.utc).isoformat(), "power_mw": row.power_mw}
                          for row in values[:3]]

    totals = {}
    daily_query = prepare(
        "SELECT energy_mwh, mean_mw, peak_mw FROM station_daily WHERE station_id = ? AND year = ? AND day = ?",
        session)
    print("\nLatest HYDRO_DN_001 readings (UTC):")
    for item in latest:
        print(f"  {item['ts'][11:19]}  {item['power_mw']:.3f} MW")
    print(f"\nStation daily aggregates reconstructed from Cassandra telemetry ({DAY} UTC):")
    for station, buckets in from_db.items():
        if len(buckets) != 96 or any(len(values) != 5 for values in buckets.values()):
            raise RuntimeError(f"Incomplete observation buckets for {station}")
        values = [math.fsum(powers) for powers in buckets.values()]
        mean, peak, energy = fmean(values), max(values), math.fsum(values) * INTERVAL / 60
        daily_row = session.execute(daily_query, (station, DAY.year, DAY)).one()
        if any(abs(measured - stored) > 0.000051
               for measured, stored in zip((energy, mean, peak), daily_row, strict=True)):
            raise RuntimeError(f"Daily record does not match telemetry for {station}")
        totals[station] = {"mean_mw": mean, "peak_mw": peak, "energy_mwh": energy}
        print(f"  {station:<6}  mean {mean:>7.2f} MW | peak {peak:>7.2f} MW | energy {energy:>8.2f} MWh")

    kyiv, kaniv = (totals[station]["mean_mw"] for station in ("KYIV", "KANIV"))
    delta = kaniv - kyiv
    print(f"\nVariant 3B: KYIV {kyiv:.2f} MW | KANIV {kaniv:.2f} MW | KANIV - KYIV {delta:+.2f} MW")
    print("Comparison is based on mean generation over the same 24 hours.")

    archive_queries = compare_archive_keys(session, archive)
    print("\nArchive reads: 60 measured repetitions/layout/scenario (10 warm-ups).")
    for scenario, profiles in archive_queries.items():
        for layout, result in profiles.items():
            print(f"  {scenario:<14} {layout:<10} {result['requests']} queries | "
                  f"p50 {result['p50_ms']:.3f} ms | p95 {result['p95_ms']:.3f} ms")
    print("PASS: all table counts, daily rollups, and archive result sets verified.")
    return {"node": node._asdict(), "python": platform.python_version(),
            "driver": cassandra.__version__, "day": DAY.isoformat(), "writes": writes,
            "latest": latest, "station_daily": totals,
            "comparison": {"difference_mw": delta, "difference_pct": delta * 100 / kyiv},
            "archive_queries": archive_queries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=9042, type=int)
    parser.add_argument("--output", default=ROOT / "evidence" / "results.json", type=Path)
    options = parser.parse_args()
    with Cluster([options.host], port=options.port, protocol_version=4) as cluster:
        results = run(cluster.connect())
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
