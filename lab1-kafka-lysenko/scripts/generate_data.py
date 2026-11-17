"""Deterministic synthetic hydro telemetry, variant 3B. No operational plant data."""
from __future__ import annotations
import csv
from datetime import datetime, timedelta, timezone
import json
import math
import random
from common import ROOT, configure_stdout

TYPES = ("kaplan", "francis", "pelton")
START = datetime(2026, 11, 10, tzinfo=timezone.utc)


def hydro_record(index: int) -> dict:
    device = index % 15
    step = index // 15
    rng = random.Random(610013 + index)
    phase = 2 * math.pi * (step * 30 / 86400) + device / 9
    flow = max(500, min(3000, 1400 + device * 55 + 240 * math.sin(phase) + rng.uniform(-8, 8)))
    level = max(10, min(25, 16 + device / 7 + 0.8 * math.cos(phase / 2) + rng.uniform(-0.02, 0.02)))
    nominal = 55 + device * 10
    power = max(10, min(200, nominal * (0.8 + 0.08 * math.sin(phase))))
    voltage = 15200 + device * 30 + rng.uniform(-10, 10)
    return {
        "device_id": f"HYDRO_DN_{device+1:03d}",
        "timestamp": (START + timedelta(seconds=step * 30)).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "power_output": round(power, 2), "efficiency": round(90 + 2 * math.sin(phase / 3), 2),
        "temperature": round(12 + 3 * math.sin(phase / 2), 2), "voltage": round(voltage, 2),
        "current": round(max(500, min(8000, power * 1e6 / (math.sqrt(3) * voltage * 0.95))), 2),
        "status": "standby" if index % 101 == 0 else "maintenance" if index % 257 == 0 else "generating",
        "location": {"lat": round(47.55 + device * 0.095, 4), "lon": round(33.1 + device * 0.19, 4)},
        "maintenance_hours": max(500, 6000 - device * 250 - step // 120),
        "water_flow": round(flow, 2), "water_level": round(level, 3), "turbine_type": TYPES[device % 3],
    }


def main():
    configure_stdout()
    data = ROOT / "data"
    evidence = ROOT / "evidence"
    data.mkdir(exist_ok=True)
    evidence.mkdir(exist_ok=True)
    records = [hydro_record(index) for index in range(1000)]
    lines = [json.dumps(item, ensure_ascii=False, separators=(",", ":"), allow_nan=False) for item in records]
    (data / "hydro-1000.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (evidence / "sample-records.json").write_text(json.dumps(records[:3], ensure_ascii=False, indent=2), encoding="utf-8")
    sizes = [len(line.encode("utf-8")) for line in lines]
    # Seven complete days at the variant's natural rate: 15 devices / 30 seconds.
    hourly = {}
    for index in range(15 * 2880 * 7):
        item = hydro_record(index)
        hour = item["timestamp"][:13]
        key = (hour, item["turbine_type"])
        row = hourly.setdefault(key, {"n": 0, "water_flow": 0.0, "water_level": 0.0, "power_output": 0.0})
        row["n"] += 1
        for field in ("water_flow", "water_level", "power_output"):
            row[field] += item[field]
    with (evidence / "history-hourly.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["hour_utc", "turbine_type", "records", "mean_water_flow_m3s", "mean_water_level_m", "mean_power_mw"])
        for (hour, turbine), row in sorted(hourly.items()):
            writer.writerow([hour, turbine, row["n"], *(round(row[field] / row["n"], 4) for field in ("water_flow", "water_level", "power_output"))])
    summary = {"records": 1000, "devices": 15, "event_interval_seconds": 30, "seed": 610013,
               "bytes_min": min(sizes), "bytes_max": max(sizes), "bytes_mean": sum(sizes) / len(sizes),
               "history_records": 15 * 2880 * 7, "history_hours": 168, "data_kind": "synthetic"}
    (evidence / "dataset.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

