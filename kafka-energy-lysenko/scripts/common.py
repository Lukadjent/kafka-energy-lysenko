"""Shared configuration and domain rules; no broker connection at import time."""
from __future__ import annotations

import json
import math
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ROOT / ".runtime" / "python-packages"
if PACKAGES.exists():
    sys.path.insert(0, str(PACKAGES))
CONFIG = json.loads((ROOT / "config" / "student.json").read_text(encoding="utf-8"))
STATIONS = (
    ("kyiv-tpp", "Київська ТЕС", "thermal", 1200),
    ("dnipro-hpp", "Дніпровська ГЕС", "hydro", 800),
    ("solar-farm", "Сонячна ферма", "solar", 150),
    ("wind-farm", "Вітряна ферма", "wind", 200),
)


def generate_power_data(rng: random.Random, sequence: int, run_id: str, demo_alerts: bool = False) -> dict:
    station_id, name, kind, maximum = STATIONS[(sequence - 1) % len(STATIONS)]
    now = datetime.now(timezone.utc)
    if kind == "solar":
        # A simplified educational model uses Kyiv local time when available.
        try:
            from zoneinfo import ZoneInfo
            hour = now.astimezone(ZoneInfo("Europe/Kyiv")).hour
        except Exception:
            hour = (now.hour + 2) % 24
        factor = rng.uniform(0.7, 0.95) if 6 <= hour < 18 else 0.0
    elif kind == "wind":
        wind = rng.uniform(0, 15)
        factor = 0 if wind < 3 else min(1.0, (wind / 12) ** 2)
    else:
        factor = rng.uniform(0.75, 0.95)
    record = {
        "schema_version": 1, "student": CONFIG["surname"],
        "run_id": run_id, "sequence": sequence, "station_id": station_id,
        "station_name": name, "station_type": kind,
        "timestamp": now.isoformat(timespec="milliseconds"),
        "power_output_mw": round(maximum * factor, 2),
        "voltage_kv": round(rng.uniform(218, 222), 1),
        "frequency_hz": round(rng.uniform(49.9, 50.1), 2),
        "efficiency_percent": round(rng.uniform(82, 88), 1),
        "kafka_version": "3.7.1",
    }
    if demo_alerts and sequence % 5 == 0:
        record.update(voltage_kv=223.5, frequency_hz=50.3, efficiency_percent=72.0)
    return record


def analyze_power_data(data: dict) -> dict:
    if not isinstance(data, dict):
        raise ValueError("JSON object required")
    required = ("station_name", "station_type", "timestamp", "power_output_mw", "voltage_kv", "frequency_hz", "efficiency_percent")
    if any(key not in data for key in required):
        raise ValueError("Missing required telemetry fields")
    if data.get("schema_version", 1) != 1:
        raise ValueError("Unsupported schema_version")
    if not all(isinstance(data[key], str) and data[key] for key in required[:3]):
        raise ValueError("Invalid station name, type, or timestamp")
    for key in required[3:]:
        value = data[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"Invalid finite number: {key}")
    power, voltage, frequency, efficiency = (data[key] for key in required[3:])
    if power < 0 or voltage < 0 or frequency < 0 or not 0 <= efficiency <= 100:
        raise ValueError("Telemetry values outside physical ranges")
    power_status = "ВІДКЛЮЧЕНА" if power == 0 else "НИЗЬКА ПОТУЖНІСТЬ" if power < 100 else "ВИСОКА ПОТУЖНІСТЬ" if power > 1000 else "НОРМАЛЬНА ПОТУЖНІСТЬ"
    result = {
        "power_status": power_status,
        "voltage_status": "НОРМА" if 219 <= voltage <= 221 else "ВІДХИЛЕННЯ",
        "frequency_status": "НОРМА" if 49.9 <= frequency <= 50.1 else "КРИТИЧНО",
        "efficiency_status": "ВІДМІННО" if efficiency >= 85 else "ДОБРЕ" if efficiency >= 80 else "ПОГАНО",
        "warnings": [],
    }
    if power < 50:
        result["warnings"].append("Критично низька потужність; перевірити режим станції")
    if voltage < 218 or voltage > 222:
        result["warnings"].append("Напруга поза допустимими межами")
    if frequency < 49.8 or frequency > 50.2:
        result["warnings"].append("КРИТИЧНО: частота поза межами")
    if efficiency < 75:
        result["warnings"].append("Низький ККД; потрібне обслуговування")
    if data["station_type"] in ("solar", "wind") and power < 50:
        result["recommendation"] = "Перевірити освітленість або швидкість вітру; низька генерація може бути природною"
    return result


def configure_stdout() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")

