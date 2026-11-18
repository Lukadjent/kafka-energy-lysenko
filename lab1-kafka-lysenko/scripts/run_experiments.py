"""Run reproducible, real Kafka benchmarks and retain every raw result."""
from __future__ import annotations
import csv
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import random
import subprocess
import sys
import time

from common import ROOT, CONFIG, configure_stdout
sys.path.insert(0, str(ROOT))
import lab
import psutil
from kafka.admin import KafkaAdminClient, NewTopic

EVIDENCE = ROOT / "evidence"
RESULTS = EVIDENCE / "runs" / "lecture-review"


def process_snapshot(pid):
    try:
        process = psutil.Process(pid)
        cpu = process.cpu_times()
        io = process.io_counters()
        return {"cpu_s": cpu.user + cpu.system, "rss": process.memory_info().rss,
                "read_bytes": io.read_bytes, "write_bytes": io.write_bytes}
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def run_trial(admin, spec, tag, index, services):
    label = spec["label"]
    partitions = spec.get("partitions", 3)
    # Every repetition gets a fresh topic; native Windows topic deletion is avoided.
    topic = f"h3b-lysenko-{tag}-{index:03d}"
    retention = spec.get("retention_ms", 30 * 86400000)
    admin.create_topics([NewTopic(topic, partitions, 1, topic_configs={"retention.ms": str(retention)})])
    config = {"bootstrap": CONFIG["bootstrap_servers"], "topic": topic, "partitions": partitions,
              "records": spec.get("records", 50000), "warmup_records": 1000, "throughput": -1,
              "batch_size": 65536, "linger_ms": 10, "compression": "none", "strategy": "device",
              "payload_file": str(ROOT / "data" / "hydro-1000.jsonl"),
              "output_file": str(RESULTS / f"{index:03d}-{label}.json"),
              "log_root": str(ROOT / "data" / "kafka-logs"), **spec}
    config_file = RESULTS / f"{index:03d}-{label}.config.json"
    config_file.write_text(json.dumps(config, indent=2), encoding="utf-8")
    command = lab.java_command("EnergyBenchmark", [str(config_file)], "benchmark")
    before = {item["name"]: process_snapshot(item["pid"]) for item in services}
    peaks = {item["name"]: 0 for item in services}
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with (RESULTS / f"{index:03d}-{label}.txt").open("w", encoding="utf-8") as output:
        output.write("java EnergyBenchmark " + config_file.name + "\n")
        output.flush()
        process = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, creationflags=flags)
        deadline = time.monotonic() + 180 + (config["records"] / config["throughput"] if config["throughput"] > 0 else 0)
        while process.poll() is None:
            for item in services:
                sample = process_snapshot(item["pid"])
                if sample:
                    peaks[item["name"]] = max(peaks[item["name"]], sample["rss"])
            if time.monotonic() > deadline:
                process.terminate()
                raise TimeoutError(f"Trial {label} timed out")
            time.sleep(0.1)
        if process.returncode:
            raise RuntimeError(f"Trial {label} failed; see {output.name}")
    result_path = Path(config["output_file"])
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert result["records"] == result["received"] == config["records"] and result["errors"] == 0
    for key in ("compression_rate_avg", "producer_outgoing_bytes", "broker_log_bytes"):
        if not isinstance(result[key], (int, float)) or not math.isfinite(result[key]):
            raise RuntimeError(f"Metric {key} missing from Java result")
    resources = {}
    for item in services:
        after = process_snapshot(item["pid"])
        base = before[item["name"]]
        if base and after:
            resources[item["name"]] = {key: after[key] - base[key] for key in ("cpu_s", "read_bytes", "write_bytes")}
            resources[item["name"]]["peak_rss_bytes"] = peaks[item["name"]]
    result["resources_trial_including_warmup"] = resources
    result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    with (RESULTS / f"{index:03d}-{label}.txt").open("a", encoding="utf-8") as output:
        output.write("RESOURCE_RESULT " + json.dumps(resources) + "\n")
    return result


def plan():
    specs = []
    for batch in (16384, 65536, 262144):
        for linger in (0, 10, 50):
            for repeat in range(3):
                specs.append(dict(label=f"batch-{batch}-{linger}-r{repeat}", family="batch", batch_size=batch, linger_ms=linger, repeat=repeat))
    # Fixed offered load separates intentional waiting from a saturation test.
    for batch in (16384, 65536, 262144):
        for linger in (0, 10, 50):
            specs.append(dict(label=f"rate500-{batch}-{linger}", family="rate500", batch_size=batch, linger_ms=linger, records=5000, throughput=500))
    for codec in ("none", "gzip", "snappy", "lz4", "zstd"):
        for repeat in range(3):
            specs.append(dict(label=f"codec-{codec}-r{repeat}", family="compression", compression=codec, batch_size=262144, linger_ms=50, records=20000, repeat=repeat))
    # Variant-specific 2/4/8 are primary; general 3/6/12 are checked as supplements.
    for partitions in (2, 4, 8, 3, 6, 12):
        for repeat in range(3):
            specs.append(dict(label=f"part-{partitions}-r{repeat}", family="partition", partitions=partitions, strategy="turbine", repeat=repeat))
    for strategy in ("device", "region", "time", "balanced"):
        for repeat in range(3):
            specs.append(dict(label=f"strategy-{strategy}-r{repeat}", family="strategy", partitions=4, strategy=strategy, repeat=repeat))
    for batch, linger in ((524288, 100), (1048576, 100), (1048576, 300)):
        for repeat in range(3):
            specs.append(dict(label=f"analytics-{batch}-{linger}-r{repeat}", family="analytics", batch_size=batch, linger_ms=linger, compression="zstd", repeat=repeat))
    for memory in (8388608, 33554432, 67108864):
        specs.append(dict(label=f"buffer-{memory}", family="buffer", buffer_memory=memory))
    for inflight in (1, 5, 10):
        specs.append(dict(label=f"inflight-{inflight}", family="inflight", max_in_flight=inflight))
    for linger in (5, 100):
        specs.append(dict(label=f"linger-extra-{linger}", family="linger-extra", linger_ms=linger))
    for days in (7, 30, 90):
        specs.append(dict(label=f"retention-{days}d", family="retention", retention_ms=days * 86400000, compression="zstd", records=20000))
    # Verify the complete operational candidate, including the delivery settings
    # recommended in lecture 4, instead of combining unrelated table rows.
    for rate in (-1, 500):
        for repeat in range(3):
            specs.append(dict(label=f"recommended-{rate}-r{repeat}", family="recommended",
                partitions=4, strategy="device", batch_size=65536, linger_ms=10,
                compression="lz4", enable_idempotence=True, max_in_flight=5,
                records=5000 if rate == 500 else 50000, throughput=rate, repeat=repeat))
    random.Random(13).shuffle(specs)
    return specs


def main():
    configure_stdout()
    RESULTS.mkdir(parents=True, exist_ok=True)
    tag = datetime.now(timezone.utc).strftime("%m%d%H%M%S")
    services = json.loads(lab.STATE.read_text(encoding="utf-8"))
    admin = KafkaAdminClient(bootstrap_servers=CONFIG["bootstrap_servers"], client_id="admin-lab1-lysenko")
    specs = plan()
    (EVIDENCE / "experiment-plan.json").write_text(json.dumps(specs, indent=2), encoding="utf-8")
    # Check the complete measurement chain before spending time on the matrix.
    pilot = run_trial(admin, dict(label="pilot", family="pilot", records=1000), tag, 0, services)
    print(f"PILOT PASS: {pilot['received']}/1000, mean JSON={pilot['mean_payload_bytes']:.1f} bytes", flush=True)
    results = []
    for index, spec in enumerate(specs, 1):
        result = run_trial(admin, spec, tag, index, services)
        results.append(result)
        (EVIDENCE / "results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"[{index}/{len(specs)}] {spec['label']}: {result['records_per_sec']:.0f} rec/s; ack={result['ack_latency']['avg_ms']:.2f} ms; saved={100*(1-result['compression_rate_avg']):.1f}%", flush=True)
    admin.close()
    (EVIDENCE / "environment.json").write_text(json.dumps({"platform": sys.platform, "python": sys.version,
        "logical_cpu": psutil.cpu_count(), "physical_cpu": psutil.cpu_count(logical=False),
        "ram_bytes": psutil.virtual_memory().total, "kafka": "3.7.1", "zookeeper": "3.8.4",
        "variant": "3B", "finished_utc": datetime.now(timezone.utc).isoformat()}, indent=2), encoding="utf-8")
    print(f"COMPLETE {len(results)} real Kafka trials", flush=True)


if __name__ == "__main__":
    main()
