"""Validate the assignment's data, experiment matrix and delivery evidence."""
import itertools
import json
import math
from common import ROOT, CONFIG, configure_stdout


def main():
    configure_stdout()
    evidence = ROOT / "evidence"
    results = json.loads((evidence / "results.json").read_text(encoding="utf-8"))
    records = [json.loads(line) for line in (ROOT / "data" / "hydro-1000.jsonl").read_text(encoding="utf-8").splitlines()]
    checks = {}
    checks["variant_13_maps_to_3B"] = (CONFIG["list_number"] - 1) % 10 + 1 == CONFIG["variant"] == 3 and "ABC"[(CONFIG["list_number"] - 1) // 10] == CONFIG["subvariant"] == "B"
    ranges = {"power_output": (10,200), "efficiency": (85,95), "temperature": (4,20), "voltage": (15000,16000), "current": (500,8000), "maintenance_hours": (500,6000), "water_flow": (500,3000), "water_level": (10,25)}
    checks["1000_records_15_devices_valid_ranges"] = len(records) == 1000 and len({row["device_id"] for row in records}) == 15 and all(all(low <= row[field] <= high for field,(low,high) in ranges.items()) for row in records)
    batch = [row for row in results if row["config"]["family"] == "batch"]
    expected = set(itertools.product((16384,65536,262144),(0,10,50),(0,1,2)))
    checks["all_27_batch_repetitions"] = {(row["config"]["batch_size"],row["config"]["linger_ms"],row["config"]["repeat"]) for row in batch} == expected
    checks["all_9_rate500_combinations"] = len([row for row in results if row["config"]["family"] == "rate500"]) == 9
    checks["five_codecs_three_repetitions"] = {(row["config"]["compression"],row["config"]["repeat"]) for row in results if row["config"]["family"] == "compression"} == set(itertools.product(("none","gzip","snappy","lz4","zstd"),range(3)))
    checks["variant_and_general_partition_counts"] = {row["config"]["partitions"] for row in results if row["config"]["family"] == "partition"} == {2,4,8,3,6,12}
    checks["four_partition_strategies"] = {row["config"]["strategy"] for row in results if row["config"]["family"] == "strategy"} == {"device","region","time","balanced"}
    checks["all_104_trials_complete_no_message_loss"] = len(results) == 104 and all(row["records"] == row["received"] == sum(row["partition_counts"]) and row["errors"] == 0 for row in results)
    checks["latency_and_resource_metrics_finite"] = all(all(math.isfinite(row[field]) for field in ("records_per_sec","jvm_cpu_ms","compression_rate_avg","producer_outgoing_bytes")) and row["ack_latency"]["p95_ms"] > 0 and row["e2e_latency"]["p95_ms"] > 0 for row in results)
    checks["analytics_batches_up_to_1MiB"] = any(row["config"]["batch_size"] == 1048576 for row in results if row["config"]["family"] == "analytics")
    checks["natural_rate_and_microbatch_measured"] = len([row for row in results if row["config"]["family"] == "natural"]) == 2 and len([row for row in results if row["config"]["family"] == "archive-window"]) == 1
    checks["retention_7_30_90_days_configured"] = {row["config"]["retention_ms"] for row in results if row["config"]["family"] == "retention"} == {7*86400000,30*86400000,90*86400000}
    load = json.loads((evidence / "load-scenarios.json").read_text(encoding="utf-8"))
    checks["exact_million_and_hundred_thousand_loads"] = [(row["records"],row["record_bytes"],row["target_rate"],row["received"]) for row in load] == [(1000000,256,10000,1000000),(100000,512,1000,100000)]
    serde = json.loads((evidence / "serialization.json").read_text(encoding="utf-8"))
    checks["three_serialization_round_trips_and_registry_kafka"] = len(serde["formats"]) == 3 and all(row["round_trip_records"] == 1000 for row in serde["formats"]) and serde["registry"]["kafka_round_trip"] == 10
    warehouse = json.loads((evidence / "warehouse.json").read_text(encoding="utf-8"))
    checks["warehouse_302400_rows_and_four_windows"] = warehouse["input_records"] == 302400 and len(warehouse["windows"]) == 4 and next(row for row in warehouse["windows"] if row["window_seconds"] == 300)["aggregate_rows"] == 30240
    (evidence / "validation.json").write_text(json.dumps({"checks": checks, "passed": sum(checks.values()), "total": len(checks)}, indent=2), encoding="utf-8")
    for name, passed in checks.items():
        print(("PASS " if passed else "FAIL ") + name)
    assert all(checks.values()), "Evidence validation failed"


if __name__ == "__main__":
    main()
