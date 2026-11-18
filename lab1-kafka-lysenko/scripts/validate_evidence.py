"""Validate the assignment's data, experiment matrix and delivery evidence."""
import itertools
import json
import math
from common import ROOT, CONFIG, configure_stdout
from kafka.partitioner.default import murmur2


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
    keyed_partitions = True
    for row in results:
        if row["config"]["family"] != "partition":
            continue
        expected_counts = [0] * row["config"]["partitions"]
        for index in range(row["records"]):
            key = records[index % len(records)]["turbine_type"].encode("utf-8")
            expected_counts[(murmur2(key) & 0x7fffffff) % len(expected_counts)] += 1
        keyed_partitions &= row["config"]["strategy"] == "turbine" and row["partition_counts"] == expected_counts
    checks["variant_key_matches_all_18_murmur2_distributions"] = keyed_partitions
    checks["four_partition_strategies"] = {row["config"]["strategy"] for row in results if row["config"]["family"] == "strategy"} == {"device","region","time","balanced"}
    checks["all_110_trials_complete_no_message_loss"] = len(results) == 110 and all(row["records"] == row["received"] == sum(row["partition_counts"]) and row["errors"] == 0 for row in results)
    stdout_matches = True
    for row in results:
        raw_path = ROOT / "evidence" / "runs" / "lecture-review" / (str(row["config"]["output_file"]).replace("\\", "/").rsplit("/", 1)[-1])
        lines = raw_path.with_suffix(".txt").read_text(encoding="utf-8").splitlines()
        measured = [json.loads(line.removeprefix("MEASUREMENT_RESULT ")) for line in lines if line.startswith("MEASUREMENT_RESULT ")]
        resources = [json.loads(line.removeprefix("RESOURCE_RESULT ")) for line in lines if line.startswith("RESOURCE_RESULT ")]
        expected_row = {key: value for key, value in row.items() if key != "resources_trial_including_warmup"}
        stdout_matches &= measured == [expected_row] and resources == [row["resources_trial_including_warmup"]]
    checks["all_reported_kafka_metrics_match_original_stdout"] = stdout_matches
    recommended = [row for row in results if row["config"]["family"] == "recommended"]
    checks["full_recommended_configuration_six_real_trials"] = len(recommended) == 6 and all(
        row["config"]["enable_idempotence"] and row["config"]["max_in_flight"] == 5 and
        row["config"]["partitions"] == 4 and row["config"]["compression"] == "lz4" and
        row["config"]["batch_size"] == 65536 and row["config"]["linger_ms"] == 10
        for row in recommended)
    checks["latency_and_resource_metrics_finite"] = all(all(math.isfinite(row[field]) for field in ("records_per_sec","jvm_cpu_ms","compression_rate_avg","producer_outgoing_bytes")) and row["ack_latency"]["p95_ms"] > 0 and row["e2e_latency"]["p95_ms"] > 0 for row in results)
    checks["analytics_batches_up_to_1MiB"] = any(row["config"]["batch_size"] == 1048576 for row in results if row["config"]["family"] == "analytics")
    checks["natural_rate_and_microbatch_measured"] = len([row for row in results if row["config"]["family"] == "natural"]) == 2 and len([row for row in results if row["config"]["family"] == "archive-window"]) == 1
    checks["retention_7_30_90_days_configured"] = {row["config"]["retention_ms"] for row in results if row["config"]["family"] == "retention"} == {7*86400000,30*86400000,90*86400000}
    load = json.loads((evidence / "load-scenarios.json").read_text(encoding="utf-8"))
    checks["exact_million_and_hundred_thousand_loads"] = [(row["records"],row["record_bytes"],row["target_rate"],row["received"]) for row in load] == [(1000000,256,10000,1000000),(100000,512,1000,100000)]
    serde = json.loads((evidence / "serialization.json").read_text(encoding="utf-8"))
    checks["three_serialization_round_trips_and_registry_kafka"] = len(serde["formats"]) == 3 and all(row["round_trip_records"] == 1000 for row in serde["formats"]) and serde["registry"]["kafka_round_trip"] == 10
    compatibility = serde["registry"]["compatibility"]
    checks["schema_backward_contract_and_incompatible_rejection"] = compatibility["new_reader_reads_old_payload_with_default"] and compatibility["incompatible_type_rejected_http"] == 409 and compatibility["optional_field_with_default_accepted_id"] != serde["registry"]["schema_id"]
    warehouse = json.loads((evidence / "warehouse.json").read_text(encoding="utf-8"))
    checks["warehouse_302400_rows_and_four_windows"] = warehouse["input_records"] == 302400 and len(warehouse["windows"]) == 4 and next(row for row in warehouse["windows"] if row["window_seconds"] == 300)["aggregate_rows"] == 30240
    contract = warehouse["rollup_contract"]
    checks["weighted_rollups_reconcile_all_105_daily_groups"] = contract["all_daily_count_sum_min_max_match"] and contract["daily_groups_verified_against_raw"] == 105
    auxiliary_stdout = True
    for filename, marker, expected_data in (("serialization-stdout.txt", "SERIALIZATION_RESULT ", serde), ("warehouse-stdout.txt", "WAREHOUSE_RESULT ", warehouse)):
        numeric = [json.loads(line.removeprefix(marker)) for line in (evidence / filename).read_text(encoding="utf-8").splitlines() if line.startswith(marker)]
        auxiliary_stdout &= numeric == [expected_data]
    checks["auxiliary_metrics_match_original_stdout"] = auxiliary_stdout
    (evidence / "validation.json").write_text(json.dumps({"checks": checks, "passed": sum(checks.values()), "total": len(checks)}, indent=2), encoding="utf-8")
    for name, passed in checks.items():
        print(("PASS " if passed else "FAIL ") + name)
    assert all(checks.values()), "Evidence validation failed"


if __name__ == "__main__":
    main()
