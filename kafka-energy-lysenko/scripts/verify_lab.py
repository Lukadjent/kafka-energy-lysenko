"""Run real CLI/Python round trips, offset checks and the 10,000-message benchmark."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from common import CONFIG, ROOT, configure_stdout

sys.path.insert(0, str(ROOT))
import lab
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.partitioner.default import murmur2

EVIDENCE = ROOT / "evidence"
BOOTSTRAP = CONFIG["bootstrap_servers"]


def tool(name: str, args: list[str], filename: str, input_text: str | None = None) -> str:
    result = lab.run_tool(name, args, input=input_text, capture_output=True, timeout=90)
    transcript = f"PROJECT={ROOT.name}\nSTUDENT={CONFIG['student']} GROUP={CONFIG['group']}\n"
    transcript += "$ python lab.py " + name + " " + " ".join(args) + "\n"
    if input_text:
        transcript += "STDIN (sent to actual Kafka console producer):\n" + input_text + "\n"
    transcript += result.stdout + result.stderr
    (EVIDENCE / filename).write_text(transcript, encoding="utf-8")
    return result.stdout


def python_script(name: str, args: list[str], filename: str) -> str:
    command = [sys.executable, "-X", "utf8", str(ROOT / "scripts" / name), *args]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, encoding="utf-8", errors="replace", timeout=90)
    transcript = "$ python scripts/" + name + " " + " ".join(args) + "\n" + result.stdout + result.stderr
    (EVIDENCE / filename).write_text(transcript, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(f"{name} failed: {transcript}")
    return result.stdout


def main() -> None:
    configure_stdout()
    EVIDENCE.mkdir(exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    print(f"VERIFY run={run_id} project={ROOT.name}", flush=True)
    group = f"energy-monitor-lysenko-verify-{run_id}"
    summary = {"run_id": run_id, "timestamp_utc": datetime.now(timezone.utc).isoformat(),
               "student": CONFIG["student"], "group": CONFIG["group"], "topic": CONFIG["topic"],
               "consumer_group": group, "checks": {}}
    create = subprocess.run([sys.executable, "-X", "utf8", str(ROOT / "lab.py"), "create-topics"],
                            capture_output=True, encoding="utf-8", errors="replace", timeout=90, check=True)
    (EVIDENCE / "01-topics.txt").write_text(f"PROJECT={ROOT.name}\n$ python lab.py create-topics\n" + create.stdout + create.stderr, encoding="utf-8")
    print("PASS five required topics created", flush=True)

    inspect = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None, client_id="verify-lysenko")
    partitions = sorted(inspect.partitions_for_topic(CONFIG["topic"]))
    assert partitions == [0, 1, 2], partitions
    topic_partitions = [TopicPartition(CONFIG["topic"], p) for p in partitions]
    baseline = inspect.end_offsets(topic_partitions)
    inspect.close()
    summary["checks"]["main_topic_three_partitions"] = True

    # Use a key that the Java default partitioner maps to partition 0.
    key = next(f"lysenko-{i}" for i in range(100) if (murmur2(f"lysenko-{i}".encode()) & 0x7fffffff) % 3 == 0)
    console_texts = [f"Lysenko {run_id}: Київська ТЕС: 1200 МВт",
                     f"Lysenko {run_id}: Дніпровська ГЕС: 800 МВт",
                     f"Lysenko {run_id}: Сонячна ферма: 150 МВт"]
    console_input = "\n".join(key + "|" + text for text in console_texts) + "\n"
    tool("console-producer", ["--bootstrap-server", BOOTSTRAP, "--topic", CONFIG["topic"],
         "--property", "parse.key=true", "--property", "key.separator=|",
         "--producer-property", "client.id=console-producer-lysenko", "--producer-property", "acks=all"],
         "02-console-producer.txt", console_input)
    console_output = tool("console-consumer", ["--bootstrap-server", BOOTSTRAP, "--topic", CONFIG["topic"],
         "--partition", "0", "--offset", str(baseline[topic_partitions[0]]),
         "--max-messages", "3", "--timeout-ms", "15000",
         "--consumer-property", "client.id=console-consumer-lysenko"], "03-console-consumer.txt")
    assert console_output.splitlines() == console_texts, console_output
    summary["checks"]["console_round_trip_three_messages"] = True
    print("PASS actual Java console producer -> consumer: 3/3 messages", flush=True)

    # A non-JSON console record and malformed JSON must not crash the Python consumer.
    raw_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, client_id="validation-producer-lysenko", acks="all")
    raw_producer.send(CONFIG["topic"], key=key.encode(), value=b'{bad json').get(timeout=30)
    raw_producer.close()
    output_path = EVIDENCE / f"processed-{run_id}.jsonl"
    consumer_args = ["--group", group, "--run-id", run_id, "--max-messages", "20", "--idle-timeout", "20", "--output", str(output_path)]
    command = [sys.executable, "-X", "utf8", str(ROOT / "scripts" / "simple_consumer_lysenko.py"), *consumer_args]
    with (EVIDENCE / "05-python-consumer.txt").open("w", encoding="utf-8") as out:
        out.write(f"$ python scripts/simple_consumer_lysenko.py --group {group} --run-id {run_id} --max-messages 20 --idle-timeout 20\n")
        out.flush()
        flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        consumer = subprocess.Popen(command, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT, creationflags=flags)
        try:
            python_script("simple_producer_lysenko.py", ["--count", "20", "--interval", "0.02", "--run-id", run_id, "--demo-alerts"], "04-python-producer.txt")
            code = consumer.wait(timeout=90)
            if code:
                raise RuntimeError(f"Python consumer exited {code}; see evidence/05-python-consumer.txt")
        finally:
            if consumer.poll() is None:
                consumer.terminate()
                consumer.wait(timeout=10)
    records = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    telemetry = [record for record in records if record["kind"] == "telemetry"]
    assert len(telemetry) == 20
    assert sorted(record["data"]["sequence"] for record in telemetry) == list(range(1, 21))
    assert all(record["data"]["run_id"] == run_id for record in telemetry)
    assert set(console_texts).issubset({record.get("text") for record in records if record["kind"] == "text"})
    assert any(record["kind"] == "rejected" and record.get("raw_hex") == b'{bad json'.hex() for record in records)
    for station in {record["data"]["station_id"] for record in telemetry}:
        station_records = [record for record in telemetry if record["data"]["station_id"] == station]
        assert len({record["partition"] for record in station_records}) == 1
        sequences = [record["data"]["sequence"] for record in station_records]
        assert sequences == sorted(sequences)
    critical = [record for record in telemetry if record["data"]["sequence"] % 5 == 0]
    assert len(critical) == 4 and all(len(record["analysis"]["warnings"]) >= 3 for record in critical)
    summary["telemetry_received"] = len(telemetry)
    summary["telemetry_partitions_used"] = sorted({record["partition"] for record in telemetry})
    summary["warnings"] = sum(len(record["analysis"]["warnings"]) for record in telemetry)
    for check in ("python_round_trip_twenty_records", "per_station_order_preserved", "mixed_text_handled", "malformed_json_rejected", "four_critical_events_detected"):
        summary["checks"][check] = True
    print("PASS Python producer -> consumer: 20/20; station order, text, malformed JSON and alerts verified", flush=True)

    inspect = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=group, client_id="offset-check-lysenko")
    ends = inspect.end_offsets(topic_partitions)
    offsets = {tp.partition: {"committed": inspect.committed(tp), "end": ends[tp]} for tp in topic_partitions}
    inspect.close()
    assert all((row["committed"] or 0) == row["end"] for row in offsets.values()), offsets
    summary["offsets"] = offsets
    tool("groups", ["--bootstrap-server", BOOTSTRAP, "--describe", "--group", group], "06-consumer-group.txt")
    resumed = python_script("simple_consumer_lysenko.py", ["--group", group, "--idle-timeout", "4", "--output", str(EVIDENCE / f"resume-{run_id}.jsonl")], "07-consumer-resume.txt")
    resume_counts = json.loads(re.search(r"CONSUMER_SUMMARY (.+)", resumed).group(1))
    assert all(value == 0 for value in resume_counts.values()), resume_counts
    summary["checks"]["committed_offsets_equal_end_and_resume_has_no_replay"] = True
    print("PASS committed offsets = end offsets; same group resumes without replay", flush=True)

    # Topic lifecycle on a new topic belonging only to this verification run.
    temporary = f"topic-operations-lysenko-{run_id.lower()}"
    lifecycle = []
    for args in (["--create", "--topic", temporary, "--partitions", "2", "--replication-factor", "1"],
                 ["--alter", "--topic", temporary, "--partitions", "4"],
                 ["--describe", "--topic", temporary]):
        result = lab.run_tool("topics", ["--bootstrap-server", BOOTSTRAP, *args], capture_output=True, timeout=60)
        lifecycle.append("$ python lab.py topics " + " ".join(args) + "\n" + result.stdout + result.stderr)
    assert "PartitionCount: 4" in "\n".join(lifecycle)
    (EVIDENCE / "08-topic-operations.txt").write_text("\n".join(lifecycle), encoding="utf-8")
    # Native Windows Kafka 3.7.1 can fail when renaming memory-mapped log files
    # during deletion. Retain this test topic; deleting it is not required for
    # the telemetry round trip, and would take down this single-broker lab.
    summary["checks"]["temporary_topic_created_and_expanded"] = True

    benchmark_topic = f"benchmark-lysenko-{run_id.lower()}"
    lab.run_tool("topics", ["--bootstrap-server", BOOTSTRAP, "--create", "--topic", benchmark_topic,
                           "--partitions", "3", "--replication-factor", "1"], capture_output=True, timeout=60)
    perf_producer = tool("producer-perf", ["--topic", benchmark_topic, "--num-records", "10000", "--record-size", "1024",
         "--throughput", "1000", "--producer-props", f"bootstrap.servers={BOOTSTRAP}", "compression.type=gzip", "client.id=perf-producer-lysenko"], "09-producer-performance.txt")
    assert "10000 records sent" in perf_producer
    perf_consumer = tool("consumer-perf", ["--bootstrap-server", BOOTSTRAP, "--topic", benchmark_topic,
         "--messages", "10000", "--timeout", "30000", "--group", f"perf-consumer-lysenko-{run_id}"], "10-consumer-performance.txt")
    csv_rows = [line for line in perf_consumer.splitlines() if re.match(r"\d{4}-\d{2}-\d{2}", line)]
    assert csv_rows, perf_consumer
    consumed = int(re.split(r",\s+", csv_rows[-1])[4].strip())
    assert consumed == 10000, consumed
    summary["benchmark"] = {"topic": benchmark_topic, "records": 10000, "record_bytes": 1024,
                            "throughput_limit": 1000, "compression": "gzip", "consumed": consumed,
                            "producer_result": perf_producer.splitlines()[-1], "consumer_csv": csv_rows[-1]}
    summary["checks"]["benchmark_ten_thousand_records_sent_and_received"] = True
    print("PASS benchmark: 10,000 x 1,024-byte records sent and received", flush=True)

    tool("api-versions", ["--bootstrap-server", BOOTSTRAP], "11-broker-api.txt")
    tool("log-dirs", ["--bootstrap-server", BOOTSTRAP, "--describe"], "12-log-directories.txt")
    tool("configs", ["--bootstrap-server", BOOTSTRAP, "--entity-type", "brokers", "--entity-name", "0", "--describe"], "13-broker-config.txt")
    tool("configs", ["--bootstrap-server", BOOTSTRAP, "--entity-type", "topics", "--entity-name", CONFIG["topic"], "--describe"], "14-topic-config.txt")
    summary["checks"]["admin_commands_completed"] = True
    summary["finished_utc"] = datetime.now(timezone.utc).isoformat()
    (EVIDENCE / "verification.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"VERIFIED {len(summary['checks'])} integration checks; evidence/verification.json", flush=True)


if __name__ == "__main__":
    main()
