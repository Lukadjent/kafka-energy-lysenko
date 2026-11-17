"""Run the exact two record-count/size/rate scenarios specified on PDF page 3."""
import json
from pathlib import Path
import re
import sys
import time
from common import ROOT, CONFIG, configure_stdout
sys.path.insert(0, str(ROOT))
import lab


def main():
    configure_stdout()
    results = []
    evidence = ROOT / "evidence"
    for name, count, size, rate in (("smart-meters", 1000000, 256, 10000), ("wind-turbines", 100000, 512, 1000)):
        topic = "hydro-load-" + name + "-lysenko-" + str(int(time.time()))
        lab.run_tool("topics", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--create", "--topic", topic,
            "--partitions", "3", "--replication-factor", "1"], capture_output=True, timeout=60)
        arguments = ["--topic", topic, "--num-records", str(count), "--record-size", str(size), "--throughput", str(rate),
            "--producer-props", "bootstrap.servers=" + CONFIG["bootstrap_servers"], "compression.type=none",
            "client.id=load-producer-lysenko"]
        print(f"LOAD START {name}: {count} x {size} bytes @ {rate}/s", flush=True)
        producer = lab.run_tool("producer-perf", arguments, capture_output=True, timeout=180)
        assert f"{count} records sent" in producer.stdout
        transcript = "$ python lab.py producer-perf " + " ".join(arguments) + "\n" + producer.stdout + producer.stderr
        (evidence / f"load-{name}-producer.txt").write_text(transcript, encoding="utf-8")
        arguments = ["--bootstrap-server", CONFIG["bootstrap_servers"], "--topic", topic,
            "--messages", str(count), "--timeout", "60000", "--group", topic + "-consumer"]
        consumer = lab.run_tool("consumer-perf", arguments, capture_output=True, timeout=120)
        transcript = "$ python lab.py consumer-perf " + " ".join(arguments) + "\n" + consumer.stdout + consumer.stderr
        (evidence / f"load-{name}-consumer.txt").write_text(transcript, encoding="utf-8")
        row = next(line for line in consumer.stdout.splitlines() if re.match(r"\d{4}-\d{2}-\d{2}", line))
        received = int(re.split(r",\s+", row)[4])
        assert received == count
        results.append({"scenario": name, "records": count, "record_bytes": size, "target_rate": rate,
            "received": received, "topic": topic, "producer_summary": producer.stdout.splitlines()[-1], "consumer_csv": row})
        (evidence / "load-scenarios.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"LOAD PASS {name}: {received}/{count}", flush=True)
    # One official performance invocation with the actual JSON corpus, not random bytes.
    topic = "hydro-json-cli-lysenko-" + str(int(time.time()))
    lab.run_tool("topics", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--create", "--topic", topic,
        "--partitions", "3", "--replication-factor", "1"], capture_output=True, timeout=60)
    args = ["--topic", topic, "--num-records", "5000", "--payload-file", str(ROOT / "data" / "hydro-1000.jsonl"),
        "--throughput", "500", "--producer-props", "bootstrap.servers=" + CONFIG["bootstrap_servers"],
        "batch.size=65536", "linger.ms=10", "compression.type=zstd"]
    completed = lab.run_tool("producer-perf", args, capture_output=True, timeout=90)
    assert "5000 records sent" in completed.stdout
    (evidence / "official-json-performance.txt").write_text("$ python lab.py producer-perf " + " ".join(args) + "\n" + completed.stdout + completed.stderr, encoding="utf-8")


if __name__ == "__main__":
    main()
