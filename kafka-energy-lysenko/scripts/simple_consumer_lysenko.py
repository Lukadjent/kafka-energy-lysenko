"""Telemetry consumer with durable evidence and explicit offset commits."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from common import CONFIG, ROOT, analyze_power_data, configure_stdout
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata


def main() -> int:
    configure_stdout()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, default=0, help="valid JSON telemetry records; 0 = continuous")
    parser.add_argument("--idle-timeout", type=float, default=0, help="seconds; 0 = wait indefinitely")
    parser.add_argument("--group", default=CONFIG["consumer_group"])
    parser.add_argument("--offset-reset", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--run-id", default=None, help="only count telemetry from this verification run")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "processed.jsonl")
    args = parser.parse_args()
    if args.max_messages < 0 or args.idle_timeout < 0:
        parser.error("limits must be nonnegative")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    counts = {"processed": 0, "text": 0, "rejected": 0, "warnings": 0, "ignored": 0}
    print(f"PROJECT={ROOT.name}\nCONSUMER=consumer-{CONFIG['surname']} TOPIC={CONFIG['topic']} GROUP={args.group}", flush=True)
    consumer = KafkaConsumer(
        CONFIG["topic"], bootstrap_servers=CONFIG["bootstrap_servers"],
        client_id=f"consumer-{CONFIG['surname']}", group_id=args.group,
        auto_offset_reset=args.offset_reset, enable_auto_commit=False,
        session_timeout_ms=30000, heartbeat_interval_ms=10000,
        max_poll_records=500, fetch_min_bytes=1, fetch_max_wait_ms=1000,
    )
    last_record = time.monotonic()
    complete = False
    try:
        with args.output.open("a", encoding="utf-8") as output:
            while not complete:
                batches = consumer.poll(timeout_ms=1000)
                if not batches and args.idle_timeout and time.monotonic() - last_record >= args.idle_timeout:
                    break
                for partition, records in batches.items():
                    for message in records:
                        last_record = time.monotonic()
                        envelope = {"topic": message.topic, "partition": message.partition, "offset": message.offset}
                        try:
                            text = message.value.decode("utf-8")
                            try:
                                data = json.loads(text)
                            except json.JSONDecodeError:
                                if text.lstrip().startswith(("{", "[")):
                                    raise ValueError("Malformed JSON telemetry")
                                counts["text"] += 1
                                envelope.update(kind="text", text=text)
                                print(f"TEXT partition={message.partition} offset={message.offset}: {text}", flush=True)
                            else:
                                analysis = analyze_power_data(data)
                                if args.run_id and data.get("run_id") != args.run_id:
                                    counts["ignored"] += 1
                                    envelope.update(kind="historical", data=data)
                                else:
                                    counts["processed"] += 1
                                    counts["warnings"] += len(analysis["warnings"])
                                    envelope.update(kind="telemetry", data=data, analysis=analysis)
                                    print(f"RECEIVED #{counts['processed']} {data['station_name']} "
                                          f"partition={message.partition} offset={message.offset} "
                                          f"power={data['power_output_mw']} MW "
                                          f"voltage={data['voltage_kv']} kV frequency={data['frequency_hz']} Hz "
                                          f"efficiency={data['efficiency_percent']}%", flush=True)
                                    print(json.dumps(analysis, ensure_ascii=False), flush=True)
                        except (UnicodeDecodeError, ValueError, TypeError) as error:
                            counts["rejected"] += 1
                            envelope.update(kind="rejected", error=str(error), raw_hex=message.value.hex())
                            print(f"REJECTED partition={message.partition} offset={message.offset}: {error}", flush=True)
                        # A processing or write error propagates; do not commit unhandled data.
                        output.write(json.dumps(envelope, ensure_ascii=False) + "\n")
                        output.flush()
                        consumer.commit({TopicPartition(message.topic, message.partition): OffsetAndMetadata(message.offset + 1, "", -1)})
                        if args.max_messages and counts["processed"] >= args.max_messages:
                            complete = True
                            break
                    if complete:
                        break
    except KeyboardInterrupt:
        print("Consumer interrupted; processed offsets already committed", flush=True)
    finally:
        consumer.close()
    print("CONSUMER_SUMMARY " + json.dumps(counts), flush=True)
    if args.max_messages and counts["processed"] < args.max_messages:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
