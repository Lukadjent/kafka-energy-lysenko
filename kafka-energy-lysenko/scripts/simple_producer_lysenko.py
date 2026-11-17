"""JSON telemetry producer for Lysenko Luka, ТР-61МП."""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid

from common import CONFIG, ROOT, configure_stdout, generate_power_data
from kafka import KafkaProducer


def main() -> int:
    configure_stdout()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=0, help="0 = continuous")
    parser.add_argument("--interval", type=float, default=3)
    parser.add_argument("--seed", type=int, default=61)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--demo-alerts", action="store_true")
    args = parser.parse_args()
    if args.count < 0 or args.interval < 0:
        parser.error("count and interval must be nonnegative")
    run_id = args.run_id or uuid.uuid4().hex[:12]
    rng = random.Random(args.seed)
    sent = 0
    print(f"PROJECT={ROOT.name}\nPRODUCER=producer-{CONFIG['surname']} TOPIC={CONFIG['topic']} RUN={run_id}", flush=True)
    producer = KafkaProducer(
        bootstrap_servers=CONFIG["bootstrap_servers"], client_id=f"producer-{CONFIG['surname']}",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"),
        acks="all", retries=3, retry_backoff_ms=500, request_timeout_ms=30000,
        batch_size=16384, linger_ms=100, compression_type="gzip", buffer_memory=33554432,
        max_in_flight_requests_per_connection=1,
    )
    try:
        while args.count == 0 or sent < args.count:
            data = generate_power_data(rng, sent + 1, run_id, args.demo_alerts)
            key = f"{CONFIG['surname']}-{data['station_id']}"
            meta = producer.send(CONFIG["topic"], key=key, value=data).get(timeout=30)
            sent += 1
            print(f"SENT #{sent} {data['station_name']} {data['power_output_mw']} MW "
                  f"partition={meta.partition} offset={meta.offset} key={key}", flush=True)
            print(json.dumps(data, ensure_ascii=False), flush=True)
            if args.count == 0 or sent < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Producer interrupted; flushing acknowledged data", flush=True)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
    print(f"PRODUCER_SUMMARY sent={sent} run_id={run_id}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

