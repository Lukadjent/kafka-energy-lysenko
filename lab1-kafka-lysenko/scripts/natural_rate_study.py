"""Check whether saturation-test compression transfers to the actual 0.5 rec/s rate."""
import json
import sys
from datetime import datetime, timezone
from common import ROOT, CONFIG, configure_stdout
from run_experiments import run_trial, RESULTS
from kafka.admin import KafkaAdminClient
import lab


def main():
    configure_stdout()
    admin = KafkaAdminClient(bootstrap_servers=CONFIG["bootstrap_servers"])
    services = json.loads(lab.STATE.read_text(encoding="utf-8"))
    results_path = ROOT / "evidence" / "results.json"
    results = json.loads(results_path.read_text(encoding="utf-8"))
    tag = datetime.now(timezone.utc).strftime("%m%d%H%M%S")
    trials = [] if "--archive-only" in sys.argv else [
        {"label": f"natural-{codec}", "family": "natural", "records": 30,
         "warmup_records": 0, "throughput": 0.5, "batch_size": 262144, "linger_ms": 50, "compression": codec}
        for codec in ("none", "zstd")]
    trials.append({"label": "archive-window-300", "family": "archive-window", "records": 150,
        "warmup_records": 0, "throughput": -1, "batch_size": 524288, "linger_ms": 100,
        "compression": "zstd", "strategy": "turbine"})
    for index, spec in enumerate(trials, len(results) + 1):
        print(f"NATURAL/ARCHIVE START {spec['label']}", flush=True)
        result = run_trial(admin, spec, tag, index, services)
        results.append(result)
        results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"NATURAL/ARCHIVE PASS {spec['label']}: compression savings {100*(1-result['compression_rate_avg']):.1f}%", flush=True)
    admin.close()


if __name__ == "__main__":
    main()
