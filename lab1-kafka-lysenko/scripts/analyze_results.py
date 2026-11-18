"""Aggregate raw measurements without dropping outliers; create standalone figures."""
import csv
from collections import defaultdict
import json
import math
import statistics
from common import ROOT, configure_stdout
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

EVIDENCE = ROOT / "evidence"
FIGURES = EVIDENCE / "figures"


def group_results(results, family, keys):
    groups = defaultdict(list)
    for result in results:
        if result["config"]["family"] == family:
            groups[tuple(result["config"][key] for key in keys)].append(result)
    output = []
    for key, rows in sorted(groups.items()):
        row = dict(zip(keys, key))
        row.update(repeats=len(rows), rec_s=statistics.median(item["records_per_sec"] for item in rows),
            rec_s_min=min(item["records_per_sec"] for item in rows), rec_s_max=max(item["records_per_sec"] for item in rows),
            ack_ms=statistics.median(item["ack_latency"]["avg_ms"] for item in rows),
            ack_p95_ms=statistics.median(item["ack_latency"]["p95_ms"] for item in rows),
            e2e_p95_ms=statistics.median(item["e2e_latency"]["p95_ms"] for item in rows),
            savings_pct=100 * (1 - statistics.median(item["compression_rate_avg"] for item in rows)),
            disk_bytes_per_record=statistics.median(item["broker_log_bytes"] / item["records"] for item in rows),
            network_bytes_per_record=statistics.median(item["producer_outgoing_bytes"] / item["records"] for item in rows),
            client_cpu_ms_per_1000=statistics.median(item["jvm_cpu_ms"] / item["records"] * 1000 for item in rows),
            active_partitions=statistics.median(sum(value > 0 for value in item["partition_counts"]) for item in rows),
            distribution=rows[0]["partition_counts"],
            cv_pct=statistics.median(statistics.pstdev(item["partition_counts"]) / statistics.mean(item["partition_counts"]) * 100 for item in rows),
            broker_peak_rss_mib=max(item["resources_trial_including_warmup"].get("kafka", {}).get("peak_rss_bytes", 0) for item in rows) / 2**20,
            broker_cpu_s=statistics.median(item["resources_trial_including_warmup"].get("kafka", {}).get("cpu_s", 0) for item in rows),
            broker_io_write_mib=statistics.median(item["resources_trial_including_warmup"].get("kafka", {}).get("write_bytes", 0) for item in rows) / 2**20)
        output.append(row)
    return output


def main():
    configure_stdout()
    results = json.loads((EVIDENCE / "results.json").read_text(encoding="utf-8"))
    assert len(results) == 110, len(results)
    assert all(item["received"] == item["records"] and not item["errors"] for item in results)
    FIGURES.mkdir(exist_ok=True)
    families = {"batch": ("batch_size", "linger_ms"), "rate500": ("batch_size", "linger_ms"),
        "compression": ("compression",), "partition": ("partitions",), "strategy": ("strategy",),
        "analytics": ("batch_size", "linger_ms"), "buffer": ("buffer_memory",), "inflight": ("max_in_flight",),
        "linger-extra": ("linger_ms",), "retention": ("retention_ms",), "natural": ("compression",), "archive-window": ("compression",), "recommended": ("throughput",)}
    summary = {family: group_results(results, family, keys) for family, keys in families.items()}
    none = next(item for item in summary["compression"] if item["compression"] == "none")
    for row in summary["compression"]:
        row["disk_savings_vs_none_pct"] = 100 * (1 - row["disk_bytes_per_record"] / none["disk_bytes_per_record"])
        row["cpu_overhead_vs_none_pct"] = 100 * (row["client_cpu_ms_per_1000"] / none["client_cpu_ms_per_1000"] - 1)
    base = next(row["rec_s"] for row in summary["partition"] if row["partitions"] == 2)
    for row in summary["partition"]:
        row["scaling_vs_2"] = row["rec_s"] / base
    natural_none = next(row for row in summary["natural"] if row["compression"] == "none")
    natural_zstd = next(row for row in summary["natural"] if row["compression"] == "zstd")
    archive = summary["archive-window"][0]
    summary["storage"] = []
    for days in (7, 30, 90, 365):
        count = 15 / 30 * 86400 * days
        summary["storage"].append({"days": days, "records": int(count),
            "json_mib": count * 314.373 / 2**20,
            "natural_none_log_mib": count * natural_none["disk_bytes_per_record"] / 2**20,
            "natural_zstd_log_mib": count * natural_zstd["disk_bytes_per_record"] / 2**20,
            "archive_zstd_log_mib": count * archive["disk_bytes_per_record"] / 2**20,
            "zstd_rf3_plus20pct_mib": count * natural_zstd["disk_bytes_per_record"] * 3 * 1.2 / 2**20})
    summary["trial_count"] = len(results)
    summary["total_measured_records"] = sum(item["records"] for item in results)
    (EVIDENCE / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    with (EVIDENCE / "results-flat.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["family", "label", "records", "partitions", "batch_bytes", "linger_ms", "compression", "records_per_sec", "ack_avg_ms", "ack_p95_ms", "e2e_p95_ms", "compression_savings_pct", "log_bytes", "client_cpu_ms"])
        for item in results:
            c = item["config"]
            writer.writerow([c["family"], c["label"], item["records"], c["partitions"], c["batch_size"], c["linger_ms"], c["compression"], item["records_per_sec"], item["ack_latency"]["avg_ms"], item["ack_latency"]["p95_ms"], item["e2e_latency"]["p95_ms"], 100*(1-item["compression_rate_avg"]), item["broker_log_bytes"], item["jvm_cpu_ms"]])

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for axis, family, field, title in ((axes[0], "batch", "rec_s", "Медіана throughput, тис. записів/с"), (axes[1], "rate500", "ack_ms", "Ack latency за 500 записів/с, мс")):
        matrix = np.array([[next(row[field] for row in summary[family] if row["batch_size"] == batch and row["linger_ms"] == linger) for linger in (0,10,50)] for batch in (16384,65536,262144)])
        if field == "rec_s": matrix /= 1000
        axis.imshow(matrix, cmap="Blues", aspect="auto")
        for i in range(3):
            for j in range(3): axis.text(j, i, f"{matrix[i,j]:.1f}", ha="center", va="center", color="white" if matrix[i,j] > matrix.max()*.7 else "black")
        axis.set(xticks=range(3), xticklabels=["0","10","50"], yticks=range(3), yticklabels=["16","64","256"], xlabel="linger.ms", ylabel="batch.size, КіБ", title=title)
    fig.tight_layout()
    fig.savefig(FIGURES / "batch.png", dpi=220)
    plt.close(fig)

    codecs = sorted(summary["compression"], key=lambda row: ("none","gzip","snappy","lz4","zstd").index(row["compression"]))
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].bar([row["compression"] for row in codecs], [row["disk_savings_vs_none_pct"] for row in codecs], color="#3977a8")
    axes[0].set(ylabel="Економія .log проти none, %", ylim=(0,100), title="Однакові JSON і 20 000 записів")
    axes[1].bar([row["compression"] for row in codecs], [row["client_cpu_ms_per_1000"] for row in codecs], color="#3c8b77")
    axes[1].set(ylabel="CPU клієнтської JVM, мс / 1000 записів", title="Producer і consumer разом")
    fig.tight_layout()
    fig.savefig(FIGURES / "compression.png", dpi=220)
    plt.close(fig)

    part = summary["partition"]
    fig, axis = plt.subplots(figsize=(8.5, 3.7))
    x = np.arange(len(part))
    y = [row["rec_s"] / 1000 for row in part]
    errors = [[(row["rec_s"] - row["rec_s_min"]) / 1000 for row in part], [(row["rec_s_max"] - row["rec_s"]) / 1000 for row in part]]
    axis.bar(x, y, color="#3977a8", yerr=errors, capsize=4)
    axis.set(xticks=x, xticklabels=[str(row["partitions"]) for row in part], xlabel="Кількість партицій", ylabel="тис. записів/с", title="Ключ turbine_type: медіана та min-max трьох прогонів")
    for i,row in enumerate(part): axis.text(i, 10, f"активних: {int(row['active_partitions'])}", rotation=90, va="bottom", ha="center", color="white")
    fig.tight_layout()
    fig.savefig(FIGURES / "partitions.png", dpi=220)
    plt.close(fig)

    history = list(csv.DictReader((EVIDENCE / "history-hourly.csv").open(encoding="utf-8")))
    fig, axes = plt.subplots(2, 1, figsize=(9, 5.2), sharex=True)
    for turbine in ("kaplan", "francis", "pelton"):
        rows = [row for row in history if row["turbine_type"] == turbine]
        axes[0].plot(range(168), [float(row["mean_water_flow_m3s"]) for row in rows], label=turbine)
        axes[1].plot(range(168), [float(row["mean_water_level_m"]) for row in rows], label=turbine)
    axes[0].set(ylabel="water_flow, м³/с", title="Синтетична історія семи діб: погодинні середні")
    axes[0].legend(ncol=3)
    axes[1].set(ylabel="water_level, м", xlabel="Година від початку синтетичного періоду")
    fig.tight_layout()
    fig.savefig(FIGURES / "history.png", dpi=220)
    plt.close(fig)
    print(f"ANALYSIS PASS: {len(results)} trials, {summary['total_measured_records']} measured JSON messages", flush=True)


if __name__ == "__main__":
    main()
