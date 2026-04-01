#!/usr/bin/env python3

import argparse
import json
import os
from collections import defaultdict
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np

import bench_lib
import bench_plot_lib as plot_lib


POLICY_ORDER = ["baseline", "baseline_mglru", "mru", "fifo", "s3fifo", "lhd", "lfu"]
POLICY_LABEL = {
    "baseline": "Default (Linux)",
    "baseline_mglru": "MGLRU (Linux)",
    "mru": "MRU (cache_ext)",
    "fifo": "FIFO (cache_ext)",
    "s3fifo": "S3-FIFO (cache_ext)",
    "lhd": "LHD (cache_ext)",
    "lfu": "LFU (cache_ext)",
}
POLICY_COLOR = {
    "baseline": "#4C78A8",
    "baseline_mglru": "#72B7B2",
    "mru": "#F58518",
    "fifo": "#E45756",
    "s3fifo": "#54A24B",
    "lhd": "#B279A2",
    "lfu": "#FF9DA6",
}


def parse_policy(config: Dict) -> str:
    if config["cgroup_name"] == bench_lib.DEFAULT_BASELINE_CGROUP:
        return "baseline_mglru" if config.get("mglru", False) else "baseline"

    policy_loader = config.get("policy_loader", "").lower()
    if "mru" in policy_loader:
        return "mru"
    if "fifo.out" in policy_loader and "s3fifo" not in policy_loader:
        return "fifo"
    if "s3fifo" in policy_loader:
        return "s3fifo"
    if "lhd" in policy_loader:
        return "lhd"
    if "sampling" in policy_loader:
        return "lfu"
    return "unknown"


def redis_name(config: Dict) -> str:
    return POLICY_LABEL.get(parse_policy(config), "<unknown>")


def load_redis_runs(path: str) -> List[bench_lib.BenchRun]:
    runs = bench_lib.parse_results_file(path, bench_lib.BenchResults)
    return [r for r in runs if r.config.get("name") == "redis_benchmark"]


def merge_runs(primary: List[bench_lib.BenchRun], extra: List[bench_lib.BenchRun]) -> List[bench_lib.BenchRun]:
    if not extra:
        return primary

    merged = list(primary)
    for r in extra:
        if not bench_lib.exists_config_in_results(merged, r.config):
            merged.append(r)
    return merged


def add_config_field(runs: List[bench_lib.BenchRun], key: str, value) -> None:
    for run in runs:
        run.config[key] = value


def add_config_field_if_missing(runs: List[bench_lib.BenchRun], key: str, value) -> None:
    for run in runs:
        run.config.setdefault(key, value)


def build_config_matches(runs: List[bench_lib.BenchRun]) -> Tuple[List[Dict], List[str], List[str]]:
    first_match_by_policy = {}
    for run in runs:
        policy = parse_policy(run.config)
        if policy == "unknown":
            continue
        if policy not in first_match_by_policy:
            match = {
                k: v
                for k, v in run.config.items()
                if k not in {"benchmark", "iteration"}
            }
            # Keep baseline and MGLRU baseline separated in partial matching.
            if run.config.get("cgroup_name") == bench_lib.DEFAULT_BASELINE_CGROUP:
                match["mglru"] = bool(run.config.get("mglru", False))
            first_match_by_policy[policy] = match

    selected_policies = [p for p in POLICY_ORDER if p in first_match_by_policy]
    config_matches = [first_match_by_policy[p] for p in selected_policies]
    colors = [POLICY_COLOR[p] for p in selected_policies]
    return config_matches, selected_policies, colors


def plot_absolute_figures(
    runs: List[bench_lib.BenchRun],
    config_matches: List[Dict],
    colors: List[str],
    benchmarks: List[str],
    output_dir: str,
    prefix: str,
):
    group_names = {"uniform": "Uniform", "zipfian": "Zipfian"}

    plot_lib.bench_plot_groupped_results(
        config_matches,
        runs,
        colors=colors,
        bench_types=benchmarks,
        bench_type_to_group=group_names,
        filename=os.path.join(output_dir, f"{prefix}_throughput.pdf"),
        y_label="Throughput (ops/sec)",
        result_select_fn=lambda r: r["throughput_avg"],
        name_func=redis_name,
        show_measurements=True,
        measurement_offset=300,
        bar_width=0.5,
        fontsize=11,
        label_fontsize=13,
        legend_fontsize=9,
    )

    plot_lib.bench_plot_groupped_results(
        config_matches,
        runs,
        colors=colors,
        bench_types=benchmarks,
        bench_type_to_group=group_names,
        filename=os.path.join(output_dir, f"{prefix}_latency_p99.pdf"),
        y_label="P99 Latency (ms)",
        result_select_fn=lambda r: r["latency_p99"] / 10**6,
        name_func=redis_name,
        show_measurements=False,
        bar_width=0.5,
        fontsize=11,
        label_fontsize=13,
        legend_fontsize=9,
    )


def summarize_runs(runs: List[bench_lib.BenchRun]) -> Dict[str, Dict[str, Dict[str, float]]]:
    grouped = defaultdict(lambda: defaultdict(lambda: {"throughput": [], "latency_p99_ms": []}))
    for run in runs:
        benchmark = run.config.get("benchmark")
        policy = parse_policy(run.config)
        if policy == "unknown":
            continue
        grouped[benchmark][policy]["throughput"].append(run.results.throughput_avg)
        grouped[benchmark][policy]["latency_p99_ms"].append(run.results.latency_p99 / 1e6)

    summary = {}
    for benchmark, benchmark_data in grouped.items():
        summary[benchmark] = {}
        for policy, metrics in benchmark_data.items():
            summary[benchmark][policy] = {
                "throughput_avg": float(np.mean(metrics["throughput"])),
                "latency_p99_ms_avg": float(np.mean(metrics["latency_p99_ms"])),
                "samples": len(metrics["throughput"]),
            }
    return summary


def plot_normalized_figure(
    summary: Dict[str, Dict[str, Dict[str, float]]],
    output_dir: str,
    prefix: str,
    benchmarks: List[str],
    selected_policies: List[str],
):
    valid_benchmarks = [b for b in benchmarks if b in summary and "baseline" in summary[b]]
    if not valid_benchmarks:
        raise ValueError("No benchmark has 'baseline' entries; cannot plot normalized figure")

    throughput_pct = {p: [] for p in selected_policies}
    latency_pct = {p: [] for p in selected_policies}

    for benchmark in valid_benchmarks:
        base_tp = summary[benchmark]["baseline"]["throughput_avg"]
        base_lat = summary[benchmark]["baseline"]["latency_p99_ms_avg"]
        for policy in selected_policies:
            metric = summary[benchmark].get(policy)
            if metric is None:
                throughput_pct[policy].append(np.nan)
                latency_pct[policy].append(np.nan)
                continue
            throughput_pct[policy].append(metric["throughput_avg"] / base_tp * 100.0)
            latency_pct[policy].append(metric["latency_p99_ms_avg"] / base_lat * 100.0)

    bench_names = {"uniform": "Uniform", "zipfian": "Zipfian"}
    x = np.arange(len(valid_benchmarks))
    width = 0.11 if len(selected_policies) >= 6 else 0.14

    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), sharey=False)

    def annotate_bar_values(ax, bars):
        for bar in bars:
            value = bar.get_height()
            if np.isnan(value):
                continue
            ax.annotate(
                f"{value:.1f}%",
                xy=(bar.get_x() + bar.get_width() / 2, value),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    for i, policy in enumerate(selected_policies):
        offset = (i - (len(selected_policies) - 1) / 2) * width
        throughput_bars = axes[0].bar(
            x + offset,
            throughput_pct[policy],
            width=width,
            label=POLICY_LABEL[policy],
            color=POLICY_COLOR[policy],
        )
        latency_bars = axes[1].bar(
            x + offset,
            latency_pct[policy],
            width=width,
            label=POLICY_LABEL[policy],
            color=POLICY_COLOR[policy],
        )
        annotate_bar_values(axes[0], throughput_bars)
        annotate_bar_values(axes[1], latency_bars)

    axes[0].axhline(100, color="black", linestyle="--", linewidth=1)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels([bench_names.get(b, b) for b in valid_benchmarks])
    axes[0].set_ylabel("Throughput (% of Default)")
    axes[0].set_title("Redis Throughput (Default = 100%)")

    axes[1].axhline(100, color="black", linestyle="--", linewidth=1)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([bench_names.get(b, b) for b in valid_benchmarks])
    axes[1].set_ylabel("P99 Latency (% of Default)")
    axes[1].set_title("Redis P99 Latency (Default = 100%)")

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.02), ncol=min(4, len(labels)))

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.savefig(
        os.path.join(output_dir, f"{prefix}_relative_to_default_pct.pdf"),
        metadata={"creationDate": None},
    )
    plt.clf()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize and plot Redis benchmark results")
    parser.add_argument(
        "--results-file",
        type=str,
        default="../results/redis_results.json",
        help="Main Redis results JSON",
    )
    parser.add_argument(
        "--mglru-results-file",
        type=str,
        default="../results/redis_results_mglru.json",
        help="Optional MGLRU Redis results JSON",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="../figures",
        help="Output directory for figures and summary JSON",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="redis",
        help="Output filename prefix",
    )
    parser.add_argument(
        "--benchmarks",
        type=str,
        default="uniform,zipfian",
        help="Comma-separated benchmark names to plot",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    benchmarks = [b.strip() for b in args.benchmarks.split(",") if b.strip()]
    if not benchmarks:
        raise ValueError("At least one benchmark must be specified")

    runs = load_redis_runs(args.results_file)
    add_config_field_if_missing(runs, "mglru", False)
    if os.path.exists(args.mglru_results_file):
        mglru_runs = load_redis_runs(args.mglru_results_file)
        add_config_field(mglru_runs, "mglru", True)
        runs = merge_runs(runs, mglru_runs)

    if not runs:
        raise ValueError("No redis_benchmark runs found in input files")

    config_matches, selected_policies, colors = build_config_matches(runs)
    if not config_matches:
        raise ValueError("No recognizable Redis policy entries found")

    plot_absolute_figures(
        runs,
        config_matches,
        colors,
        benchmarks,
        args.output_dir,
        args.prefix,
    )

    summary = summarize_runs(runs)
    plot_normalized_figure(
        summary,
        args.output_dir,
        args.prefix,
        benchmarks,
        selected_policies,
    )

    summary_path = os.path.join(args.output_dir, f"{args.prefix}_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print("Generated:")
    print(f"  - {os.path.join(args.output_dir, f'{args.prefix}_throughput.pdf')}")
    print(f"  - {os.path.join(args.output_dir, f'{args.prefix}_latency_p99.pdf')}")
    print(f"  - {os.path.join(args.output_dir, f'{args.prefix}_relative_to_default_pct.pdf')}")
    print(f"  - {summary_path}")


if __name__ == "__main__":
    main()
