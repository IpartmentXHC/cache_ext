import argparse
import logging
import os
from contextlib import suppress
from time import sleep
from typing import Dict, List

from bench_lib import *


log = logging.getLogger(__name__)
GiB = 2**30
CLEANUP_TASKS = []


class RedisBenchmark(BenchmarkFramework):
    def __init__(self, benchresults_cls=BenchResults, cli_args=None):
        super().__init__("redis_benchmark", benchresults_cls, cli_args)

        self.ycsb_root = os.path.abspath(os.path.join(self.args.bench_binary_dir, ".."))

        if self.args.redis_script_dir is None:
            self.args.redis_script_dir = os.path.join(self.ycsb_root, "script")
        if self.args.redis_config_dir is None:
            self.args.redis_config_dir = os.path.join(self.ycsb_root, "redis", "config")
        if self.args.redis_data_dir is None:
            self.args.redis_data_dir = os.path.join(self.ycsb_root, "redis")

        self.cache_ext_policy = CacheExtPolicy(
            DEFAULT_CACHE_EXT_CGROUP,
            self.args.policy_loader,
            self.args.redis_data_dir,
        )

        CLEANUP_TASKS.append(lambda: self._safe_stop_policy())
        CLEANUP_TASKS.append(lambda: self.stop_redis_servers())
        CLEANUP_TASKS.append(lambda: enable_smt())

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--policy-loader",
            type=str,
            required=True,
            help="Path to cache_ext policy loader binary",
        )
        parser.add_argument(
            "--bench-binary-dir",
            type=str,
            required=True,
            help="Directory containing run_redis and init_redis binaries",
        )
        parser.add_argument(
            "--benchmark",
            type=str,
            required=True,
            help="Comma-separated benchmark configs, e.g. 'uniform,zipfian'",
        )
        parser.add_argument(
            "--redis-script-dir",
            type=str,
            default=None,
            help="Directory containing run_redis.sh (default: <ycsb_root>/script)",
        )
        parser.add_argument(
            "--redis-config-dir",
            type=str,
            default=None,
            help="Directory containing redis benchmark YAMLs (default: <ycsb_root>/redis/config)",
        )
        parser.add_argument(
            "--redis-data-dir",
            type=str,
            default=None,
            help="Redis data dir used as cache_ext watch_dir (default: <ycsb_root>/redis)",
        )
        parser.add_argument(
            "--redis-host",
            type=str,
            default="127.0.0.1",
            help="Redis host written to benchmark YAML",
        )
        parser.add_argument(
            "--redis-port",
            type=int,
            default=6380,
            help="Redis port for benchmark target",
        )
        parser.add_argument(
            "--redis-servers",
            type=int,
            default=1,
            help="Number of Redis servers to start via run_redis.sh",
        )
        parser.add_argument(
            "--redis-batch-size",
            type=int,
            default=1,
            help="Redis pipeline batch size written to benchmark YAML",
        )
        parser.add_argument(
            "--cgroup-size",
            type=int,
            default=1 * GiB,
            help="Per-cgroup memory limit in bytes",
        )
        parser.add_argument(
            "--nr-op",
            type=int,
            default=200000,
            help="Number of ops for measured run",
        )
        parser.add_argument(
            "--nr-warmup-op",
            type=int,
            default=0,
            help="Number of warmup ops",
        )

    def generate_configs(self, configs: List[Dict]) -> List[Dict]:
        configs = add_config_option(
            "benchmark",
            parse_strings_string(self.args.benchmark),
            configs,
        )
        configs = add_config_option("cgroup_size", [self.args.cgroup_size], configs)

        if self.args.default_only:
            configs = add_config_option("cgroup_name", [DEFAULT_BASELINE_CGROUP], configs)
        else:
            configs = add_config_option(
                "cgroup_name",
                [DEFAULT_BASELINE_CGROUP, DEFAULT_CACHE_EXT_CGROUP],
                configs,
            )

        new_configs = []
        policy_loader_name = os.path.basename(self.cache_ext_policy.loader_path)
        for config in configs:
            new_config = config.copy()
            if new_config["cgroup_name"] == DEFAULT_CACHE_EXT_CGROUP:
                new_config["policy_loader"] = policy_loader_name
            new_configs.append(new_config)

        return add_config_option(
            "iteration",
            list(range(1, self.args.iterations + 1)),
            new_configs,
        )

    def benchmark_prepare(self, config):
        drop_page_cache()
        disable_swap()
        disable_smt()

        if config["cgroup_name"] == DEFAULT_CACHE_EXT_CGROUP:
            recreate_cache_ext_cgroup(limit_in_bytes=config["cgroup_size"])
            self.cache_ext_policy.start()
        else:
            recreate_baseline_cgroup(limit_in_bytes=config["cgroup_size"])

        self.start_redis_servers()

    def benchmark_cmd(self, config):
        bench_binary = os.path.join(self.args.bench_binary_dir, "run_redis")
        bench_file = os.path.join(self.args.redis_config_dir, f"{config['benchmark']}.yaml")
        if not os.path.exists(bench_file):
            raise Exception(f"Redis benchmark config not found: {bench_file}")
        if not os.path.exists(bench_binary):
            raise Exception(f"Redis benchmark binary not found: {bench_binary}")

        with edit_yaml_file(bench_file) as bench_config:
            bench_config["redis"]["addr"] = self.args.redis_host
            bench_config["redis"]["port"] = self.args.redis_port
            bench_config["redis"]["batch_size"] = self.args.redis_batch_size
            bench_config["workload"]["nr_op"] = self.args.nr_op
            bench_config["workload"]["nr_warmup_op"] = self.args.nr_warmup_op

        return [
            "sudo",
            "cgexec",
            "-g",
            f"memory:{config['cgroup_name']}",
            bench_binary,
            bench_file,
            str(self.args.redis_port),
        ]

    def after_benchmark(self, config):
        if config["cgroup_name"] == DEFAULT_CACHE_EXT_CGROUP:
            self._safe_stop_policy()
        self.stop_redis_servers()
        sleep(2)
        enable_smt()

    def parse_results(self, stdout: str) -> BenchResults:
        return BenchResults(parse_redis_bench_results(stdout))

    def start_redis_servers(self):
        run(
            ["bash", "./run_redis.sh", str(self.args.redis_servers)],
            cwd=self.args.redis_script_dir,
        )
        sleep(2)

    def stop_redis_servers(self):
        with suppress(subprocess.CalledProcessError):
            run(["sudo", "pkill", "redis-server"])
            sleep(1)

    def _safe_stop_policy(self):
        with suppress(Exception):
            if self.cache_ext_policy.has_started:
                self.cache_ext_policy.stop()


def main():
    redis_bench = RedisBenchmark()

    if not os.path.exists(redis_bench.args.bench_binary_dir):
        raise Exception(
            "Benchmark binary directory not found: %s"
            % redis_bench.args.bench_binary_dir
        )
    if not os.path.exists(redis_bench.args.redis_script_dir):
        raise Exception(
            "Redis script directory not found: %s" % redis_bench.args.redis_script_dir
        )
    if not os.path.exists(redis_bench.args.redis_config_dir):
        raise Exception(
            "Redis config directory not found: %s" % redis_bench.args.redis_config_dir
        )
    if not os.path.exists(redis_bench.args.redis_data_dir):
        raise Exception(
            "Redis data directory not found: %s" % redis_bench.args.redis_data_dir
        )

    redis_bench.benchmark()


if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        main()
    except Exception as e:
        log.error("Error in main: %s", e)
        log.info("Cleaning up")
        for task in CLEANUP_TASKS:
            task()
        log.error("Re-raising exception")
        raise e
