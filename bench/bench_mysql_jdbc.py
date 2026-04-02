import argparse
import grp
import logging
import os
import pwd
import re
import shlex
import socket
import subprocess
import time
from contextlib import suppress
from typing import Dict, List

from bench_lib import *


log = logging.getLogger(__name__)
GiB = 2**30
CLEANUP_TASKS = []


def _escape_sql_string(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _normalize_op_name(op: str) -> str:
    return op.strip().upper().replace("-", "_")


def _primary_group_name(user: str) -> str:
    return grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name


def _set_op_latency(results: Dict, op: str, metric: str, value_us: float):
    value_ns = value_us * 1000.0
    prefix = op.lower()
    if metric == "avg":
        results[f"{prefix}_latency_avg"] = value_ns
    elif metric == "p99":
        results[f"{prefix}_latency_p99"] = value_ns


def parse_ycsb_jdbc_results(stdout: str) -> Dict:
    # Example YCSB lines:
    # [OVERALL], Throughput(ops/sec), 11289.51
    # [READ], AverageLatency(us), 837.3
    # [READ], 99thPercentileLatency(us), 3395
    results: Dict[str, float] = {}

    throughput_re = re.compile(
        r"^\[OVERALL\],\s*Throughput\(ops/sec\),\s*([0-9]+(?:\.[0-9]+)?)$"
    )
    latency_avg_re = re.compile(
        r"^\[(READ|UPDATE|INSERT|SCAN|READ_MODIFY_WRITE|READ-MODIFY-WRITE)\],\s*"
        r"(?:AverageLatency|Average)\(us\),\s*([0-9]+(?:\.[0-9]+)?)$"
    )
    latency_p99_re = re.compile(
        r"^\[(READ|UPDATE|INSERT|SCAN|READ_MODIFY_WRITE|READ-MODIFY-WRITE)\],\s*"
        r"99thPercentileLatency\(us\),\s*([0-9]+(?:\.[0-9]+)?)$"
    )

    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("["):
            continue

        m = throughput_re.match(line)
        if m:
            results["throughput_avg"] = float(m.group(1))
            continue

        m = latency_avg_re.match(line)
        if m:
            op = _normalize_op_name(m.group(1))
            _set_op_latency(results, op, "avg", float(m.group(2)))
            continue

        m = latency_p99_re.match(line)
        if m:
            op = _normalize_op_name(m.group(1))
            _set_op_latency(results, op, "p99", float(m.group(2)))
            continue

    # Keep output compatible with existing plotting/summary scripts.
    if "read_latency_avg" in results:
        results["latency_avg"] = results["read_latency_avg"]
    elif "update_latency_avg" in results:
        results["latency_avg"] = results["update_latency_avg"]
    elif "insert_latency_avg" in results:
        results["latency_avg"] = results["insert_latency_avg"]

    if "read_latency_p99" in results:
        results["latency_p99"] = results["read_latency_p99"]
    elif "update_latency_p99" in results:
        results["latency_p99"] = results["update_latency_p99"]
    elif "insert_latency_p99" in results:
        results["latency_p99"] = results["insert_latency_p99"]

    required = ["throughput_avg", "latency_avg", "latency_p99"]
    if not all(key in results for key in required):
        raise Exception("Could not parse YCSB JDBC results from stdout:\n%s" % stdout)

    return results


class MySQLJDBCBenchmark(BenchmarkFramework):
    def __init__(self, benchresults_cls=BenchResults, cli_args=None):
        super().__init__("mysql_jdbc_benchmark", benchresults_cls, cli_args)

        self.args.ycsb_root = os.path.abspath(self.args.ycsb_root)
        self.args.mysql_data_dir = os.path.abspath(self.args.mysql_data_dir)
        self.args.mysql_runtime_dir = os.path.abspath(self.args.mysql_runtime_dir)
        self.args.jdbc_driver_jar = self._resolve_path(self.args.jdbc_driver_jar)
        self.args.jdbc_props_template = self._resolve_path(self.args.jdbc_props_template)

        self.socket_path = os.path.join(self.args.mysql_runtime_dir, "mysql.sock")
        self.pid_path = os.path.join(self.args.mysql_runtime_dir, "mysqld.pid")
        self.log_path = os.path.join(self.args.mysql_runtime_dir, "mysqld.log")
        self.init_log_path = os.path.join(self.args.mysql_runtime_dir, "mysqld-init.log")
        self.generated_props_path = os.path.join(
            self.args.mysql_runtime_dir,
            "ycsb_mysql.properties",
        )

        self.mysql_proc = None

        self.cache_ext_policy = CacheExtPolicy(
            DEFAULT_CACHE_EXT_CGROUP,
            self.args.policy_loader,
            self.args.mysql_data_dir,
        )

        CLEANUP_TASKS.append(lambda: self._safe_stop_policy())
        CLEANUP_TASKS.append(lambda: self._safe_stop_mysql())
        CLEANUP_TASKS.append(lambda: enable_smt())

    def _resolve_path(self, path: str) -> str:
        if os.path.isabs(path):
            return path
        return os.path.abspath(os.path.join(self.args.ycsb_root, path))

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--policy-loader",
            type=str,
            required=True,
            help="Path to cache_ext policy loader binary",
        )
        parser.add_argument(
            "--ycsb-root",
            type=str,
            required=True,
            help="Path to ycsb-jdbc root directory",
        )
        parser.add_argument(
            "--benchmark",
            type=str,
            default="workloada",
            help="Comma-separated workload names under ycsb-jdbc/workloads",
        )
        parser.add_argument(
            "--jdbc-driver-jar",
            type=str,
            required=True,
            help="Path to mysql connector jar",
        )
        parser.add_argument(
            "--jdbc-props-template",
            type=str,
            default="conf/mysql_cacheext.properties.sample",
            help="Template properties file under ycsb-jdbc",
        )
        parser.add_argument(
            "--ycsb-python",
            type=str,
            default="python2",
            help="Python interpreter used by ycsb-jdbc/bin/ycsb",
        )

        parser.add_argument(
            "--mysql-host",
            type=str,
            default="127.0.0.1",
            help="MySQL host for JDBC URL",
        )
        parser.add_argument(
            "--mysql-port",
            type=int,
            default=3307,
            help="MySQL TCP port for benchmark",
        )
        parser.add_argument(
            "--mysql-data-dir",
            type=str,
            default="/var/lib/mysql/cacheext",
            help="MySQL data directory, recreated for each run",
        )
        parser.add_argument(
            "--mysql-runtime-dir",
            type=str,
            default="/tmp/cache_ext_mysql_runtime",
            help="Runtime directory for socket/pid/log/properties",
        )
        parser.add_argument(
            "--mysqld-bin",
            type=str,
            default="mysqld",
            help="Path to mysqld binary",
        )
        parser.add_argument(
            "--mysql-bin",
            type=str,
            default="mysql",
            help="Path to mysql client binary",
        )
        parser.add_argument(
            "--mysqld-run-user",
            type=str,
            default="mysql",
            help="mysqld --user value when started via sudo cgexec",
        )
        parser.add_argument(
            "--mysqld-extra-args",
            type=str,
            default="",
            help="Extra args appended to mysqld command",
        )
        parser.add_argument(
            "--mysql-start-timeout",
            type=int,
            default=60,
            help="Timeout in seconds waiting for mysqld startup",
        )

        parser.add_argument(
            "--mysql-admin-user",
            type=str,
            default="root",
            help="Admin user used to create DB/table and grants",
        )
        parser.add_argument(
            "--mysql-admin-passwd",
            type=str,
            default="",
            help="Admin password (empty for --initialize-insecure defaults)",
        )
        parser.add_argument(
            "--mysql-user",
            type=str,
            default="ycsb",
            help="JDBC user",
        )
        parser.add_argument(
            "--mysql-passwd",
            type=str,
            default="ycsb-pass",
            help="JDBC password",
        )
        parser.add_argument(
            "--mysql-db",
            type=str,
            default="ycsb",
            help="Database name",
        )
        parser.add_argument(
            "--mysql-table",
            type=str,
            default="usertable",
            help="YCSB table name",
        )

        parser.add_argument(
            "--threads",
            type=int,
            default=10,
            help="YCSB threads",
        )
        parser.add_argument(
            "--recordcount",
            type=int,
            default=100000,
            help="YCSB load recordcount",
        )
        parser.add_argument(
            "--operationcount",
            type=int,
            default=100000,
            help="YCSB run operationcount",
        )
        parser.add_argument(
            "--target",
            type=int,
            default=0,
            help="Target ops/sec for ycsb run (0 means unlimited)",
        )

        parser.add_argument(
            "--cgroup-size",
            type=int,
            default=2 * GiB,
            help="Per-cgroup memory.max in bytes",
        )

    def generate_configs(self, configs: List[Dict]) -> List[Dict]:
        configs = add_config_option(
            "benchmark",
            parse_strings_string(self.args.benchmark),
            configs,
        )
        configs = add_config_option("cgroup_size", [self.args.cgroup_size], configs)
        configs = add_config_option("threads", [self.args.threads], configs)
        configs = add_config_option("recordcount", [self.args.recordcount], configs)
        configs = add_config_option(
            "operationcount", [self.args.operationcount], configs
        )

        if self.args.default_only:
            configs = add_config_option("cgroup_name", [DEFAULT_BASELINE_CGROUP], configs)
        else:
            configs = add_config_option(
                "cgroup_name",
                [DEFAULT_BASELINE_CGROUP, DEFAULT_CACHE_EXT_CGROUP],
                configs,
            )

        policy_loader_name = os.path.basename(self.cache_ext_policy.loader_path)
        new_configs = []
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
            policy_loader_name = os.path.basename(self.cache_ext_policy.loader_path)
            if policy_loader_name == "cache_ext_s3fifo.out":
                self.cache_ext_policy.start(cgroup_size=config["cgroup_size"])
            else:
                self.cache_ext_policy.start()
        else:
            recreate_baseline_cgroup(limit_in_bytes=config["cgroup_size"])

        self._initialize_mysql_data_dir()
        self._start_mysql(config)
        self._wait_mysql_ready(timeout_sec=self.args.mysql_start_timeout)
        self._prepare_mysql_schema()
        self._render_jdbc_properties()

        # Load is part of benchmark setup so each config starts from the same state.
        load_cmd = self._build_ycsb_cmd(config, phase="load", include_operation_count=False)
        load_cmd = ["taskset", "-c", "0-%s" % str(config["cpus"] - 1)] + load_cmd
        log.info("Running YCSB load command: %s", load_cmd)
        load_stdout = run_command_with_live_output(
            load_cmd, cwd=self.args.ycsb_root, env=os.environ.copy()
        )
        self._assert_no_ycsb_db_errors(load_stdout, phase="load")

    def benchmark_cmd(self, config):
        return self._build_ycsb_cmd(config, phase="run", include_operation_count=True)

    def after_benchmark(self, config):
        if config["cgroup_name"] == DEFAULT_CACHE_EXT_CGROUP:
            self._safe_stop_policy()
        self._safe_stop_mysql()
        time.sleep(2)
        enable_smt()

    def parse_results(self, stdout: str) -> BenchResults:
        return BenchResults(parse_ycsb_jdbc_results(stdout))

    def _workload_path(self, benchmark_name: str) -> str:
        if os.path.isabs(benchmark_name):
            return benchmark_name
        if "/" in benchmark_name:
            return os.path.join(self.args.ycsb_root, benchmark_name)
        return os.path.join(self.args.ycsb_root, "workloads", benchmark_name)

    def _build_ycsb_cmd(self, config: Dict, phase: str, include_operation_count: bool):
        workload_path = self._workload_path(config["benchmark"])
        if not os.path.exists(workload_path):
            raise Exception("Workload file not found: %s" % workload_path)

        if not os.path.exists(self.args.jdbc_driver_jar):
            raise Exception("JDBC driver jar not found: %s" % self.args.jdbc_driver_jar)

        ycsb_exec = os.path.join(self.args.ycsb_root, "bin", "ycsb")
        if not os.path.exists(ycsb_exec):
            raise Exception("ycsb executable not found: %s" % ycsb_exec)

        cmd = [
            "sudo",
            "cgexec",
            "-g",
            "memory:%s" % config["cgroup_name"],
            self.args.ycsb_python,
            ycsb_exec,
            phase,
            "jdbc",
            "-s",
            "-P",
            workload_path,
            "-P",
            self.generated_props_path,
            "-cp",
            self.args.jdbc_driver_jar,
            "-p",
            "table=%s" % self.args.mysql_table,
            "-threads",
            str(config["threads"]),
            "-p",
            "recordcount=%s" % config["recordcount"],
        ]

        if include_operation_count:
            cmd += ["-p", "operationcount=%s" % config["operationcount"]]
            if self.args.target > 0:
                cmd += ["-target", str(self.args.target)]

        return cmd

    def _assert_no_ycsb_db_errors(self, output: str, phase: str):
        error_markers = [
            "Error in database operation",
            "site.ycsb.DBException",
            "SQLNonTransientConnectionException",
            "UnableToConnectException",
        ]
        for marker in error_markers:
            if marker in output:
                raise Exception("YCSB %s failed due to DB error marker: %s" % (phase, marker))

    def _initialize_mysql_data_dir(self):
        self._safe_stop_mysql()

        with suppress(FileNotFoundError):
            os.remove(self.socket_path)

        run(["sudo", "rm", "-rf", self.args.mysql_data_dir])
        parent_dir = os.path.dirname(self.args.mysql_data_dir)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        os.makedirs(self.args.mysql_runtime_dir, exist_ok=True)
        os.chmod(self.args.mysql_runtime_dir, 0o777)
        with suppress(FileNotFoundError):
            os.remove(self.init_log_path)

        init_cmd = [
            "sudo",
            self.args.mysqld_bin,
            "--no-defaults",
            "--initialize-insecure",
            "--datadir=%s" % self.args.mysql_data_dir,
            "--log-error=%s" % self.init_log_path,
        ]

        log.info("Initializing MySQL data dir: %s", init_cmd)
        init_proc = subprocess.run(
            init_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if init_proc.returncode != 0:
            init_log_tail = ""
            if os.path.exists(self.init_log_path):
                with open(self.init_log_path, "r") as f:
                    init_log_tail = "\n".join(f.read().splitlines()[-80:])
            extra_hint = ""
            if "errno 13" in init_proc.stderr.lower() or "permission denied" in init_proc.stderr.lower():
                extra_hint = (
                    "\nHint: this host likely restricts mysqld datadir via AppArmor. "
                    "Use a subdirectory under /var/lib/mysql (for example /var/lib/mysql/cacheext), "
                    "or update AppArmor profile if you must use /mydata."
                )
            raise Exception(
                "mysqld initialization failed (code=%s).\n"
                "cmd: %s\n"
                "stdout:\n%s\n"
                "stderr:\n%s\n"
                "init_log_tail:\n%s"
                "%s"
                % (
                    init_proc.returncode,
                    init_cmd,
                    init_proc.stdout,
                    init_proc.stderr,
                    init_log_tail,
                    extra_hint,
                )
            )

        if self.args.mysqld_run_user:
            run_group = _primary_group_name(self.args.mysqld_run_user)
            run(
                [
                    "sudo",
                    "chown",
                    "-R",
                    "%s:%s" % (self.args.mysqld_run_user, run_group),
                    self.args.mysql_data_dir,
                ]
            )

    def _start_mysql(self, config: Dict):
        extra_args = shlex.split(self.args.mysqld_extra_args)

        cmd = [
            "sudo",
            "cgexec",
            "-g",
            "memory:%s" % config["cgroup_name"],
            self.args.mysqld_bin,
            "--no-defaults",
            "--datadir=%s" % self.args.mysql_data_dir,
            "--bind-address=%s" % self.args.mysql_host,
            "--port=%s" % self.args.mysql_port,
            "--socket=%s" % self.socket_path,
            "--pid-file=%s" % self.pid_path,
            "--log-error=%s" % self.log_path,
            "--skip-networking=0",
        ]

        if self.args.mysqld_run_user:
            cmd.append("--user=%s" % self.args.mysqld_run_user)

        cmd += extra_args

        log.info("Starting mysqld: %s", cmd)
        self.mysql_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _wait_mysql_ready(self, timeout_sec: int):
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if self.mysql_proc and self.mysql_proc.poll() is not None:
                out, err = self.mysql_proc.communicate()
                raise Exception(
                    "mysqld exited unexpectedly. stdout:\n%s\nstderr:\n%s" % (out, err)
                )

            if os.path.exists(self.socket_path):
                try:
                    self._mysql_exec("SELECT 1;", use_socket=True)
                    return
                except Exception:
                    pass

            # Fallback probe via TCP.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.3)
                if sock.connect_ex((self.args.mysql_host, self.args.mysql_port)) == 0:
                    try:
                        self._mysql_exec("SELECT 1;", use_socket=False)
                        return
                    except Exception:
                        pass

            time.sleep(1)

        if os.path.exists(self.log_path):
            with open(self.log_path, "r") as f:
                log_tail = "\n".join(f.read().splitlines()[-50:])
        else:
            log_tail = "<no mysqld log found>"

        raise Exception("Timed out waiting for MySQL startup. Log tail:\n%s" % log_tail)

    def _mysql_exec(self, sql: str, use_socket: bool):
        cmd = [self.args.mysql_bin]

        if use_socket:
            cmd += ["--protocol=socket", "--socket=%s" % self.socket_path]
        else:
            cmd += [
                "--protocol=tcp",
                "--host=%s" % self.args.mysql_host,
                "--port=%s" % self.args.mysql_port,
            ]

        cmd += ["-u", self.args.mysql_admin_user, "-e", sql]

        env = os.environ.copy()
        if self.args.mysql_admin_passwd:
            env["MYSQL_PWD"] = self.args.mysql_admin_passwd

        run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def _prepare_mysql_schema(self):
        db = self.args.mysql_db
        table = self.args.mysql_table
        user = _escape_sql_string(self.args.mysql_user)
        passwd = _escape_sql_string(self.args.mysql_passwd)

        sql = "\n".join(
            [
                "CREATE DATABASE IF NOT EXISTS `%s`;" % db,
                "CREATE USER IF NOT EXISTS '%s'@'127.0.0.1' IDENTIFIED BY '%s';" % (user, passwd),
                "CREATE USER IF NOT EXISTS '%s'@'localhost' IDENTIFIED BY '%s';" % (user, passwd),
                "GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'127.0.0.1';" % (db, user),
                "GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'localhost';" % (db, user),
                "FLUSH PRIVILEGES;",
                "USE `%s`;" % db,
                "DROP TABLE IF EXISTS `%s`;" % table,
                "CREATE TABLE `%s` (" % table,
                "YCSB_KEY VARCHAR(255) PRIMARY KEY,",
                "FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT,",
                "FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT",
                ");",
            ]
        )

        self._mysql_exec(sql, use_socket=True)

    def _render_jdbc_properties(self):
        if not os.path.exists(self.args.jdbc_props_template):
            raise Exception("JDBC properties template not found: %s" % self.args.jdbc_props_template)

        props: Dict[str, str] = {}
        with open(self.args.jdbc_props_template, "r") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                props[key.strip()] = value.strip()

        jdbc_url = (
            "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&characterEncoding=utf8"
            % (self.args.mysql_host, self.args.mysql_port, self.args.mysql_db)
        )

        props["db.driver"] = "com.mysql.cj.jdbc.Driver"
        props["db.url"] = jdbc_url
        props["db.user"] = self.args.mysql_user
        props["db.passwd"] = self.args.mysql_passwd
        props.setdefault("jdbc.fetchsize", "100")
        props.setdefault("jdbc.autocommit", "true")
        props.setdefault("jdbc.batchupdateapi", "true")
        props.setdefault("db.batchsize", "1000")

        with open(self.generated_props_path, "w") as f:
            f.write("# Auto-generated by bench_mysql_jdbc.py\n")
            for key in sorted(props.keys()):
                f.write("%s=%s\n" % (key, props[key]))

    def _safe_stop_policy(self):
        with suppress(Exception):
            if self.cache_ext_policy.has_started:
                self.cache_ext_policy.stop()

    def _safe_stop_mysql(self):
        pid = None
        if os.path.exists(self.pid_path):
            try:
                with open(self.pid_path, "r") as f:
                    pid = f.read().strip()
            except Exception:
                pid = None

        if pid:
            with suppress(subprocess.CalledProcessError):
                run(["sudo", "kill", "-TERM", pid])

        if self.mysql_proc:
            with suppress(subprocess.TimeoutExpired):
                self.mysql_proc.wait(timeout=20)
            if self.mysql_proc.poll() is None:
                with suppress(subprocess.CalledProcessError):
                    run(["sudo", "kill", "-9", str(self.mysql_proc.pid)])
                with suppress(subprocess.TimeoutExpired):
                    self.mysql_proc.wait(timeout=5)

        self.mysql_proc = None
        with suppress(FileNotFoundError):
            os.remove(self.socket_path)


def main():
    mysql_bench = MySQLJDBCBenchmark()

    ycsb_bin = os.path.join(mysql_bench.args.ycsb_root, "bin", "ycsb")
    if not os.path.exists(ycsb_bin):
        raise Exception("ycsb executable not found: %s" % ycsb_bin)

    if not os.path.exists(mysql_bench.args.jdbc_driver_jar):
        raise Exception("JDBC driver jar not found: %s" % mysql_bench.args.jdbc_driver_jar)

    mysql_bench.benchmark()


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
