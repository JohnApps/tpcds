# mi_tpcds_quack_bench.py
# Mistral.ai version
# cl_tpcds_quack_bench.py
"""
TPC-DS Benchmark Harness for DuckDB Quack
Target: Windows 11 / DuckDB 2.0 / CPython 3.13.11 (standard GIL build)

Execution modes:
  1. local_inprocess - in-process DuckDB against the database file (read-only)
  2. local_quack     - Quack server on localhost
  3. remote_quack    - Quack server on a remote host

Repeats are warm: one connection per mode (sequential) or per worker (concurrent)
stays open across all repeats, and warmup runs once before the first repeat.

Note: DuckDB settings such as threads, memory_limit and preserve_insertion_order
are instance-wide. All connections to the same database file in this process
share them, so concurrent workers must use the same WORKER_THREADS value.

Usage:
  python cl_tpcds_quack_bench.py [--smoke] [--modes M1,M2,...] [--workers N]
                                 [--repeat N] [--queries N] [--no-warmup]
                                 [--no-plots] [--validate-only] [--tag LABEL]

Environment (.env or OS):
  DB_PATH                   TPC-DS database file      (default H:\\tpcds\\tpcds_sf10.db)
  CACHE_QUERIES             cached queries file       (default H:\\tpcds\\queries_cached.txt)
  SQL_FILE_PATH             generated SQL file        (default H:\\tpcds\\tpcds_queries.sql)
  QUACK_HOST                Quack server host         (default localhost)
  QUACK_PORT                Quack server port        (default 9494)
  QUACK_TOKEN               Quack auth token          (required for *_quack modes)
  RESULTS_DIR               output directory          (default H:\\tpcds\\results)
  N_QUERIES                 number of TPC-DS queries  (default 99, max 99)
  N_WORKERS                 concurrent workers        (default 1)
  N_REPEAT                  repetitions               (default 3)
  WARMUP_QUERIES            warmup queries per conn   (default 5)
  MODE                      default execution mode    (default local_inprocess)
  PHYSICAL_CORES            physical core count       (default 8)
  QUERY_THREADS             DuckDB threads for sequential runs, 0 = PHYSICAL_CORES
  WORKER_THREADS            DuckDB threads per concurrent worker (default 1)
  MEMORY_LIMIT              e.g. '8GB' (recommended; empty = engine default)
  PRESERVE_INSERTION_ORDER  true/false (default false; faster for analytic queries)
  RESULT_MODE               full | drain | arrow  (how query results are consumed)
  FETCH_CHUNK_ROWS          rows per fetchmany() when RESULT_MODE=drain (default 65536)
  BARRIER_TIMEOUT           seconds to wait for synchronised worker start (default 600)
  CHART_FORMAT              svg | png  (default svg; the other is the fallback)

Optimized revision highlights (behaviour-compatible with the original):
  * per-run CSV export (tpcds_runs.csv) for spreadsheet analysis
  * --tag LABEL groups all output files of one run
  * stats are computed once per mode and reused by report + chart
  * deferred matplotlib import: skipped entirely with --no-plots/--validate-only
  * iterative flatten (no recursion), single-pass SQL parser cleanup
  * empty --modes lists are rejected instead of silently producing no results
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import platform
import re
import socket
import statistics
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import duckdb
from dotenv import load_dotenv

# --- optional accelerators ---------------------------------------------------
try:
    import numpy as np
except ImportError:
    np = None


# ============================================================================
# Console + logging
# ============================================================================
def configure_console() -> None:
    """Force UTF-8 on Windows consoles."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError, OSError):
            pass


_LOG_FORMATTER = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S")


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("tpcds_bench")
    if logger.handlers:
        return logger
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_LOG_FORMATTER)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


LOG = _get_logger()


# ============================================================================
# Configuration
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
VALID_MODES = ("local_inprocess", "local_quack", "remote_quack")
QUACK_MODES = ("local_quack", "remote_quack")
RESULT_MODES = ("full", "drain", "arrow")
CHART_FORMATS = ("svg", "png")


@dataclass(slots=True)
class Config:
    db_path: Path
    cache_queries: Path
    sql_file_path: Path
    quack_host: str
    quack_port: int
    quack_token: str
    results_dir: Path
    n_queries: int
    n_workers: int
    n_repeat: int
    mode: str
    warmup_queries: int
    physical_cores: int
    query_threads: int
    worker_threads: int
    memory_limit: str
    preserve_insertion_order: bool
    result_mode: str
    fetch_chunk_rows: int
    barrier_timeout: float
    chart_format: str

    def snapshot(self) -> Dict[str, Any]:
        return {
            "db_path": str(self.db_path),
            "cache_queries": str(self.cache_queries),
            "sql_file_path": str(self.sql_file_path),
            "quack_host": self.quack_host,
            "quack_port": self.quack_port,
            "quack_token": self.quack_token[:8] + "..." if self.quack_token else "",
            "results_dir": str(self.results_dir),
            "n_queries": self.n_queries,
            "n_workers": self.n_workers,
            "n_repeat": self.n_repeat,
            "mode": self.mode,
            "warmup_queries": self.warmup_queries,
            "physical_cores": self.physical_cores,
            "query_threads": self.query_threads,
            "worker_threads": self.worker_threads,
            "memory_limit": self.memory_limit,
            "preserve_insertion_order": self.preserve_insertion_order,
            "result_mode": self.result_mode,
            "fetch_chunk_rows": self.fetch_chunk_rows,
            "barrier_timeout": self.barrier_timeout,
            "chart_format": self.chart_format,
        }


def _env_path(name: str, default: str) -> Path:
    raw = os.getenv(name, default)
    return Path(os.path.expandvars(raw)).expanduser()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_config() -> Config:
    dotenv_path = os.getenv("DOTENV_PATH")
    if dotenv_path:
        load_dotenv(dotenv_path, override=True)
    else:
        load_dotenv(BASE_DIR / ".env", override=True)

    physical = max(1, int(os.getenv("PHYSICAL_CORES", "8")))
    query_threads = int(os.getenv("QUERY_THREADS", "0"))

    return Config(
        db_path=_env_path("DB_PATH", r"H:\tpcds\tpcds_sf10.db"),
        cache_queries=_env_path("CACHE_QUERIES", r"H:\tpcds\queries_cached.txt"),
        sql_file_path=_env_path("SQL_FILE_PATH", r"H:\tpcds\tpcds_queries.sql"),
        quack_host=os.getenv("QUACK_HOST", "localhost"),
        quack_port=int(os.getenv("QUACK_PORT", "9494")),
        quack_token=os.getenv("QUACK_TOKEN", ""),
        results_dir=_env_path("RESULTS_DIR", r"H:\tpcds\results"),
        n_queries=int(os.getenv("N_QUERIES", "99")),
        n_workers=int(os.getenv("N_WORKERS", "1")),
        n_repeat=int(os.getenv("N_REPEAT", "3")),
        mode=os.getenv("MODE", "local_inprocess").strip().lower(),
        warmup_queries=int(os.getenv("WARMUP_QUERIES", "5")),
        physical_cores=physical,
        query_threads=query_threads if query_threads > 0 else physical,
        worker_threads=max(1, int(os.getenv("WORKER_THREADS", "1"))),
        memory_limit=os.getenv("MEMORY_LIMIT", "").strip(),
        preserve_insertion_order=_env_bool("PRESERVE_INSERTION_ORDER", False),
        result_mode=os.getenv("RESULT_MODE", "full").strip().lower(),
        fetch_chunk_rows=max(1, int(os.getenv("FETCH_CHUNK_ROWS", "65536"))),
        barrier_timeout=float(os.getenv("BARRIER_TIMEOUT", "600")),
        chart_format=os.getenv("CHART_FORMAT", "svg").strip().lower(),
    )


CONFIG = load_config()


# ============================================================================
# Issues and validation
# ============================================================================
@dataclass(slots=True)
class Issue:
    level: str
    message: str


def validate_config(cfg: Config, modes: Sequence[str]) -> List[Issue]:
    issues: List[Issue] = []

    if not modes:
        issues.append(Issue("error", "no execution mode selected"))
        return issues

    for m in modes:
        if m not in VALID_MODES:
            issues.append(Issue("error", f"unknown mode '{m}'"))
    if any(i.level == "error" for i in issues):
        return issues

    if not (1 <= cfg.n_queries <= 99):
        issues.append(Issue("error", f"N_QUERIES must be 1..99, got {cfg.n_queries}"))
    if cfg.n_repeat < 1:
        issues.append(Issue("error", "N_REPEAT must be >= 1"))
    if cfg.n_workers < 1:
        issues.append(Issue("error", "N_WORKERS must be >= 1"))
    if cfg.warmup_queries < 0:
        issues.append(Issue("error", "WARMUP_QUERIES must be >= 0"))
    if not (1 <= cfg.quack_port <= 65535):
        issues.append(Issue("error", f"QUACK_PORT out of range: {cfg.quack_port}"))

    if cfg.result_mode not in RESULT_MODES:
        issues.append(Issue("error", f"RESULT_MODE must be one of {', '.join(RESULT_MODES)}"))
    if cfg.result_mode == "arrow":
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            issues.append(Issue("error", "RESULT_MODE=arrow requires pyarrow (pip install pyarrow)"))

    if cfg.chart_format not in CHART_FORMATS:
        issues.append(Issue("error", f"CHART_FORMAT must be one of {', '.join(CHART_FORMATS)}"))

    if "local_inprocess" in modes and not cfg.db_path.is_file():
        issues.append(Issue("error", f"database file not found: {cfg.db_path}"))
    if any(m in QUACK_MODES for m in modes) and not cfg.quack_token:
        issues.append(Issue("error", "QUACK_TOKEN required for Quack modes"))

    if cfg.n_workers > 1 and cfg.n_workers * cfg.worker_threads > cfg.physical_cores:
        issues.append(Issue(
            "warning",
            f"N_WORKERS x WORKER_THREADS = {cfg.n_workers * cfg.worker_threads} exceeds "
            f"PHYSICAL_CORES = {cfg.physical_cores}; results will include contention",
        ))
    if cfg.n_workers == 1 and cfg.query_threads > cfg.physical_cores:
        issues.append(Issue(
            "warning",
            f"QUERY_THREADS = {cfg.query_threads} exceeds PHYSICAL_CORES = {cfg.physical_cores}",
        ))
    if not cfg.memory_limit:
        issues.append(Issue(
            "warning",
            "MEMORY_LIMIT is not set; set it explicitly so runs are reproducible",
        ))

    try:
        cfg.results_dir.mkdir(parents=True, exist_ok=True)
        probe = cfg.results_dir / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as exc:
        issues.append(Issue("error", f"results directory not writable: {exc}"))

    return issues


def report_issues(issues: Sequence[Issue]) -> int:
    errors = sum(1 for issue in issues if issue.level == "error")
    for issue in issues:
        if issue.level == "error":
            LOG.error(f"[ERROR] {issue.message}")
        else:
            LOG.warning(f"[WARN]  {issue.message}")
    return errors


# ============================================================================
# Matplotlib pre-check (deferred: only called when charts are wanted)
# ============================================================================
_MATPLOTLIB_AVAILABLE: Optional[bool] = None
_MATPLOTLIB_ERROR: Optional[str] = None


def check_matplotlib() -> Tuple[bool, Optional[str]]:
    global _MATPLOTLIB_AVAILABLE, _MATPLOTLIB_ERROR
    if _MATPLOTLIB_AVAILABLE is not None:
        return (_MATPLOTLIB_AVAILABLE, _MATPLOTLIB_ERROR)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot  # noqa: F401
        _MATPLOTLIB_AVAILABLE = True
        _MATPLOTLIB_ERROR = None
        LOG.info(f"matplotlib available: version {matplotlib.__version__}, "
                 f"backend {matplotlib.get_backend()}")
    except Exception as exc:
        _MATPLOTLIB_AVAILABLE = False
        _MATPLOTLIB_ERROR = f"{type(exc).__name__}: {exc}"
        LOG.warning(f"matplotlib unavailable: {_MATPLOTLIB_ERROR}")
    return (_MATPLOTLIB_AVAILABLE, _MATPLOTLIB_ERROR)


def gil_is_enabled() -> Optional[bool]:
    probe = getattr(sys, "_is_gil_enabled", None)
    if callable(probe):
        try:
            return bool(probe())
        except Exception:
            return None
    return None


# ============================================================================
# Query loading
# ============================================================================
_RE_QUERY_NR = re.compile(r"^query_nr\s*=\s*(\d+)")
_RE_QUERY_EQ = re.compile(r"^\s*query\s*=\s*(.*)")


def parse_sql_file(sql_path: Path, n_queries: int) -> List[str]:
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    content = sql_path.read_text(encoding="utf-8")
    queries: Dict[int, str] = {}
    current_nr: Optional[int] = None
    current_lines: List[str] = []
    in_query = False

    def flush() -> None:
        nonlocal current_nr, current_lines
        if current_nr is not None and current_lines:
            queries[current_nr] = "\n".join(current_lines)

    for line in content.splitlines():
        stripped = line.rstrip()
        text = stripped.strip()

        nr_match = _RE_QUERY_NR.match(stripped)
        if nr_match:
            flush()
            current_nr = int(nr_match.group(1))
            current_lines = []
            in_query = False
            if len(queries) >= n_queries:
                break
            continue

        if current_nr is None:
            continue

        if not in_query:
            eq_match = _RE_QUERY_EQ.match(stripped)
            if eq_match:
                in_query = True
                after_eq = eq_match.group(1).strip()
                if after_eq:
                    current_lines.append(after_eq)
            continue

        if text and not text.startswith("--"):
            current_lines.append(text)

    flush()

    missing = [i for i in range(1, n_queries + 1) if i not in queries]
    if missing:
        head = missing[:10]
        raise RuntimeError(f"Missing queries: {head}{'...' if len(missing) > 10 else ''}")

    return [queries[i] for i in range(1, n_queries + 1)]


def fetch_queries_via_tpcds_queries(n_queries: int) -> List[str]:
    con = duckdb.connect(":memory:")
    try:
        try:
            con.execute("INSTALL tpcds; LOAD tpcds;")
        except Exception:
            con.execute("LOAD tpcds;")

        rows = con.execute(
            "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
        ).fetchall()
        queries = [text for _, text in rows]
        if len(queries) < n_queries:
            raise RuntimeError(f"Only fetched {len(queries)} queries")
        return queries[:n_queries]
    finally:
        con.close()


def _cache_payload(queries: Sequence[str]) -> str:
    return "\n;\n".join(queries)


def write_cache(cache_path: Path, queries: Sequence[str]) -> str:
    payload = _cache_payload(queries)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(payload, encoding="utf-8")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_or_fetch_queries(cfg: Config) -> Tuple[List[str], str]:
    cache_path = cfg.cache_queries
    sql_path = cfg.sql_file_path
    n_queries = cfg.n_queries

    # The SQL file is the source of truth. Re-parse it whenever it is newer than
    # the cache, so results never describe a stale query set.
    sql_is_newer = sql_path.is_file() and (
        not cache_path.is_file()
        or sql_path.stat().st_mtime > cache_path.stat().st_mtime
    )
    if sql_is_newer:
        queries = parse_sql_file(sql_path, n_queries)
        digest = write_cache(cache_path, queries)
        LOG.info(f"Query set parsed from {sql_path} (cache refreshed)")
        return queries, digest

    if cache_path.is_file():
        raw = cache_path.read_text(encoding="utf-8")
        queries = [q.strip() for q in raw.split("\n;\n") if q.strip()]
        if len(queries) >= n_queries:
            queries = queries[:n_queries]
            digest = hashlib.sha256(_cache_payload(queries).encode("utf-8")).hexdigest()
            LOG.info(f"Query set loaded from cache {cache_path}")
            return queries, digest
        LOG.warning(f"Cache holds {len(queries)} queries, need {n_queries}; regenerating")

    try:
        queries = fetch_queries_via_tpcds_queries(n_queries)
        digest = write_cache(cache_path, queries)
        LOG.info("Query set generated via tpcds_queries()")
        return queries, digest
    except Exception as exc:
        raise RuntimeError(f"All query loading methods failed: {exc}") from exc


# ============================================================================
# Connections
# ============================================================================
def _set_if_supported(con: duckdb.DuckDBPyConnection, statement: str) -> None:
    try:
        con.execute(statement)
    except Exception as exc:
        LOG.warning(f"Setting not applied: {statement} ({type(exc).__name__}: {exc})")


def _apply_session_settings(con: duckdb.DuckDBPyConnection, cfg: Config, threads: int) -> None:
    if threads > 0:
        _set_if_supported(con, f"SET threads TO {threads};")
    if cfg.memory_limit:
        _set_if_supported(con, f"SET memory_limit = '{cfg.memory_limit}';")
    flag = "true" if cfg.preserve_insertion_order else "false"
    _set_if_supported(con, f"SET preserve_insertion_order = {flag};")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def get_connection(cfg: Config, mode: str, *, threads: int = 0) -> duckdb.DuckDBPyConnection:
    if mode == "local_inprocess":
        if not cfg.db_path.is_file():
            raise FileNotFoundError(f"Database file not found: {cfg.db_path}")
        con = duckdb.connect(str(cfg.db_path), read_only=True)
        _apply_session_settings(con, cfg, threads)
        return con

    if mode in QUACK_MODES:
        con = duckdb.connect(":memory:")
        try:
            try:
                con.execute("INSTALL quack; LOAD quack;")
            except Exception:
                con.execute("LOAD quack;")

            quack_uri = f"quack:{cfg.quack_host}:{cfg.quack_port}"

            if cfg.quack_token:
                con.execute(
                    f"CREATE OR REPLACE SECRET (TYPE quack, TOKEN {_sql_literal(cfg.quack_token)}, "
                    f"SCOPE {_sql_literal(quack_uri)})"
                )

            if mode == "remote_quack":
                con.execute(f"ATTACH '{quack_uri}' AS quack_remote (DISABLE_SSL true);")
            else:
                con.execute(f"ATTACH '{quack_uri}' AS quack_remote;")
            con.execute("CONNECT quack_remote;")
            _apply_session_settings(con, cfg, threads)
            return con
        except Exception:
            con.close()
            raise

    raise ValueError(f"Unknown mode: {mode}")


def settle_result(con: duckdb.DuckDBPyConnection) -> None:
    for name in ("complete", "abort"):
        fn = getattr(con, name, None)
        if callable(fn):
            try:
                fn()
            except Exception:
                pass


def close_connection(con: Optional[duckdb.DuckDBPyConnection]) -> None:
    if con is None:
        return
    settle_result(con)
    try:
        try:
            con.execute("DISCONNECT;")
        except Exception:
            pass
        con.close()
    except Exception:
        pass


def probe_server(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ============================================================================
# Query execution
# ============================================================================
@dataclass(slots=True)
class QueryOutcome:
    query_nr: int
    mode: str
    duration_sec: float
    row_count: int
    success: bool
    error: Optional[str] = None
    error_class: Optional[str] = None
    worker_id: Optional[int] = None
    repeat: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "query_nr": self.query_nr,
            "mode": self.mode,
            "duration_sec": self.duration_sec,
            "row_count": self.row_count,
            "success": self.success,
            "error": self.error,
            "error_class": self.error_class,
            "worker_id": self.worker_id,
            "repeat": self.repeat,
        }


def _consume_result(cur: Any, cfg: Config) -> int:
    if cfg.result_mode == "arrow":
        # Columnar result; no per-row Python objects are created.
        return cur.fetch_arrow_table().num_rows
    if cfg.result_mode == "drain":
        total = 0
        while True:
            chunk = cur.fetchmany(cfg.fetch_chunk_rows)
            if not chunk:
                break
            total += len(chunk)
        return total
    return len(cur.fetchall())


_ERROR_CLASSIFICATION = {
    "Binder": "BINDER",
    "Catalog": "CATALOG",
    "Parser": "PARSER",
    "Syntax": "PARSER",
    "IO": "IO",
    "OutOfMemory": "OOM",
    "Memory": "OOM",
    "InvalidInput": "INVALID_INPUT",
    "Permission": "PERMISSION",
    "NotImplemented": "NOT_IMPLEMENTED",
}


def _classify_error(exc: BaseException) -> str:
    name = type(exc).__name__
    for key, value in _ERROR_CLASSIFICATION.items():
        if key in name:
            return value
    return name.upper()


def execute_query(con: duckdb.DuckDBPyConnection, query: str, query_nr: int, *,
                  mode: str, cfg: Config, worker_id: Optional[int] = None,
                  repeat: Optional[int] = None) -> QueryOutcome:
    start = time.perf_counter()
    try:
        cur = con.execute(query)
        row_count = _consume_result(cur, cfg)
        duration = time.perf_counter() - start
        return QueryOutcome(query_nr, mode, round(duration, 4), row_count, True,
                            None, None, worker_id, repeat)
    except Exception as exc:
        duration = time.perf_counter() - start
        settle_result(con)
        return QueryOutcome(query_nr, mode, round(duration, 4), 0, False,
                            str(exc)[:500], _classify_error(exc), worker_id, repeat)


def warmup_database(con: duckdb.DuckDBPyConnection, queries: Sequence[str],
                    n: int, cfg: Config, mode: str) -> int:
    if n <= 0:
        return 0
    count = min(n, len(queries))
    for i in range(count):
        outcome = execute_query(con, queries[i], i + 1, mode=mode, cfg=cfg)
        if not outcome.success:
            LOG.warning(f"Warmup query {i + 1} failed: {outcome.error_class}: "
                f"{(outcome.error or '')[:200]}")
    return count


# ============================================================================
# Sequential execution (warm repeats: one connection for all repeats)
# ============================================================================
def run_queries_sequential(cfg: Config, queries: Sequence[str],
                           mode: str, n_repeat: int) -> List[List[QueryOutcome]]:
    total_queries = len(queries)
    all_repeats: List[List[QueryOutcome]] = []

    con = get_connection(cfg, mode, threads=cfg.query_threads)
    try:
        warmup_database(con, queries, cfg.warmup_queries, cfg, mode)

        for repeat in range(1, n_repeat + 1):
            outcomes: List[QueryOutcome] = []
            for i, query in enumerate(queries, 1):
                outcome = execute_query(con, query, i, mode=mode, cfg=cfg, repeat=repeat)
                if not outcome.success:
                    LOG.error(f"Q{i} [{outcome.error_class}] {(outcome.error or '')[:300]}")
                outcomes.append(outcome)
                if i % 10 == 0 or i == total_queries:
                    mark = "OK  " if outcome.success else "FAIL"
                    LOG.info(f"Repeat {repeat} Query {i:3d}/{total_queries} {mark} "
                             f"{outcome.duration_sec:8.3f}s")
            all_repeats.append(outcomes)
    finally:
        close_connection(con)

    return all_repeats


# ============================================================================
# Concurrent execution (warm repeats: one connection per worker for all repeats)
# ============================================================================
def split_indices(n_items: int, n_workers: int) -> List[List[int]]:
    n_workers = max(1, min(n_workers, n_items))
    base, extra = divmod(n_items, n_workers)
    blocks: List[List[int]] = []
    start = 1
    for w in range(n_workers):
        size = base + (1 if w < extra else 0)
        blocks.append(list(range(start, start + size)))
        start += size
    return blocks


def run_concurrent_benchmark(cfg: Config, queries: Sequence[str], mode: str,
                             n_workers: int, n_repeat: int) -> List[List[QueryOutcome]]:
    blocks = split_indices(len(queries), n_workers)

    per_repeat: List[List[QueryOutcome]] = [[] for _ in range(n_repeat)]
    lock = threading.Lock()
    barrier = threading.Barrier(len(blocks))
    failures: List[str] = []

    def worker(worker_id: int, indices: List[int]) -> None:
        con: Optional[duckdb.DuckDBPyConnection] = None
        local: List[QueryOutcome] = []
        try:
            con = get_connection(cfg, mode, threads=cfg.worker_threads)

            warmup_list = [queries[i - 1] for i in indices]
            warmup_database(con, warmup_list, min(cfg.warmup_queries, len(indices)), cfg, mode)

            for repeat in range(1, n_repeat + 1):
                barrier.wait(timeout=cfg.barrier_timeout)
                for nr in indices:
                    local.append(execute_query(con, queries[nr - 1], nr, mode=mode,
                                               cfg=cfg, worker_id=worker_id, repeat=repeat))
        except threading.BrokenBarrierError:
            with lock:
                failures.append(f"Worker {worker_id}: barrier broken")
        except Exception as exc:
            with lock:
                failures.append(f"Worker {worker_id}: {type(exc).__name__}: {exc}")
            try:
                barrier.abort()
            except Exception:
                pass
        finally:
            close_connection(con)
            with lock:
                for outcome in local:
                    idx = (outcome.repeat or 1) - 1
                    if 0 <= idx < len(per_repeat):
                        per_repeat[idx].append(outcome)

    threads = [threading.Thread(target=worker, args=(wid, block), name=f"bench-worker-{wid}")
               for wid, block in enumerate(blocks)]

    started = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - started

    for msg in failures:
        LOG.error(f"[ERROR] {msg}")

    total = sum(len(r) for r in per_repeat)
    LOG.info(f"Concurrent run finished in {wall:.2f}s, {total} executions recorded")
    return per_repeat


# ============================================================================
# Statistics
# ============================================================================
def _flatten(repeats: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    """Iterative flatten of nested outcome lists into dicts (no recursion)."""
    out: List[Dict[str, Any]] = []
    stack: List[Any] = list(repeats)
    while stack:
        item = stack.pop()
        if isinstance(item, QueryOutcome):
            out.append(item.as_dict())
        elif isinstance(item, dict):
            out.append(item)
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
    return out


def compute_stats(results: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]

    if not successful:
        return {
            "total_queries": len(results),
            "successful": 0,
            "failed": len(failed),
            "success_rate": 0.0,
            "error": "No successful queries",
        }

    durations = [float(r["duration_sec"]) for r in successful]

    if np is not None:
        arr = np.asarray(durations, dtype=np.float64)
        mean = float(arr.mean())
        median = float(np.median(arr))
        std = float(arr.std(ddof=1)) if arr.size > 1 else 0.0
        lo, hi = float(arr.min()), float(arr.max())
        total = float(arr.sum())
        p95 = float(np.percentile(arr, 95))
    else:
        mean = statistics.fmean(durations)
        median = statistics.median(durations)
        std = statistics.stdev(durations) if len(durations) > 1 else 0.0
        lo, hi = min(durations), max(durations)
        total = sum(durations)
        ordered = sorted(durations)
        p95 = ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]

    return {
        "total_queries": len(results),
        "successful": len(successful),
        "failed": len(failed),
        "success_rate": round(len(successful) / len(results) * 100, 2),
        "total_time_sec": round(total, 4),
        "mean_duration": round(mean, 4),
        "median_duration": round(median, 4),
        "p95_duration": round(p95, 4),
        "std_dev": round(std, 4),
        "min_duration": round(lo, 4),
        "max_duration": round(hi, 4),
        "throughput_qps": round(len(successful) / total, 2) if total > 0 else 0.0,
    }


# ============================================================================
# Reporting
# ============================================================================
def environment_fingerprint(cfg: Config, query_digest: str) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "python": sys.version.split()[0],
        "gil_enabled": gil_is_enabled(),
        "duckdb": getattr(duckdb, "__version__", "unknown"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "physical_cores": cfg.physical_cores,
        "query_set_sha256": query_digest,
        "repeat_mode": "warm",
        "config": cfg.snapshot(),
    }


def save_results(outcomes: Sequence[QueryOutcome], cfg: Config, mode: str,
                 repeat_idx: int, meta: Dict[str, Any],
                 run_label: str = "") -> Path:
    cfg.results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{run_label}" if run_label else ""
    filepath = cfg.results_dir / f"tpcds_{mode}_r{repeat_idx}{suffix}_{stamp}.json"

    payload = {
        "metadata": {**meta, "mode": mode, "repeat": repeat_idx,
                     "stats": compute_stats([o.as_dict() for o in outcomes])},
        "results": [o.as_dict() for o in outcomes],
    }
    filepath.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    LOG.info(f"Results saved to {filepath}")
    return filepath


_CSV_COLUMNS = ("run_label", "timestamp", "mode", "repeat", "worker_id",
                "query_nr", "duration_sec", "row_count", "success",
                "error_class", "error")


def export_runs_csv(cfg: Config, all_results: Dict[str, List[List[QueryOutcome]]],
                    meta: Dict[str, Any], run_label: str = "") -> Optional[Path]:
    """Append every execution as one CSV row for spreadsheet/pivot analysis."""
    csv_path = cfg.results_dir / "tpcds_runs.csv"
    try:
        cfg.results_dir.mkdir(parents=True, exist_ok=True)
        new_file = not csv_path.is_file()
        with csv_path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
            if new_file:
                writer.writeheader()
            for mode, repeats in all_results.items():
                for outcomes in repeats:
                    for o in outcomes:
                        writer.writerow({
                            "run_label": run_label,
                            "timestamp": meta["timestamp"],
                            "mode": o.mode,
                            "repeat": o.repeat,
                            "worker_id": o.worker_id if o.worker_id is not None else 0,
                            "query_nr": o.query_nr,
                            "duration_sec": o.duration_sec,
                            "row_count": o.row_count,
                            "success": o.success,
                            "error_class": o.error_class or "",
                            "error": (o.error or "").replace("\r", " ").replace("\n", " "),
                        })
        LOG.info(f"CSV export appended to {csv_path}")
        return csv_path
    except OSError as exc:
        LOG.warning(f"CSV export failed: {exc}")
        return None


def generate_summary_report(cfg: Config, all_results: Dict[str, List[List[QueryOutcome]]],
                            meta: Dict[str, Any]) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    lines: List[str] = []
    lines.append("=" * 78)
    lines.append("TPC-DS Benchmark Summary Report")
    lines.append("=" * 78)
    lines.append(f"Timestamp:      {meta['timestamp']}")
    lines.append(f"Platform:       {meta['platform']}  ({meta['machine']}, "
                 f"{meta['physical_cores']} physical / {meta['logical_cpus']} logical CPUs)")
    lines.append(f"Python:         {meta['python']}  (GIL enabled: {meta['gil_enabled']})")
    lines.append(f"DuckDB:         {meta['duckdb']}")
    lines.append("Scale factor:   SF=10 (~10 GB)")
    lines.append(f"Database:       {cfg.db_path}")
    lines.append(f"Query set hash: {meta['query_set_sha256'][:32]}")
    lines.append(f"Queries/repeat: {cfg.n_queries}   Repeats: {cfg.n_repeat}   "
                 f"Workers: {cfg.n_workers}   Warmup: {cfg.warmup_queries}")
    lines.append("Repeat mode:    warm (connection kept open across repeats)")
    lines.append(f"Threads:        sequential={cfg.query_threads}  worker={cfg.worker_threads}  "
                 f"memory_limit={cfg.memory_limit or 'engine default'}")
    lines.append(f"Result mode:    {cfg.result_mode}")

    per_mode_stats: Dict[str, Dict[str, Any]] = {}

    for mode, repeats in all_results.items():
        flat = _flatten(repeats)
        stats = compute_stats(flat)
        per_mode_stats[mode] = stats

        lines.append("")
        lines.append("-" * 78)
        lines.append(f"[{mode.upper().replace('_', ' ')}]")
        lines.append(f"  Executions:   {stats.get('total_queries', 0)}")
        lines.append(f"  Success rate: {stats.get('success_rate', 0):.1f}%")
        lines.append(f"  Mean:         {stats.get('mean_duration', 0):.4f}s")
        lines.append(f"  Median:       {stats.get('median_duration', 0):.4f}s")
        lines.append(f"  P95:          {stats.get('p95_duration', 0):.4f}s")
        lines.append(f"  Min / Max:    {stats.get('min_duration', 0):.4f}s / "
                     f"{stats.get('max_duration', 0):.4f}s")
        lines.append(f"  Std dev:      {stats.get('std_dev', 0):.4f}s")
        lines.append(f"  Throughput:   {stats.get('throughput_qps', 0):.2f} QPS")
        lines.append(f"  Total time:   {stats.get('total_time_sec', 0):.2f}s")

        slowest = sorted([r for r in flat if r.get("success")],
                         key=lambda r: r["duration_sec"], reverse=True)[:5]
        if slowest:
            lines.append("  Slowest queries:")
            for row in slowest:
                lines.append(f"    Q{row['query_nr']:3d}: {row['duration_sec']:8.3f}s "
                             f"({row['row_count']} rows)")

        bad = [r for r in flat if not r.get("success")]
        if bad:
            lines.append(f"  Failures ({len(bad)}):")
            for row in bad[:10]:
                err_msg = str(row.get("error", ""))[:110]
                lines.append(f"    Q{row['query_nr']:3d}: [{row.get('error_class')}] {err_msg}")

    baseline = per_mode_stats.get("local_inprocess")
    if baseline and baseline.get("successful"):
        lines.append("")
        lines.append("-" * 78)
        lines.append("Relative to LOCAL INPROCESS (median query time):")
        base_median = baseline["median_duration"]
        for mode, stats in per_mode_stats.items():
            if mode == "local_inprocess" or not stats.get("successful"):
                continue
            ratio = stats["median_duration"] / base_median if base_median else float("nan")
            verdict = "faster" if ratio < 1 else "slower"
            lines.append(f"  {mode:<18} {ratio:6.2f}x  ({verdict})")

    lines.append("")
    lines.append("=" * 78)

    report = "\n".join(lines)
    cfg.results_dir.mkdir(parents=True, exist_ok=True)
    filepath = cfg.results_dir / f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    filepath.write_text(report, encoding="utf-8")
    LOG.info(f"Summary report saved to {filepath}")
    return report, per_mode_stats


def plot_comparison(cfg: Config, modes: Sequence[str],
                    per_mode_stats: Dict[str, Dict[str, Any]],
                    run_label: str = "") -> Optional[Path]:
    available, mpl_error = check_matplotlib()
    if not available:
        LOG.error(f"PLOTS SKIPPED: matplotlib not available ({mpl_error})")
        LOG.info("Install with: pip install matplotlib")
        return None

    modes = list(modes)
    if not modes:
        LOG.error("PLOTS SKIPPED: no results to plot")
        return None

    LOG.info(f"Generating comparison chart for {len(modes)} mode(s): {', '.join(modes)}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        plt.rcParams.update({
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.labelsize": 10,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "figure.dpi": 100,
        })
        # Write text as SVG <text> elements rather than glyph outlines, so the
        # SVG path never invokes FreeType rasterization.
        matplotlib.rcParams["svg.fonttype"] = "none"
    except Exception as exc:
        LOG.error(f"PLOTS SKIPPED: matplotlib import/config failed: {exc}")
        LOG.debug(traceback.format_exc())
        return None

    stats = per_mode_stats
    for m in modes:
        if stats.get(m, {}).get("total_queries", 0) == 0:
            LOG.warning(f"Mode '{m}' has no executions; chart shows zero values")

    means = [stats.get(m, {}).get("mean_duration", 0) for m in modes]
    medians = [stats.get(m, {}).get("median_duration", 0) for m in modes]
    qps = [stats.get(m, {}).get("throughput_qps", 0) for m in modes]
    labels = [m.upper().replace("_", " ") for m in modes]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    positions = list(range(len(modes)))
    width = 0.38

    bars1 = ax1.bar([p - width / 2 for p in positions], means, width, label="Mean")
    bars2 = ax1.bar([p + width / 2 for p in positions], medians, width, label="Median")
    ax1.set_ylabel("Seconds")
    ax1.set_title("Latency by mode")
    ax1.set_xticks(positions)
    ax1.set_xticklabels(labels)
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)
    for bar in list(bars1) + list(bars2):
        height = bar.get_height()
        ax1.annotate(f"{height:.2f}s", (bar.get_x() + bar.get_width() / 2, height),
                     ha="center", va="bottom", fontsize=7)

    bars3 = ax2.bar(positions, qps, alpha=0.85)
    ax2.set_ylabel("QPS")
    ax2.set_title("Throughput by mode")
    ax2.set_xticks(positions)
    ax2.set_xticklabels(labels)
    ax2.grid(axis="y", alpha=0.3)
    for bar in bars3:
        height = bar.get_height()
        ax2.annotate(f"{height:.2f}", (bar.get_x() + bar.get_width() / 2, height),
                     ha="center", va="bottom", fontsize=8)

    fig.tight_layout(pad=1.0)

    cfg.results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{run_label}" if run_label else ""
    out_stem = cfg.results_dir / f"comparison{suffix}_{stamp}"

    preferred = cfg.chart_format if cfg.chart_format in CHART_FORMATS else "svg"
    order = [preferred] + [f for f in CHART_FORMATS if f != preferred]

    out_path: Optional[Path] = None
    for fmt in order:
        candidate = out_stem.with_suffix(f".{fmt}")
        try:
            if fmt == "png":
                fig.savefig(candidate, dpi=100, bbox_inches="tight")
            else:
                fig.savefig(candidate, bbox_inches="tight")
            if candidate.exists() and candidate.stat().st_size > 0:
                out_path = candidate
                LOG.info(f"Saved {fmt.upper()}: {out_path} ({out_path.stat().st_size:,} bytes)")
                break
        except Exception as exc:
            LOG.warning(f"{fmt.upper()} savefig failed: {type(exc).__name__}: {exc}")

    plt.close(fig)

    if out_path is None:
        LOG.error("PLOTS FAILED: neither SVG nor PNG could be written")
        return None

    LOG.info(f"Comparison chart saved to {out_path}")
    return out_path


# ============================================================================
# Smoke test
# ============================================================================
def smoke_test(cfg: Config) -> bool:
    LOG.info("=" * 74)
    LOG.info("SMOKE TEST")
    LOG.info("=" * 74)

    LOG.info(f"Python {sys.version.split()[0]} on {platform.platform()}")
    LOG.info(f"DuckDB {getattr(duckdb, '__version__', 'unknown')}")

    if gil_is_enabled() is False:
        LOG.error("Free-threaded CPython detected; this harness expects the standard GIL build.")
        return False

    ok = True

    try:
        queries, digest = load_or_fetch_queries(cfg)
        LOG.info(f"[OK]   query loading: {len(queries)} queries (sha256 {digest[:16]})")
    except Exception as exc:
        LOG.error(f"[FAIL] query loading: {type(exc).__name__}: {exc}")
        return False

    try:
        con = get_connection(cfg, "local_inprocess")
        try:
            rows = con.execute("SELECT COUNT(*) FROM store_sales").fetchall()
            LOG.info(f"[OK]   local in-process connection (store_sales rows: {rows[0][0]:,})")
        finally:
            close_connection(con)
    except Exception as exc:
        LOG.error(f"[FAIL] local in-process connection: {type(exc).__name__}: {str(exc)[:160]}")
        ok = False

    if not cfg.quack_token:
        LOG.warning("No QUACK_TOKEN configured; skipping Quack modes")
    else:
        if probe_server(cfg.quack_host, cfg.quack_port):
            LOG.info(f"[OK]   TCP reachable: {cfg.quack_host}:{cfg.quack_port}")
        else:
            LOG.error(f"[FAIL] no TCP listener on {cfg.quack_host}:{cfg.quack_port}")
            ok = False

        # Only attempt handshakes if the server is reachable at all.
        if ok:
            for mode in QUACK_MODES:
                try:
                    con = get_connection(cfg, mode)
                    try:
                        con.execute("SELECT 42").fetchall()
                    finally:
                        close_connection(con)
                    LOG.info(f"[OK]   {mode.replace('_', ' ')} handshake")
                except Exception as exc:
                    LOG.error(f"[FAIL] {mode.replace('_', ' ')}: "
                              f"{type(exc).__name__}: {str(exc)[:160]}")
                    ok = False

    LOG.info("=" * 74)
    LOG.info("Smoke test complete" if ok else "Smoke test reported problems")
    LOG.info("=" * 74)
    return ok


# ============================================================================
# Main
# ============================================================================
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TPC-DS benchmark harness for DuckDB Quack (Windows 11 / DuckDB 2.0)"
    )
    parser.add_argument("--smoke", action="store_true", help="run smoke test only")
    parser.add_argument("--validate-only", action="store_true",
                        help="validate configuration and exit")
    parser.add_argument("--mode", choices=VALID_MODES, help="single execution mode")
    parser.add_argument("--modes", help="comma-separated list of modes to compare")
    parser.add_argument("--workers", type=int, help="concurrent workers")
    parser.add_argument("--repeat", type=int, help="repetitions")
    parser.add_argument("--queries", type=int, help="number of queries (1..99)")
    parser.add_argument("--no-warmup", action="store_true", help="skip warmup")
    parser.add_argument("--no-plots", action="store_true", help="skip chart generation")
    parser.add_argument("--tag", default="", metavar="LABEL",
                        help="label used in all output filenames of this run")
    return parser.parse_args(argv)


def resolve_modes(args: argparse.Namespace) -> List[str]:
    if args.modes is not None:
        requested = [m.strip().lower() for m in args.modes.split(",") if m.strip()]
        unknown = [m for m in requested if m not in VALID_MODES]
        if unknown:
            raise SystemExit(f"Unknown mode(s): {', '.join(unknown)}")
        if not requested:
            raise SystemExit("--modes was given but no valid mode was specified")
        return requested
    if args.mode:
        return [args.mode]
    return [CONFIG.mode]


def main(argv: Optional[Sequence[str]] = None) -> int:
    configure_console()

    args = parse_args(argv)

    if args.repeat is not None:
        CONFIG.n_repeat = args.repeat
    if args.workers is not None:
        CONFIG.n_workers = args.workers
    if args.queries is not None:
        CONFIG.n_queries = args.queries
    if args.no_warmup:
        CONFIG.warmup_queries = 0

    modes = resolve_modes(args)
    run_label = args.tag.strip()

    # Deferred matplotlib check: skip the import entirely when charts
    # cannot be produced anyway.
    mat_available = True
    if not args.no_plots and not args.validate_only and not args.smoke:
        mat_available, _ = check_matplotlib()
        if not mat_available:
            LOG.warning("matplotlib unavailable - charts will be disabled")

    LOG.info("=" * 74)
    LOG.info("TPC-DS Quack Benchmark Harness")
    LOG.info("=" * 74)
    LOG.info(f"Modes:          {', '.join(modes)}")
    LOG.info(f"Repeats:        {CONFIG.n_repeat} (warm)")
    LOG.info(f"Workers:        {CONFIG.n_workers}")
    LOG.info(f"Queries:        {CONFIG.n_queries}")
    LOG.info(f"Warmup:         {CONFIG.warmup_queries}")
    LOG.info(f"Physical cores: {CONFIG.physical_cores}")
    LOG.info(f"Result mode:    {CONFIG.result_mode}")
    LOG.info(f"Chart format:   {CONFIG.chart_format}")
    LOG.info(f"Database:       {CONFIG.db_path}")
    LOG.info(f"Quack server:   {CONFIG.quack_host}:{CONFIG.quack_port}")
    LOG.info(f"Results dir:    {CONFIG.results_dir}")
    if run_label:
        LOG.info(f"Run label:      {run_label}")
    LOG.info(f"Plots enabled:  {'Yes' if not args.no_plots and mat_available else 'No'}")
    print()

    errors = report_issues(validate_config(CONFIG, modes))
    if errors:
        LOG.error(f"Aborting: {errors} configuration error(s)")
        return 1
    if args.validate_only:
        LOG.info("Configuration valid")
        return 0

    if not smoke_test(CONFIG):
        LOG.warning("Continuing despite smoke-test problems")

    if args.smoke:
        return 0

    queries, digest = load_or_fetch_queries(CONFIG)
    meta = environment_fingerprint(CONFIG, digest)

    all_results: Dict[str, List[List[QueryOutcome]]] = {}

    for mode in modes:
        if CONFIG.n_workers > 1:
            repeats = run_concurrent_benchmark(CONFIG, queries, mode,
                                               CONFIG.n_workers, CONFIG.n_repeat)
        else:
            repeats = run_queries_sequential(CONFIG, queries, mode, CONFIG.n_repeat)

        all_results[mode] = repeats

        for idx, outcomes in enumerate(repeats, 1):
            if outcomes:
                save_results(outcomes, CONFIG, mode, idx, meta, run_label)

    export_runs_csv(CONFIG, all_results, meta, run_label)

    report, per_mode_stats = generate_summary_report(CONFIG, all_results, meta)
    print()
    print(report)

    if not args.no_plots:
        if plot_comparison(CONFIG, modes, per_mode_stats, run_label) is None:
            LOG.info("Charts were not generated. Run with --help for options.")
    else:
        LOG.info("Plot generation disabled by --no-plots flag")

    LOG.info("Benchmark complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())