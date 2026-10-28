# tpcds_quack_bench.py
"""
TPC-DS Benchmark Harness for DuckDB Quack (v2 Final)
Target: Windows 11 / DuckDB 2.x / CPython 3.14 (GIL build — NOT 3.14t)
Note: Uses SVG output with fallback to work around matplotlib/FreeType issues in 3.14

Execution modes:
  1. LOCAL_INPROCESS - in-process DuckDB against the database file (read-only)
  2. LOCAL_QUACK     - Quack server on localhost
  3. REMOTE_QUACK    - Quack server on a remote host

Usage:
  python tpcds_quack_bench.py [--smoke] [--modes M1,M2,...] [--workers N]
                              [--repeat N] [--queries N] [--no-warmup]
                              [--no-pin] [--validate-only]

Environment (.env or OS):
  DB_PATH            TPC-DS database file      (default H:\\tpcds\\tpcds_sf10.db)
  CACHE_QUERIES      cached queries file       (default H:\\tpcds\\queries_cached.txt)
  SQL_FILE_PATH      generated SQL file        (default H:\\tpcds\\tpcds_queries.sql)
  QUACK_HOST         Quack server host         (default localhost)
  QUACK_PORT         Quack server port         (default 9494)
  QUACK_TOKEN        Quack auth token          (required for *_quack modes)
  RESULTS_DIR        output directory          (default H:\\tpcds\\results)
  N_QUERIES          number of TPC-DS queries  (default 99, max 99)
  N_WORKERS          concurrent workers        (default 1)
  N_REPEAT           repetitions               (default 3)
  WARMUP_QUERIES     warmup queries per conn   (default 5)
  MODE               default execution mode    (default local_inprocess)
  QUERY_THREADS      DuckDB intra-query threads, 0 = engine default
  WORKER_THREADS     DuckDB threads per concurrent worker (default 1)
  MEMORY_LIMIT       e.g. '8GB'; empty = engine default
  PIN_CORES          true/false, pin worker threads to distinct logical CPUs
  RESULT_MODE        full | drain   (how query results are consumed)
  FETCH_CHUNK_ROWS   rows per fetchmany() when RESULT_MODE=drain (default 65536)
  BARRIER_TIMEOUT    seconds to wait for synchronised worker start (default 600)
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import logging
import mmap
import os
import platform
import re
import socket
import statistics
import sys
import threading
import time
import traceback
from dataclasses import dataclass, asdict
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

try:
    import psutil
except ImportError:
    psutil = None


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
_LOG_HANDLER = None
_LOG_INSTANCE = None


def _get_logger() -> logging.Logger:
    global _LOG_INSTANCE, _LOG_HANDLER
    if _LOG_INSTANCE is not None:
        return _LOG_INSTANCE
    
    _LOG_INSTANCE = logging.getLogger("tpcds_bench")
    if _LOG_INSTANCE.handlers:
        return _LOG_INSTANCE
    
    _LOG_HANDLER = logging.StreamHandler(sys.stdout)
    _LOG_HANDLER.setFormatter(_LOG_FORMATTER)
    _LOG_INSTANCE.addHandler(_LOG_HANDLER)
    _LOG_INSTANCE.setLevel(logging.INFO)
    _LOG_INSTANCE.propagate = False
    return _LOG_INSTANCE


LOG = _get_logger()


# ============================================================================
# Configuration
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent
MMAP_THRESHOLD_BYTES = 10 * 1024 * 1024


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
    query_threads: int
    worker_threads: int
    memory_limit: str
    pin_cores: bool
    result_mode: str
    fetch_chunk_rows: int
    barrier_timeout: float

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
            "query_threads": self.query_threads,
            "worker_threads": self.worker_threads,
            "memory_limit": self.memory_limit,
            "pin_cores": self.pin_cores,
            "result_mode": self.result_mode,
            "fetch_chunk_rows": self.fetch_chunk_rows,
            "barrier_timeout": self.barrier_timeout,
        }


def _env_path(name: str, default: str) -> Path:
    raw = os.getenv(name, default)
    return Path(os.path.expandvars(raw)).expanduser()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_config() -> Config:
    dotenv_path = os.getenv("DOTENV_PATH")
    if dotenv_path:
        load_dotenv(dotenv_path, override=True)
    else:
        load_dotenv(BASE_DIR / ".env", override=True)

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
        query_threads=int(os.getenv("QUERY_THREADS", "0")),
        worker_threads=max(1, int(os.getenv("WORKER_THREADS", "1"))),
        memory_limit=os.getenv("MEMORY_LIMIT", "").strip(),
        pin_cores=_env_bool("PIN_CORES", True),
        result_mode=os.getenv("RESULT_MODE", "full").strip().lower(),
        fetch_chunk_rows=max(1, int(os.getenv("FETCH_CHUNK_ROWS", "65536"))),
        barrier_timeout=float(os.getenv("BARRIER_TIMEOUT", "600")),
    )


CONFIG = load_config()
VALID_MODES = ("local_inprocess", "local_quack", "remote_quack")


# ============================================================================
# Issues and validation
# ============================================================================
@dataclass(slots=True)
class Issue:
    level: str
    message: str


def validate_config(cfg: Config) -> List[Issue]:
    issues: List[Issue] = []

    if cfg.mode not in VALID_MODES:
        issues.append(Issue("error", f"unknown mode '{cfg.mode}'"))
        return issues

    if not (1 <= cfg.n_queries <= 99):
        issues.append(Issue("error", f"N_QUERIES must be 1..99, got {cfg.n_queries}"))

    if cfg.n_repeat < 1:
        issues.append(Issue("error", f"N_REPEAT must be >= 1"))

    if cfg.n_workers < 1:
        issues.append(Issue("error", f"N_WORKERS must be >= 1"))

    if cfg.warmup_queries < 0:
        issues.append(Issue("error", f"WARMUP_QUERIES must be >= 0"))

    if not (1 <= cfg.quack_port <= 65535):
        issues.append(Issue("error", f"QUACK_PORT out of range: {cfg.quack_port}"))

    if cfg.result_mode not in ("full", "drain"):
        issues.append(Issue("error", f"RESULT_MODE must be 'full' or 'drain'"))

    if cfg.mode == "local_inprocess":
        if not cfg.db_path.is_file():
            issues.append(Issue("error", f"database file not found: {cfg.db_path}"))
    else:
        if not cfg.quack_token:
            issues.append(Issue("error", f"QUACK_TOKEN required for mode '{cfg.mode}'"))

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
# Matplotlib pre-check
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
        import matplotlib.pyplot
        _MATPLOTLIB_AVAILABLE = True
        _MATPLOTLIB_ERROR = None
        LOG.info(f"matplotlib available: version {matplotlib.__version__}, backend {matplotlib.get_backend()}")
        return (True, None)
    except Exception as exc:
        _MATPLOTLIB_AVAILABLE = False
        _MATPLOTLIB_ERROR = f"{type(exc).__name__}: {exc}"
        LOG.warning(f"matplotlib unavailable: {_MATPLOTLIB_ERROR}")
        return (False, _MATPLOTLIB_ERROR)


# ============================================================================
# Platform helpers
# ============================================================================
_KERNEL32 = None
if os.name == "nt":
    try:
        _KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
        _KERNEL32.GetCurrentThread.restype = ctypes.c_void_p
        _KERNEL32.SetThreadAffinityMask.restype = ctypes.c_size_t
        _KERNEL32.SetThreadAffinityMask.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    except OSError:
        _KERNEL32 = None


def allowed_cores() -> List[int]:
    if psutil is not None:
        try:
            cores = sorted(int(c) for c in psutil.Process().cpu_affinity())
            if cores:
                return cores
        except Exception:
            pass
    return list(range(os.cpu_count() or 1))


def pin_current_thread(core_index: int) -> bool:
    if _KERNEL32 is None:
        return False
    try:
        mask = ctypes.c_size_t(1 << core_index)
        handle = _KERNEL32.GetCurrentThread()
        previous = _KERNEL32.SetThreadAffinityMask(handle, mask)
        return bool(previous)
    except Exception:
        return False


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


def read_text_fast(path: Path) -> str:
    size = path.stat().st_size
    if size > MMAP_THRESHOLD_BYTES:
        with open(path, "rb") as fh:
            with mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                return mm.read().decode("utf-8")
    return path.read_text(encoding="utf-8")


def parse_sql_file(sql_path: Path, n_queries: int) -> List[str]:
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    content = read_text_fast(sql_path)
    queries: Dict[int, str] = {}
    current_nr: Optional[int] = None
    current_lines: List[str] = []
    in_query = False

    for line in content.splitlines():
        stripped = line.rstrip()

        nr_match = _RE_QUERY_NR.match(stripped)
        if nr_match:
            if current_nr is not None and current_lines:
                queries[current_nr] = "\n".join(current_lines)
                if len(queries) >= n_queries:
                    break
            current_nr = int(nr_match.group(1))
            current_lines = []
            in_query = False
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
            continue

        if stripped.strip() and not stripped.strip().startswith("--"):
            current_lines.append(stripped.strip())

    if current_nr is not None and current_lines and len(queries) < n_queries:
        queries[current_nr] = "\n".join(current_lines)

    missing = [i for i in range(1, n_queries + 1) if i not in queries]
    if missing:
        head = missing[:10]
        raise RuntimeError(f"Missing queries: {head}{'...' if len(missing) > 10 else ''}")

    result = [queries[i] for i in range(1, n_queries + 1)]
    return result


def fetch_queries_via_tpcds_queries() -> List[str]:
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

        if len(queries) < 99:
            raise RuntimeError(f"Only fetched {len(queries)} queries")

        return queries
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

    if cache_path.is_file():
        raw = read_text_fast(cache_path)
        queries = [q.strip() for q in raw.split("\n;\n") if q.strip()]
        if len(queries) >= n_queries:
            queries = queries[:n_queries]
            digest = hashlib.sha256(_cache_payload(queries).encode("utf-8")).hexdigest()
            return queries, digest

    if sql_path.is_file():
        queries = parse_sql_file(sql_path, n_queries)
        digest = write_cache(cache_path, queries)
        return queries, digest

    try:
        queries = fetch_queries_via_tpcds_queries()[:n_queries]
        digest = write_cache(cache_path, queries)
        return queries, digest
    except Exception as exc:
        raise RuntimeError(f"All query loading methods failed: {exc}") from exc


# ============================================================================
# Connections
# ============================================================================
def _set_if_supported(con: duckdb.DuckDBPyConnection, statement: str) -> None:
    try:
        con.execute(statement)
    except Exception:
        pass


def _apply_session_settings(con: duckdb.DuckDBPyConnection, cfg: Config, threads: int) -> None:
    if threads > 0:
        _set_if_supported(con, f"SET threads TO {threads};")
    if cfg.memory_limit:
        _set_if_supported(con, f"SET memory_limit = '{cfg.memory_limit}';")


def get_connection(cfg: Config, mode: str, *, threads: int = 0) -> duckdb.DuckDBPyConnection:
    if mode == "local_inprocess":
        if not cfg.db_path.is_file():
            raise FileNotFoundError(f"Database file not found: {cfg.db_path}")
        con = duckdb.connect(str(cfg.db_path), read_only=True)
        _apply_session_settings(con, cfg, threads)
        return con

    if mode in ("local_quack", "remote_quack"):
        con = duckdb.connect(":memory:")
        try:
            try:
                con.execute("INSTALL quack; LOAD quack;")
            except Exception:
                con.execute("LOAD quack;")

            quack_uri = f"quack:{cfg.quack_host}:{cfg.quack_port}"

            if cfg.quack_token:
                con.execute(
                    "CREATE SECRET (TYPE quack, TOKEN ?, SCOPE ?)",
                    [cfg.quack_token, quack_uri],
                )

            attach_stmt = f"ATTACH '{quack_uri}' AS quack_remote (DISABLE_SSL true);" if mode == "remote_quack" else f"ATTACH '{quack_uri}' AS quack_remote;"
            con.execute(attach_stmt)
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


_FETCHMANY_SUPPORTED: Optional[bool] = None


def _consume_full(cur: Any) -> int:
    return len(cur.fetchall())


def _consume_drain(cur: Any, chunk_rows: int) -> int:
    global _FETCHMANY_SUPPORTED
    if _FETCHMANY_SUPPORTED is False or not hasattr(cur, "fetchmany"):
        return _consume_full(cur)

    total = 0
    try:
        while True:
            chunk = cur.fetchmany(chunk_rows)
            if not chunk:
                break
            total += len(chunk)
        _FETCHMANY_SUPPORTED = True
        return total
    except AttributeError:
        _FETCHMANY_SUPPORTED = False
        return _consume_full(cur)


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
        if cfg.result_mode == "drain":
            row_count = _consume_drain(cur, cfg.fetch_chunk_rows)
        else:
            row_count = _consume_full(cur)
        duration = time.perf_counter() - start
        return QueryOutcome(query_nr, mode, round(duration, 4), row_count, True,
                            None, None, worker_id, repeat)
    except Exception as exc:
        duration = time.perf_counter() - start
        settle_result(con)
        error_msg = str(exc)[:500]
        error_class = _classify_error(exc)
        return QueryOutcome(query_nr, mode, round(duration, 4), 0, False,
                            error_msg, error_class, worker_id, repeat)


def warmup_database(con: duckdb.DuckDBPyConnection, queries: Sequence[str],
                    n: int, cfg: Config, mode: str) -> int:
    if n <= 0:
        return 0
    count = min(n, len(queries))
    for i in range(count):
        outcome = execute_query(con, queries[i], i + 1, mode=mode, cfg=cfg)
        if not outcome.success:
            LOG.warning(f"Warmup query {i + 1} failed: {outcome.error_class}")
    return count


# ============================================================================
# Sequential execution
# ============================================================================
def run_queries_sequential(cfg: Config, queries: Sequence[str],
                           mode: str, n_repeat: int) -> List[List[QueryOutcome]]:
    all_repeats: List[List[QueryOutcome]] = []
    total_queries = len(queries)

    for repeat in range(1, n_repeat + 1):
        con = get_connection(cfg, mode, threads=cfg.query_threads)
        outcomes: List[QueryOutcome] = []
        try:
            warmup_database(con, queries, cfg.warmup_queries, cfg, mode)
            
            for i, query in enumerate(queries, 1):
                outcome = execute_query(con, query, i, mode=mode, cfg=cfg, repeat=repeat)
                outcomes.append(outcome)
                
                if i % 10 == 0 or i == total_queries:
                    mark = "OK  " if outcome.success else "FAIL"
                    LOG.info(f"Query {i:3d}/{total_queries} {mark} {outcome.duration_sec:8.3f}s")
        finally:
            close_connection(con)

        all_repeats.append(outcomes)
        if repeat < n_repeat:
            time.sleep(1.0)

    return all_repeats


# ============================================================================
# Concurrent execution
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
    n_workers = len(blocks)
    cores = allowed_cores() if cfg.pin_cores else []

    per_repeat: List[List[QueryOutcome]] = [[] for _ in range(n_repeat)]
    lock = threading.Lock()
    barrier = threading.Barrier(n_workers)
    failures: List[str] = []

    def worker(worker_id: int, indices: List[int]) -> None:
        con: Optional[duckdb.DuckDBPyConnection] = None
        local: List[QueryOutcome] = []
        try:
            if cores:
                core = cores[worker_id % len(cores)]
                if pin_current_thread(core):
                    LOG.info(f"Worker {worker_id}: pinned to CPU {core}")
                else:
                    LOG.warning(f"Worker {worker_id}: thread pinning unavailable")

            con = get_connection(cfg, mode, threads=cfg.worker_threads)
            
            warmup_queries_list = [queries[i - 1] for i in indices]
            warmup_count = min(cfg.warmup_queries, len(indices))
            warmup_database(con, warmup_queries_list, warmup_count, cfg, mode)

            for repeat in range(1, n_repeat + 1):
                barrier.wait(timeout=cfg.barrier_timeout)
                for nr in indices:
                    outcome = execute_query(con, queries[nr - 1], nr, mode=mode,
                                            cfg=cfg, worker_id=worker_id, repeat=repeat)
                    local.append(outcome)
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

    threads = [threading.Thread(target=worker, args=(wid, block),
                                name=f"bench-worker-{wid}")
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
    out: List[Dict[str, Any]] = []
    for item in repeats:
        if isinstance(item, QueryOutcome):
            out.append(item.as_dict())
        elif isinstance(item, dict):
            out.append(item)
        elif isinstance(item, (list, tuple)):
            out.extend(_flatten(item))
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
        "usable_cpus": len(allowed_cores()),
        "query_set_sha256": query_digest,
        "config": cfg.snapshot(),
    }


def save_results(outcomes: Sequence[QueryOutcome], cfg: Config, mode: str,
                 repeat_idx: int, meta: Dict[str, Any]) -> Path:
    cfg.results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = cfg.results_dir / f"tpcds_{mode}_r{repeat_idx}_{stamp}.json"

    payload = {
        "metadata": {**meta, "mode": mode, "repeat": repeat_idx,
                     "stats": compute_stats([o.as_dict() for o in outcomes])},
        "results": [o.as_dict() for o in outcomes],
    }
    filepath.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    LOG.info(f"Results saved to {filepath}")
    return filepath


def generate_summary_report(cfg: Config, all_results: Dict[str, List[List[QueryOutcome]]],
                            meta: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("=" * 78)
    lines.append("TPC-DS Benchmark Summary Report")
    lines.append("=" * 78)
    lines.append(f"Timestamp:      {meta['timestamp']}")
    lines.append(f"Platform:       {meta['platform']}  ({meta['machine']}, "
                 f"{meta['usable_cpus']}/{meta['logical_cpus']} CPUs)")
    lines.append(f"Python:         {meta['python']}  (GIL enabled: {meta['gil_enabled']})")
    lines.append(f"DuckDB:         {meta['duckdb']}")
    lines.append(f"Scale factor:   SF=10 (~10 GB)")
    lines.append(f"Database:       {cfg.db_path}")
    lines.append(f"Query set hash: {meta['query_set_sha256'][:32]}")
    lines.append(f"Queries/repeat: {cfg.n_queries}   Repeats: {cfg.n_repeat}   "
                 f"Workers: {cfg.n_workers}   Warmup: {cfg.warmup_queries}")
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
                err_msg = str(row.get('error', ''))[:110]
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
    return report


def plot_comparison(cfg: Config, all_results: Dict[str, List[List[QueryOutcome]]]) -> Optional[Path]:
    """
    Generate comparison charts with SVG fallback to work around matplotlib/FreeType
    issues in Python 3.14. Uses bundled DejaVu Sans font and vector output.
    """
    available, mpl_error = check_matplotlib()
    if not available:
        LOG.error(f"PLOTS SKIPPED: matplotlib not available ({mpl_error})")
        LOG.info("Install with: pip install matplotlib")
        return None
    
    modes = list(all_results.keys())
    num_modes = len(modes)
    
    if num_modes == 0:
        LOG.error("PLOTS SKIPPED: no results to plot")
        return None
    
    LOG.info(f"Generating comparison chart for {num_modes} mode(s): {', '.join(modes)}")
    
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        
        # Force bundled font to avoid Windows FreeType issues in 3.14
        plt.rcParams.update({
            'font.family': 'DejaVu Sans',
            'font.size': 10,
            'axes.titlesize': 12,
            'axes.labelsize': 10,
            'xtick.labelsize': 8,
            'ytick.labelsize': 8,
            'legend.fontsize': 8,
            'figure.dpi': 100,
        })
        
    except Exception as exc:
        LOG.error(f"PLOTS SKIPPED: matplotlib import/config failed: {exc}")
        LOG.debug(traceback.format_exc())
        return None

    stats = {}
    for m in modes:
        flat = _flatten(all_results[m])
        s = compute_stats(flat)
        stats[m] = s
        
        if s.get("total_queries", 0) == 0:
            LOG.warning(f"Mode '{m}' has no successful queries; will show zero values in chart")

    means = [stats[m].get("mean_duration", 0) for m in modes]
    medians = [stats[m].get("median_duration", 0) for m in modes]
    qps = [stats[m].get("throughput_qps", 0) for m in modes]
    labels = [m.upper().replace("_", " ") for m in modes]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    positions = range(len(modes))
    width = 0.38

    bars1 = ax1.bar([p - width / 2 for p in positions], means, width, label="Mean")
    bars2 = ax1.bar([p + width / 2 for p in positions], medians, width, label="Median")
    ax1.set_ylabel("Seconds")
    ax1.set_title("Latency by mode")
    ax1.set_xticks(list(positions))
    ax1.set_xticklabels(labels)
    ax1.legend(fontsize=8)
    ax1.grid(axis="y", alpha=0.3)
    for bar in list(bars1) + list(bars2):
        height = bar.get_height()
        ax1.annotate(f"{height:.2f}s",
                     (bar.get_x() + bar.get_width() / 2, height),
                     ha="center", va="bottom", fontsize=7)

    bars3 = ax2.bar(list(positions), qps, alpha=0.85)
    ax2.set_ylabel("QPS")
    ax2.set_title("Throughput by mode")
    ax2.set_xticks(list(positions))
    ax2.set_xticklabels(labels)
    ax2.grid(axis="y", alpha=0.3)
    for bar in bars3:
        height = bar.get_height()
        ax2.annotate(f"{height:.2f}",
                     (bar.get_x() + bar.get_width() / 2, height),
                     ha="center", va="bottom", fontsize=8)

    fig.tight_layout(pad=1.0)
    
    cfg.results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_stem = cfg.results_dir / f"comparison_{stamp}"

    # Try PNG first, fall back to SVG if FreeType rasterization fails (common in 3.14)
    out_path = None
    try:
        out_path = out_stem.with_suffix(".png")
        fig.savefig(out_path, dpi=100, bbox_inches="tight")
        LOG.info(f"Saved PNG: {out_path} ({out_path.stat().st_size:,} bytes)")
    except Exception as exc:
        LOG.warning(f"PNG savefig failed: {exc}")
        LOG.info("Falling back to SVG (vector, no FreeType text rasterization)")
        try:
            import matplotlib
            matplotlib.rcParams["svg.fonttype"] = "none"
            out_path = out_stem.with_suffix(".svg")
            fig.savefig(out_path, bbox_inches="tight")
            LOG.info(f"Saved SVG: {out_path} ({out_path.stat().st_size:,} bytes)")
        except Exception as exc2:
            LOG.error(f"SVG fallback also failed: {exc2}")
            plt.close(fig)
            return None

    plt.close(fig)
    
    if out_path and out_path.exists() and out_path.stat().st_size > 0:
        LOG.info(f"Comparison chart saved to {out_path}")
        return out_path
    
    LOG.error(f"PLOTS FAILED: {out_path} missing or empty")
    return None


# ============================================================================
# Smoke test
# ============================================================================
def smoke_test(cfg: Config) -> bool:
    LOG.info("=" * 74)
    LOG.info("SMOKE TEST")
    LOG.info("=" * 74)

    gil = gil_is_enabled()
    LOG.info(f"Python {sys.version.split()[0]} on {platform.platform()}")
    LOG.info(f"DuckDB {getattr(duckdb, '__version__', 'unknown')}")
    
    if gil is False:
        LOG.error("Free-threaded CPython detected; DuckDB 2.0 does not support it.")
        return False

    ok = True

    try:
        queries, digest = load_or_fetch_queries(cfg)
        LOG.info(f"[OK]   query loading: {len(queries)} queries (sha256 {digest[:16]})")
        preview = queries[0][:400] + ("..." if len(queries[0]) > 400 else "")
        print("-" * 74)
        print(preview)
        print("-" * 74)
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
        reachable = probe_server(cfg.quack_host, cfg.quack_port)
        if reachable:
            LOG.info(f"[OK]   TCP reachable: {cfg.quack_host}:{cfg.quack_port}")
        else:
            LOG.error(f"[FAIL] no TCP listener on {cfg.quack_host}:{cfg.quack_port}")
            ok = False

        for mode in ("local_quack", "remote_quack"):
            try:
                con = get_connection(cfg, mode)
                try:
                    con.execute("SELECT 42").fetchall()
                finally:
                    close_connection(con)
                LOG.info(f"[OK]   {mode.replace('_', ' ')} handshake")
            except Exception as exc:
                LOG.error(f"[FAIL] {mode.replace('_', ' ')}: {type(exc).__name__}: {str(exc)[:160]}")
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
        description="TPC-DS benchmark harness for DuckDB Quack (Windows 11 / DuckDB 2.x)"
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
    parser.add_argument("--no-pin", action="store_true", help="disable CPU pinning")
    parser.add_argument("--no-plots", action="store_true", help="skip chart generation")
    return parser.parse_args(argv)


def resolve_modes(args: argparse.Namespace) -> List[str]:
    if args.modes:
        requested = [m.strip().lower() for m in args.modes.split(",") if m.strip()]
        unknown = [m for m in requested if m not in VALID_MODES]
        if unknown:
            raise SystemExit(f"Unknown mode(s): {', '.join(unknown)}")
        return requested
    if args.mode:
        return [args.mode]
    return [CONFIG.mode]


def main(argv: Optional[Sequence[str]] = None) -> int:
    configure_console()
    
    mat_available, mat_err = check_matplotlib()
    if not mat_available:
        LOG.warning("matplotlib unavailable - charts will be disabled")
    
    args = parse_args(argv)

    if args.repeat is not None:
        CONFIG.n_repeat = args.repeat
    if args.workers is not None:
        CONFIG.n_workers = args.workers
    if args.queries is not None:
        CONFIG.n_queries = args.queries
    if args.no_warmup:
        CONFIG.warmup_queries = 0
    if args.no_pin:
        CONFIG.pin_cores = False

    modes = resolve_modes(args)

    LOG.info("=" * 74)
    LOG.info("TPC-DS Quack Benchmark Harness")
    LOG.info("=" * 74)
    LOG.info(f"Modes:        {', '.join(modes)}")
    LOG.info(f"Repeats:      {CONFIG.n_repeat}")
    LOG.info(f"Workers:      {CONFIG.n_workers}")
    LOG.info(f"Queries:      {CONFIG.n_queries}")
    LOG.info(f"Warmup:       {CONFIG.warmup_queries}")
    LOG.info(f"Database:     {CONFIG.db_path}")
    LOG.info(f"Quack server: {CONFIG.quack_host}:{CONFIG.quack_port}")
    LOG.info(f"Results dir:  {CONFIG.results_dir}")
    LOG.info(f"Plots enabled: {'Yes' if not args.no_plots and mat_available else 'No'}")
    print()

    errors = report_issues(validate_config(CONFIG))
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
                save_results(outcomes, CONFIG, mode, idx, meta)

    report = generate_summary_report(CONFIG, all_results, meta)
    print()
    print(report)

    if not args.no_plots:
        chart_path = plot_comparison(CONFIG, all_results)
        if chart_path is None:
            LOG.info("Charts were not generated. Run with --help for options.")
    else:
        LOG.info("Plot generation disabled by --no-plots flag")

    LOG.info("Benchmark complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())