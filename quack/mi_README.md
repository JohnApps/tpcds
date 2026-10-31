\# TPC-DS Quack Benchmark Harness — Documentation



Benchmark harness for running the TPC-DS query set (up to 99 queries, SF=10)

against DuckDB 2.0, either in-process or through a Quack server, sequentially

or with concurrent workers.



Target environment: Windows 11, CPython 3.13 (standard GIL build), DuckDB 2.0.



\---

\## 0. Operating

\- \*\* All three modes, including the remote server
python cl_tpcds_quack_bench.py --modes local_inprocess,local_quack,remote_quack

\- \*\*Combined with other options, e.g. quick 20-query comparison, 2 repeats
python cl_tpcds_quack_bench.py --modes local_inprocess,local_quack --queries 20 --repeat 2

\- \*\*Label the run so files from the comparison group together (optimized version)
python cl_tpcds_quack_bench.py --modes local_inprocess,local_quack --tag inproc-vs-quack

\- \*\*QUACK_TOKEN must be set in your .env — Quack modes abort on validation without it.

\- \*\*The Quack server must be running and reachable at QUACK_HOST:QUACK_PORT (the smoke test checks this before starting).

\- \*\*You can set a default in .env with MODE=local_inprocess — --modes overrides it.
\- \*\*Alternatively, a single mode can be picked with --mode local_quack.


\## 1. Overview



The harness answers one question: \*\*how do query latencies compare between

execution modes, and how does the engine behave under concurrency?\*\*



It supports three execution modes:



| Mode | Description |

|---|---|

| `local\_inprocess` | DuckDB opens the database file directly, read-only |

| `local\_quack` | Queries go through a Quack server on localhost |

| `remote\_quack` | Queries go through a Quack server on a remote host (SSL disabled) |



Benchmark methodology:



\- \*\*Warm repeats\*\* — one connection stays open across all repeats (per mode in

&#x20; sequential runs, per worker in concurrent runs), so caching effects are

&#x20; consistent after an initial warmup.

\- \*\*Warmup\*\* — `WARMUP\_QUERIES` queries run once before the first repeat.

\- \*\*Synchronized repeats (concurrent)\*\* — workers meet at a barrier at the

&#x20; start of each repeat so all workers begin together.

\- Per-query wall time includes result consumption (fetching all rows), which

&#x20; is what an application would experience.



\---



\## 2. Requirements



\- Python 3.10+ (developed for 3.13, standard GIL build)

\- `duckdb` (2.0)

\- `python-dotenv`

\- Optional: `numpy` (faster statistics), `matplotlib` (charts),

&#x20; `pyarrow` (only if `RESULT\_MODE=arrow`)



```

pip install duckdb python-dotenv

pip install numpy matplotlib pyarrow   # optional

```



\---



\## 3. Configuration



Settings come from a `.env` file next to the script (or the path in

`DOTENV\_PATH`), overridable by OS environment variables.



\### 3.1 Core paths



| Variable | Default | Description |

|---|---|---|

| `DB\_PATH` | `H:\\tpcds\\tpcds\_sf10.db` | TPC-DS database file (read-only, in-process mode) |

| `CACHE\_QUERIES` | `H:\\tpcds\\queries\_cached.txt` | Cached query text, `\\n;\\n`-separated |

| `SQL\_FILE\_PATH` | `H:\\tpcds\\tpcds\_queries.sql` | Generated SQL file (source of truth) |

| `RESULTS\_DIR` | `H:\\tpcds\\results` | Output directory for JSON, CSV, summary, charts |



\### 3.2 Quack server



| Variable | Default | Description |

|---|---|---|

| `QUACK\_HOST` | `localhost` | Quack server host |

| `QUACK\_PORT` | `9494` | Quack server port |

| `QUACK\_TOKEN` | \*(empty)\* | Auth token; \*\*required\*\* for Quack modes |



\### 3.3 Benchmark parameters



| Variable | Default | Description |

|---|---|---|

| `N\_QUERIES` | `99` | Number of TPC-DS queries (1–99) |

| `N\_REPEAT` | `3` | Warm repetitions |

| `N\_WORKERS` | `1` | `1` = sequential; `>1` = concurrent workers |

| `WARMUP\_QUERIES` | `5` | Warmup queries per connection (0 = none) |

| `MODE` | `local\_inprocess` | Default mode when no `--mode/--modes` given |



\### 3.4 Engine tuning



| Variable | Default | Description |

|---|---|---|

| `PHYSICAL\_CORES` | `8` | Physical core count (used for validation warnings) |

| `QUERY\_THREADS` | `0` | DuckDB threads for sequential runs; `0` = `PHYSICAL\_CORES` |

| `WORKER\_THREADS` | `1` | DuckDB threads per concurrent worker |

| `MEMORY\_LIMIT` | \*(empty)\* | e.g. `8GB`. Strongly recommended for reproducibility |

| `PRESERVE\_INSERTION\_ORDER` | `false` | `false` is faster for analytic queries |



> \*\*Important:\*\* DuckDB's `threads`, `memory\_limit`, and

> `preserve\_insertion\_order` are instance-wide settings. All connections to

> the same database file in the process share them, so concurrent workers

> must use the same `WORKER\_THREADS` value.



\### 3.5 Result handling and output



| Variable | Default | Description |

|---|---|---|

| `RESULT\_MODE` | `full` | `full` = `fetchall()`, `drain` = chunked `fetchmany()`, `arrow` = `fetch\_arrow\_table()` |

| `FETCH\_CHUNK\_ROWS` | `65536` | Chunk size for `drain` mode |

| `BARRIER\_TIMEOUT` | `600` | Seconds to wait at the worker barrier |

| `CHART\_FORMAT` | `svg` | Preferred chart format (`png` is the fallback) |



`RESULT\_MODE` tradeoffs:



\- `full` — realistic application path; creates Python row objects (slowest).

\- `drain` — chunked fetching; low memory, less Python-object overhead.

\- `arrow` — columnar, no per-row Python objects (fastest; needs `pyarrow`).



\---



\## 4. Command line



```

python cl\_tpcds\_quack\_bench.py \[options]

```



| Flag | Description |

|---|---|

| `--smoke` | Run only the smoke test (connectivity + query loading) |

| `--validate-only` | Validate configuration and exit |

| `--mode M` | Single mode (`local\_inprocess`, `local\_quack`, `remote\_quack`) |

| `--modes M1,M2` | Comma-separated list of modes to compare |

| `--workers N` | Override `N\_WORKERS` |

| `--repeat N` | Override `N\_REPEAT` |

| `--queries N` | Override `N\_QUERIES` (1–99) |

| `--no-warmup` | Skip warmup queries |

| `--no-plots` | Skip chart generation |

| `--tag LABEL` | \*(new in optimized version)\* Group all output files of one run under a shared label |



Examples:



```bash

\# Quick 10-query sanity check, no charts

python cl\_tpcds\_quack\_bench.py --queries 10 --no-plots



\# Compare in-process vs local Quack

python cl\_tpcds\_quack\_bench.py --modes local\_inprocess,local\_quack



\# 4 concurrent workers, 5 repeats

python cl\_tpcds\_quack\_bench.py --workers 4 --repeat 5

```



\---



\## 5. How it works



\### 5.1 Query loading (priority order)



1\. \*\*SQL file\*\* (`SQL\_FILE\_PATH`) — the source of truth. Re-parsed whenever

&#x20;  it is newer than the cache.

2\. \*\*Cache file\*\* (`CACHE\_QUERIES`) — used if it exists and holds enough queries.

3\. \*\*`tpcds\_queries()`\*\* — DuckDB's built-in generator; refreshes the cache.



The loaded query set is hashed (SHA-256) and stored in every result file, so

runs are always attributable to an exact query set.



\### 5.2 Execution flow



```

main()

&#x20;├── configure console, parse args, validate config

&#x20;├── smoke test (skippable, non-fatal)

&#x20;├── load queries + environment fingerprint

&#x20;├── for each mode:

&#x20;│     ├── N\_WORKERS == 1 → sequential (one connection, warm repeats)

&#x20;│     └── N\_WORKERS > 1  → concurrent (one connection per worker,

&#x20;│                          barrier at each repeat start)

&#x20;├── save per-repeat JSON (+ per-run CSV export)

&#x20;├── summary report (TXT)

&#x20;└── comparison chart (SVG/PNG)

```



\### 5.3 Error handling



\- Query failures never abort a run; each execution records

&#x20; `success`, `error`, and an `error\_class`

&#x20; (BINDER, CATALOG, PARSER, IO, OOM, INVALID\_INPUT, …).

\- After a failure the connection is settled (`complete()`/`abort()`) before

&#x20; reuse, so one bad query does not poison the connection.

\- Concurrent workers that crash abort the barrier and log an error; the

&#x20; results of completed work are still recorded.



\### 5.4 Statistics



Per mode, across all repeats: mean, median, P95, std dev, min/max,

success rate, throughput (QPS), total time, plus the 5 slowest queries.

The summary report also shows each mode's \*\*median ratio\*\* relative to

`local\_inprocess` (the baseline).



\---



\## 6. Outputs (in `RESULTS\_DIR`)



| File | Description |

|---|---|

| `tpcds\_<mode>\_r<repeat>\_<stamp>.json` | Per-repeat results with metadata and stats |

| `tpcds\_runs.csv` | \*(new)\* Append-only CSV of every execution (query, mode, repeat, worker, duration, rows, success) for spreadsheet analysis |

| `summary\_<stamp>.txt` | Human-readable summary report |

| `comparison\_<stamp>.svg` / `.png` | Latency + throughput chart |



\---



\## 7. Performance recommendations



1\. \*\*Set `MEMORY\_LIMIT` explicitly\*\* (e.g. `8GB`) — reproducible plans,

&#x20;  predictable spilling.

2\. Keep `PRESERVE\_INSERTION\_ORDER=false` for analytic throughput.

3\. Keep `N\_WORKERS × WORKER\_THREADS ≤ PHYSICAL\_CORES` to avoid contention

&#x20;  (the harness warns otherwise).

4\. Use `RESULT\_MODE=arrow` when you only need timings, `full` when you want

&#x20;  realistic end-to-end latency.

5\. Use enough warmup queries (≥5) to stabilize the OS file cache.

6\. For stable comparisons, run the battery on an idle machine and prefer the

&#x20;  \*\*median\*\* over the mean (TPC-DS has heavy-tailed query times).



\---



\## 8. Troubleshooting



| Symptom | Likely cause / fix |

|---|---|

| `database file not found` | `DB\_PATH` wrong, or DB locked by another process |

| `QUACK\_TOKEN required` | Set the token; Quack modes always need it |

| `no TCP listener on host:port` | Quack server not running / firewall |

| `RESULT\_MODE=arrow requires pyarrow` | `pip install pyarrow` |

| Charts missing | matplotlib not installed; harness continues without plots |

| `Missing queries: \[...]` | SQL file incomplete for the requested `N\_QUERIES` |

| Concurrent results noisy | Oversubscribed threads; reduce `WORKER\_THREADS` |

| Wide variance between repeats | Increase `WARMUP\_QUERIES`; check background load |



\---



\## 9. Known limitations



\- DuckDB settings are instance-wide, so concurrent workers cannot use

&#x20; different thread counts against the same database file.

\- `remote\_quack` attaches with `DISABLE\_SSL true` — only for trusted networks.

\- Concurrent repeats are synchronized at their \*\*start\*\*, but workers drift

&#x20; apart within a repeat by design (this is what is being measured).

\- The harness expects the standard GIL build; a free-threaded CPython is

&#x20; rejected by the smoke test.

