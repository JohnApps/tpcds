TPC-DS Quack Benchmark Harness — Documentation
File: tpcds_quack_bench.py Target environment: Windows 11 · DuckDB 2.x · PostgreSQL 18 (client-side tooling) · CPython 3.14 (GIL build, not 3.14t) Last revised: 2026-10-29

1. Purpose
A benchmark harness that measures TPC-DS query latency against a 10 GB DuckDB database (SF=10) under three execution modes, compares them statistically, and emits JSON results, a text summary report, and comparison charts.

The three modes:

Mode	Description	Requires
local_inprocess	Opens the .db file directly, read-only, in the same process	DB_PATH pointing to a valid TPC-DS database
local_quack	Connects through the DuckDB Quack extension to a server on localhost	QUACK_TOKEN, Quack server listening on QUACK_HOST:QUACK_PORT
remote_quack	Same as local_quack but attaches with DISABLE_SSL true	As above, plus network reachability
The harness benchmarks identical SQL under identical conditions across modes so relative timings are comparable. All queries run against TPC-DS query set 1–99.

2. Command Line
python tpcds_quack_bench.py [OPTIONS]
Option	Effect
--smoke	Run the smoke test only, then exit
--validate-only	Validate configuration and exit
--mode MODE	Run a single mode (local_inprocess, local_quack, remote_quack)
--modes M1,M2	Run multiple modes in one invocation and chart them together
--workers N	Override N_WORKERS (concurrent workers)
--repeat N	Override N_REPEAT (repetitions per mode)
--queries N	Override N_QUERIES (1–99)
--no-warmup	Skip warmup queries
--no-pin	Disable CPU affinity pinning
--no-plots	Skip chart generation
Typical comparative run:

python tpcds_quack_bench.py --modes local_inprocess,local_quack
Important: run the modes you want compared in a single invocation. Separate invocations each see only one mode, so neither the comparison chart nor the summary's "relative to local inprocess" section will have a baseline to compare against.

3. Configuration
Settings come from a .env file beside the script (or $DOTENV_PATH), overridable by OS environment variables. Command-line flags override both.

Variable	Default	Meaning
DB_PATH	H:\tpcds\tpcds_sf10.db	TPC-DS database file (read-only in-process mode)
CACHE_QUERIES	H:\tpcds\queries_cached.txt	Cached query text (fastest load path)
SQL_FILE_PATH	H:\tpcds\tpcds_queries.sql	Generated SQL source file
QUACK_HOST / QUACK_PORT	localhost / 9494	Quack server endpoint
QUACK_TOKEN	(empty)	Auth token; mandatory for Quack modes
RESULTS_DIR	H:\tpcds\results	Output directory
N_QUERIES	99	Number of queries to run (1–99)
N_WORKERS	1	Concurrent workers; 1 = sequential mode
N_REPEAT	3	Repetitions per mode
WARMUP_QUERIES	5	Untimed warmup queries per connection
MODE	local_inprocess	Default mode when none is given
QUERY_THREADS	0	DuckDB intra-query threads (0 = engine default)
WORKER_THREADS	1	DuckDB threads per concurrent worker
MEMORY_LIMIT	(empty)	e.g. 8GB; empty = engine default
PIN_CORES	true	Pin worker threads to distinct logical CPUs
RESULT_MODE	full	full = fetchall(); drain = chunked fetchmany()
FETCH_CHUNK_ROWS	65536	Chunk size when RESULT_MODE=drain
BARRIER_TIMEOUT	600	Seconds to wait for synchronised worker start
Query loading order
load_or_fetch_queries() resolves the query set in priority order:

Cache file (CACHE_QUERIES) — fastest; trusted as-is if it contains ≥ N_QUERIES entries separated by \n;\n
SQL file (SQL_FILE_PATH) — parsed, then written to the cache
tpcdds_queries() — fetched from the DuckDB tpcds extension, then cached
A SHA-256 digest of the resolved query set is embedded in every result file and the summary so runs remain verifiable and comparable.

Result consumption
Timing includes full result consumption (end-to-end), which is deliberate — mode comparison would be meaningless otherwise.

RESULT_MODE=full — fetchall(), materialises the full result
RESULT_MODE=drain — drains in FETCH_CHUNK_ROWS chunks; lower peak memory on wide result sets with negligible timing difference
4. Architecture
main()
 ├─ configure_console()          UTF-8 stdout/stderr (Windows)
 ├─ check_matplotlib()           one-shot import test, logs version/backend
 ├─ parse_args() / overrides     CLI flags beat .env values
 ├─ validate_config()            hard-fails on bad config (port range, missing files, …)
 ├─ smoke_test()                 query loading, in-process connect, Quack handshake
 ├─ load_or_fetch_queries()      cache → SQL file → extension, with digest
 │
 ├─ per mode:
 │   ├─ run_queries_sequential()     N_WORKERS == 1
 │   └─ run_concurrent_benchmark()   N_WORKERS > 1
 │       (both return List[List[QueryOutcome]] — outer = repeat, inner = executions)
 │   └─ save_results()              one JSON per repeat
 │
 ├─ generate_summary_report()   aggregated text report + baseline ratios
 └─ plot_comparison()           SVG chart (PNG attempted first, see §6)
Execution paths
Sequential (N_WORKERS=1): one connection per repeat, opened fresh each repeat. Warmup runs untimed, then every query is timed individually with time.perf_counter() around both execution and result consumption. One-second pause between repeats.

Concurrent (N_WORKERS>1): queries split into contiguous blocks (split_indices()), one worker thread per block. Each worker pins itself to one logical CPU (Windows SetThreadAffinityMask via ctypes), opens its own connection with WORKER_THREADS, warms up, then waits on a threading.Barrier so all workers start timed execution simultaneously. Per-worker results are merged into per_repeat[repeat] under a lock.

Workloads and workers can be sized independently:

QUERY_THREADS — intra-query parallelism (one big query using many cores)
N_WORKERS × WORKER_THREADS — inter-query parallelism (many queries each using few cores)
For throughput-oriented runs, WORKER_THREADS=1 with more workers generally beats fewer fatter workers.

Error handling
Each execution is wrapped; failures produce a QueryOutcome with success=False, a truncated error message, and a coarse classification (BINDER, PARSER, OOM, …) derived from the exception class name. Failures never abort a run — they are counted and reported. A failed execution calls settle_result() on the connection to release the pending result before continuing.

5. Outputs
All outputs land in RESULTS_DIR:

Artifact	Pattern	Contents
Per-repeat results	tpcds_<mode>_r<n>_<timestamp>.json	Full metadata + per-query outcomes + computed stats
Summary report	summary_<timestamp>.txt	Per-mode aggregates, slowest queries, failures, baseline ratios
Comparison chart	comparison_<timestamp>.svg	Latency (mean/median) and throughput (QPS) panels
The JSON schema:

{
  "metadata": {
    "timestamp": "...", "python": "3.14.x", "gil_enabled": true,
    "duckdb": "2.x", "platform": "...", "logical_cpus": 24,
    "query_set_sha256": "...", "config": { "...sanitised snapshot..." },
    "mode": "local_quack", "repeat": 2, "stats": { "...aggregate stats..." }
  },
  "results": [
    {
      "query_nr": 34, "mode": "local_quack", "duration_sec": 1.2345,
      "row_count": 280, "success": true, "error": null,
      "error_class": null, "worker_id": null, "repeat": 2
    }
  ]
}
Note: the QUACK_TOKEN in the embedded config snapshot is truncated to its first 8 characters — secrets are never written to disk in full.

Aggregate statistics include mean, median, P95, standard deviation, min/max, total time, success rate, and throughput (QPS). NumPy accelerates these when present; pure-statistics fallback otherwise.

6. Chart Generation (Python 3.14 workaround)
Known issue: on CPython 3.14 the matplotlib/FreeType binding raises FT_Render_Glyph ... error 0x62: raster overflow when rasterising text to PNG. The charts themselves are unaffected on 3.12; the fault is in the 3.14 wheel, not this code or your font configuration.

Workaround implemented: plot_comparison() attempts PNG first; on failure it saves as SVG with svg.fonttype = "none":

SVG output emits text as editable text nodes, so FreeType is never invoked — the failure mode cannot occur
Opened in Edge/Chrome (or converted later), the SVG is resolution-independent
Expected log output on 3.14: a PNG savefig failed warning followed by Saved SVG: ...
The chart forces the bundled DejaVu Sans font family and modest sizes (10×4 inch figure, dpi 100) to minimise pressure on the rasteriser during the PNG attempt.

Optional cleanup: once satisfied SVG suffices, replace the PNG-then-SVG try/except block in plot_comparison() with a direct SVG save to remove the wasted render cycle and the cosmetic error line from the log.

7. Dependencies
Package	Required	Used for
duckdb	Yes	Database connections, tpcds extension
python-dotenv	Yes	.env loading
psutil	Optional	CPU affinity discovery (falls back to os.cpu_count())
numpy	Optional	Faster statistics (falls back to statistics module)
matplotlib	Optional	Charts (harness runs fine without it; plots are skipped with a logged reason)
CPython must be a GIL build. The smoke test refuses to proceed on a free-threaded (3.14t) interpreter since DuckDB 2.x does not support it.

8. Known Limitations & Caveats
Single-invocation comparison. Modes compared in separate invocations produce no comparison chart and no baseline-relative section — each run only knows about itself.
SQL parser comment handling. parse_sql_file() drops lines beginning with -- at any position after stripping. A -- sequence at line start inside a string literal in the source SQL would be silently removed. Verify against your generated tpcds_queries.sql if results look anomalous.
Wasted PNG attempt. On 3.14 the PNG render fails before the SVG fallback fires (~1–2 s per run). Cosmetic; remove per §6 if desired.
CPU pinning granularity. Workers pin to core_index % len(cores) of the process's allowed set. On a single-socket machine with psutil absent, this falls back to sequential core assignment from os.cpu_count().
store_sales count probe. The smoke test assumes the standard TPC-DS table store_sales exists in the database file. A differently-built schema will fail the smoke test even though the queries might be valid.
9. Exit Codes
Code	Meaning
0	Completed (includes completion despite smoke-test warnings)
1	Configuration validation errors — run aborted before benchmarking
SystemExit with a message is raised for unknown --modes values at argument resolution.