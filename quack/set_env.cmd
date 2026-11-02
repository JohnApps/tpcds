echo off
rem set_env.cmd
rem envs for running duckdb quack with the tpc-ds database
rem

set db_path=H:\tpcds\tpcds_sf10.db

set CACHE_QUERIES=H:\tpcds\quack\tpcds_queries.sql
set SQL_FILE_PATH=H:\tpcds\quack\cache_queries.sql

set QUACK_HOST=localhost
set QUACK_PORT=9494
set QUACK_TOKEN=super_secret

set RESULTS_DIR=H:\tpcds\quack\results

SET N_QUERIES=99
SET N_REPEAT=3
SET N_WORKERS=1
set MODE=local_inprocess
