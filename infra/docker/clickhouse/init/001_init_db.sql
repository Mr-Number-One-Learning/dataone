-- Runs automatically on first ClickHouse container start.
-- Creates the database that holds the curated business marts served to Grafana.
-- Table DDL lives in 002_marts.sql, run right after this file (alphabetical
-- order — see docker-entrypoint-initdb.d's execution order).

CREATE DATABASE IF NOT EXISTS dataone_marts;
