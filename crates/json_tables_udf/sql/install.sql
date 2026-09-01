-- Install the in-database JSON Tables loader.
--
-- Prerequisites:
--   * the Rust language container registered as RUST (see language-container-rs)
--   * libjson_tables_udf.so uploaded to the BucketFS path referenced below
--
-- The CONNECTION is how the driver opens its own SQL session. On a multi-node
-- cluster, point it at the node address rather than loopback.

CREATE SCHEMA IF NOT EXISTS JSON_TABLES;

CREATE OR REPLACE CONNECTION JSON_TABLES_SELF
  TO '127.0.0.1:8563'
  USER 'sys'
  IDENTIFIED BY 'exasol';

-- One call: infer the family, create the tables, load them, stamp provenance.
CREATE OR REPLACE RUST SCALAR SCRIPT JSON_TABLES.INGEST_JSON(
  src             VARCHAR(2000),
  target_schema   VARCHAR(128),
  conn_name       VARCHAR(128),
  opts            VARCHAR(4000)
) EMITS (
  table_name  VARCHAR(128),
  rows_loaded DECIMAL(18,0),
  status      VARCHAR(200)
) AS
%connection JSON_TABLES_SELF;
%udf_object /buckets/bfsdefault/rust/libjson_tables_udf.so;
/

-- Pass 1 only: the plan the loaders take, and the DDL the driver would run.
CREATE OR REPLACE RUST SCALAR SCRIPT JSON_TABLES.PLAN_JSON(
  src             VARCHAR(2000),
  conn_name       VARCHAR(128),
  opts            VARCHAR(4000)
) EMITS (
  plan_json VARCHAR(2000000),
  ddl  VARCHAR(2000000)
) AS
%connection JSON_TABLES_SELF;
%udf_object /buckets/bfsdefault/rust/libjson_tables_udf.so;
/

-- The row-emitting loader for one table.
--
-- Declared with Exasol's dynamic parameters, `(...) EMITS (...)`: the driver
-- supplies the column list per call from the plan it inferred. A static EMITS
-- clause here would make the engine reject the call with "the script has a
-- static return argument definition".
--
-- Arguments, by position: source, table path, plan, connection name.
CREATE OR REPLACE RUST SCALAR SCRIPT JSON_TABLES.LOAD_TABLE (...) EMITS (...) AS
%connection JSON_TABLES_SELF;
%udf_object /buckets/bfsdefault/rust/libjson_tables_udf.so;
/
