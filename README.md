# Self-healing pipeline — Bronze/Silver1/Silver2/Gold Lakehouse

Kafka → Spark Structured Streaming jobs (Bronze, Silver1, Silver2, Gold) → Delta
Lake, running as a multi-hop pipeline: each layer streams from the previous
layer's Delta table, following the Medallion Lakehouse pattern.

Each data folder (`data/Crash1` .. `data/Crash4`) is its own source with its own
Bronze, Silver 1 and Silver 2 tables. The branches meet only in Gold, a UNION ALL
of every Silver 2 table, so a problem in one folder stays in its own branch
(and in lineage, its blast radius is that branch plus Gold):

```
crashes.crash1 --> bronze_crash1 --> silver1_crash1 --> silver2_crash1 --+
crashes.crash2 --> bronze_crash2 --> silver1_crash2 --> silver2_crash2 --+--> gold (UNION ALL,
crashes.crash3 --> bronze_crash3 --> silver1_crash3 --> silver2_crash3 --+     source_table)
crashes.crash4 --> bronze_crash4 --> silver1_crash4 --> silver2_crash4 --+
                                          |                  |
                                          v                  v
                             silver1_dead_letter     silver2_quarantine, silver2_dead_letter,
                                  (shared)           silver2_fuzzy_audit (shared, each row
                                                     tagged with its source / target_table)
```

The layout lives in `spark-jobs/sources.py`. `SOURCES` (default
`crash1,crash2,crash3,crash4`) on the Spark services must name the same folders as
the producer's `TABLES`; a source with no data blocks the layers after it.

## Prerequisites

- Docker + Docker Compose v2
- ~5 GB RAM free (Kafka + two Spark JVMs; the Silver 2 + Gold driver uses 2 GB)

## Setup

```bash
docker compose up --build
```

The producer replays the partitioned datasets (`data/Crash1` .. `data/Crash4`)
across their respective parts through one Kafka topic each (`crashes.crash1` ..
`crashes.crash4`) into each source's Bronze table automatically.

**Moving from the old single-branch layout:** the per-source tables live at new
paths (`delta/bronze_crash1`, ...), so nothing old is reused. For a clean start,
stop the stack and delete `delta/` and `checkpoints/` (the reference mappings are
re-seeded on start), then `docker compose up -d --build`.

First run takes a minute or two (Spark images install a JVM, connector
jars get pulled). Subsequent runs are fast.

## Watch it work

- **Kafka UI** — http://localhost:8080 — browse `crashes.crash1` .. `crashes.crash4`
- **Producer logs** — `[Crash1] Batch N: published 50 rows to 'crashes.crash1'`
- **bronze-silver1-job logs** — one Spark driver running 8 queries (2 per source):
  - `[bronze_crash1]` streams its topic into `delta/bronze_crash1`
  - `[silver1_crash1]` unpacks JSON into 49 fields and runs formatting/cleaning SQL -> `delta/silver1_crash1`
- **silver2-gold-job logs** — one Spark driver running 5 queries:
  - `[silver2_crash1]` standardizes Silver 1 -> `delta/silver2_crash1`, failures -> `delta/silver2_quarantine`
  - `[gold]` streams the UNION ALL of every Silver 2 table into `delta/gold`
- **lineage-consumer logs** — `[lineage] write_gold COMPLETED (n of m events written)`
- **Neo4j browser** — http://localhost:7474 (user `neo4j`, password `NEO4J_PASSWORD`, default
  `selfhealing`) — try `MATCH p=(:Table)-[:FEEDS*]->(:Table) RETURN p`

## Verify data landed, from the host (no Spark needed)

```bash
pip install deltalake pandas
python verify_delta.py bronze_crash1
python verify_delta.py silver1_crash1
python verify_delta.py silver2_crash3
python verify_delta.py silver2_quarantine
python verify_delta.py ref_mappings
python verify_delta.py gold
```

For a richer view, `Read Data/read_parquet_v3.py` (needs `duckdb deltalake pandas`,
`rich` optional) knows the per-source layout:

```bash
python "Read Data/read_parquet_v3.py" overview                # rows per source and layer
python "Read Data/read_parquet_v3.py" silver2 --table 10      # all sources, with source_table
python "Read Data/read_parquet_v3.py" quarantine --source crash3
python "Read Data/read_parquet_v3.py" trace                   # one record through its branch
```

To stop: `docker compose down`. To wipe all state and start clean:
`docker compose down && rm -rf delta checkpoints`.

## What's in each piece

- **`producer/`** — reads partitioned CSVs, publishes rows to Kafka
  in batches on an interval, wrapped in an envelope
  (`record_id`, `batch_id`, `table_name`, `ingestion_ts`, `payload`, `**row`).
- **`spark-jobs/common.py`** — shared Spark session builder and a
  wait-for-upstream-table guard, used by all jobs.
- **`spark-jobs/run_layers.py`** — runs several layers as streaming queries in
  one Spark driver (`python run_layers.py bronze silver1`). Compose uses it to
  run the four layers in two containers instead of four JVMs. Each
  `<layer>_job.py` exposes `start(spark)` and still runs on its own too.
- **`spark-jobs/bronze_job.py`** — Kafka → Delta, untouched raw landing. Raw string
  values plus Kafka's own metadata.
- **`spark-jobs/silver1_job.py`** — Bronze → Silver 1. Unpacks JSON envelope and
  provides `SILVER1_TRANSFORM_SQL` for formatting and cleaning.
- **`spark-jobs/silver2_job.py`** — Silver 1 → Silver 2. Provides
  `SILVER2_TRANSFORM_SQL` for data standardization.
- **`spark-jobs/gold_job.py`** — Silver 2 → Gold. Provides
  `GOLD_TRANSFORM_SQL` for business-ready fields and metrics.
- **`lineage/`** — pipeline lineage in Neo4j. See [Lineage](#lineage) below.
- **`delta/{bronze,silver1,silver2,gold}`** — the four Lakehouse tables.
  Inspect with `verify_delta.py`.
- **`checkpoints/`** — Structured Streaming's checkpoint state per job.

## Lineage

Every Spark job carries the `openlineage-spark` listener (wired in
`spark-jobs/common.py`). It publishes OpenLineage events to the Kafka topic
`openlineage.events`; `lineage-consumer` reads them and writes the graph into Neo4j
(spec KG1):

```
Spark jobs --OpenLineage events--> kafka: openlineage.events --> lineage-consumer --> Neo4j
```

- `lineage/normalize.py` keeps only the events that write a pipeline table and maps
  paths to table names (`/data/delta/silver2_crash3` becomes `silver2_crash3`; tables
  must sit directly under `/data/delta`, a nested path is dropped). The raw stream is
  mostly Spark-internal plans and `_delta_log` reads, which carry no lineage.
- `lineage/graph.py` merges `(:Job)`, `(:Run)`, `(:Table)`, `(:SchemaVersion)` and
  `(:Table)-[:FEEDS]->(:Table)` into Neo4j. Every statement is a `MERGE`, so a replayed
  event changes nothing. The `:Table` nodes are the ones incidents attach to.
- `graph.downstream_tables()` / `upstream_tables()` answer "what does a failure in
  `silver2_crash3` reach" (just `gold`, not the other sources) for the Deterministic
  Lookup step. Incidents are keyed per layer (`silver2|column|value`), so a fix learned
  on one source is found again on another.

The listener must be **openlineage-spark 1.53.0 or newer** (pinned to 1.54.0). Earlier
versions do not report the Gold write, so the `silver2 -> gold` edge is missing.

Set `OPENLINEAGE_ENABLED=false` on a Spark service to turn the listener off.

Neo4j only applies `lineage/schema.cypher` when `neo4j-init` runs. If your Neo4j
container already exists, apply the new constraints once with
`docker compose up neo4j-init`.

Unit tests (no Kafka or Neo4j needed): `cd lineage && python -m pytest tests`.

## Deterministic Lookup

`lookup/` ranks failures and checks the incident memory before any LLM runs. See
[lookup/README.md](lookup/README.md).

```
silver2_quarantine --lookup-scanner--> kafka: recovery.failures  --+
Qwen (QWEN_KAFKA_ENABLED=true) ------> kafka: recovery.incidents --+--> deterministic-lookup --> kafka: recovery.lookup
                                                                          (Neo4j lineage + incidents)
```

Each `recovery.lookup` message is a stack, highest impact first. Each item holds the
structured failure, its lineage context (blast radius score 0-100 with a breakdown) and
its incident context (best past fix, and whether it can be reused). `route` says
`remediate` or `diagnose`. Unit tests: `python -m pytest lookup/tests`.

## Recovery agent (LangGraph loop)

`agent/` reads `recovery.lookup` and runs one LangGraph workflow per failure:
deterministic lookup, diagnose, policy gate, remediate, validate, escalate, learn.
The state of every workflow is saved after each step (SQLite, volume `agent-state`),
so a crashed workflow resumes and a redelivered one is skipped. See
[agent/README.md](agent/README.md).

Diagnose asks a reasoning model (Qwen on Ollama, `DIAGNOSE_MODEL`) for a root cause
and one fix out of a pre-approved action list. The policy gate lets the fix run only
when the model's confidence reaches `POLICY_CONFIDENCE_THRESHOLD` (0.85) and the
action is in the safe set (today only `ADD_VARIANT`); everything else goes to an
engineer with the reasons. Where a record can decide the value (`crash_month` and
`crash_day_of_week` against `crash_date`) the gate checks it against the quarantined
records; where nothing can, the stricter `POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD`
(0.95) applies. `docker compose exec recovery-agent python -m agent.check_model`
confirms the model answers.

Remediate executes an approved fix exactly once: it checks the audit table first,
stages the failure's quarantined records with the proposed mapping, and writes one
audit record (action, parameters, affected records, timestamp) to the Delta table
`delta/remediation_audit`. Staging changes nothing in Silver 2, the quarantine or the
reference table. Print the records with
`docker compose exec recovery-agent python -m agent.audit`.

Validate and the steps after it are still stubs, so every failure currently ends
`ESCALATED`. Unit tests: `python -m pytest agent`.

## Swapping in the real dataset later

1. Drop the real CSV into `data/` (e.g. `data/cars_dataset_2025.csv`).
2. Change `CSV_PATH` under the `producer` service in `docker-compose.yml`
   to point at it.
3. If its column names match the mock CSV's header exactly, nothing else
   changes. If they differ, update `PAYLOAD_SCHEMA` in `silver_job.py` to
   match — that's the only place column names are hardcoded.

## Deliberately deferred (next steps)

- **Two-table Silver join in Gold** — marked with a comment in
  `gold_job.py`. Needs a watermark on both streaming sides once you get
  there; ask if you want to work through that pattern.
- **Multi-source / schema-drift simulation** — vary formatting per
  simulated source, deliberately rename/drop a field mid-stream.
- **Fault injection harness** — a corruption step between the CSV and the
  producer.
- **Detect/triage layer** — Great Expectations checkpoints on `silver_cars`,
  plus the severity-classification logic from the architecture doc.

## Notes

- Kafka runs in KRaft mode (no ZooKeeper) via the official `apache/kafka`
  image.
- `pyspark==3.5.3` / `delta-spark==3.3.1` is a verified-compatible pairing.
- Each Spark service's `depends_on: condition: service_started` only ensures
  container start order for readable logs -- the real safety comes from
  `wait_for_delta_table()` in `common.py`, which polls until the upstream
  table actually exists before starting a stream read on it.
- Every field in the mock CSV mirrors the real dataset's messy formatting
  (units embedded in strings, comma-formatted prices) on purpose, so the
  Silver transform you write against it transfers directly.

## Replay one partition and test source schema mismatch

The producer can select exact CSV basenames with `CSV_FILES`. For example, put
these values in `.env` to replay only the column-mismatch partition:

```dotenv
TABLES=Crash4
CSV_FILES=Crash4-2.csv
SOURCES=crash4
QWEN_ENABLED=true
```

Run `docker compose up --build -d`. `CSV_FILES` is optional: leaving it empty
keeps the existing all-partitions behavior for the selected tables. Kafka retains
old messages, so use a dedicated topic or isolated output/checkpoint directories
when repeating an experiment; restarting the producer republishes records.

Silver 1 validates JSON payload keys before parsing with its fixed schema. Schema
mismatches are stored in `silver1_dead_letter` with the original raw payload and
Kafka offsets, then sent to local Qwen. Invalid rows cannot enter Silver 1, Silver
2, or Gold. The schema guard does not rename fields or implement lookup/remediation.
When all source records have invalid schemas, downstream jobs may wait for a valid
Silver 1 table; this is expected and is not evidence of failed Qwen classification.
