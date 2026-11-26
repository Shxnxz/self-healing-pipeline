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

Slash and dash dates are ambiguous (`01/03/2021`), so each source declares its
order. `DAY_FIRST_SOURCES` (default `crash3`) lists the sources that write
`dd/MM/yyyy`; every other source is read `MM/dd/yyyy`. Silver 2 refuses to start if
`DAY_FIRST_SOURCES` is set to something that is not in `SOURCES` (the default `crash3`
is exempt, so a run with `SOURCES=crash4` needs no change). Day-first sources are
strict (a value like `02/24/2021` goes to quarantine); month-first sources still fall
back to day first for values like `25/12/2021`.

Records that Silver 2 cannot standardize wait in `silver2_quarantine` instead of
reaching Gold. The self-healing half picks them up from there:

```
silver2_quarantine --lookup-scanner--> recovery.failures --> deterministic-lookup --> recovery.lookup
                                                             (Neo4j: lineage, past incidents)  |
                                                                                               v
  recovery-agent, one LangGraph workflow per failure:
  deterministic_lookup -> diagnose -> policy_gate -> remediate -> validate -> learn
                                          |              |           |           |
                                          v              v           v           v
                              escalate: waits for an engineer   silver2_crashN   Neo4j incidents,
                              (python -m agent.review, via      (rows released)  ref_canonical_mappings
                               kafka: recovery.decisions)                         (variants promoted)
```

1. **Detect and rank** ([lookup/](lookup/README.md)): the scanner groups new quarantine
   rows into failures; the Deterministic Lookup scores each one's blast radius from the
   lineage graph and checks Neo4j for a past fix that can be reused.
2. **Recover** ([agent/](agent/README.md)): one LangGraph workflow per failure.
   A reused past fix, or a fix a reasoning model (Qwen on Ollama) proposes and the
   policy gate accepts, is staged and audited (Remediate), replayed through the real
   Silver 2 code and checked (Validate), and its records are released into Silver 2.
   Anything else waits for an engineer's decision.
3. **Learn**: every outcome becomes an incident in Neo4j, so the next occurrence is
   found by the lookup, and a variant an engineer approved is promoted into
   `ref_canonical_mappings`, so Silver 2 maps it by itself from then on.

Lineage (OpenLineage from every Spark job into Neo4j), a replay tool and a fault
injector for evaluation complete it. Each has its own section below.

## Prerequisites

- Docker + Docker Compose v2
- ~8 GB RAM free for the whole stack: Kafka, two Spark JVMs (the Silver 2 + Gold driver
  uses 2 GB), Neo4j (~1 GB) and the recovery agent (~1.5 GB once Validate has started
  its Spark session). The pipeline alone fits in ~5 GB.
- Optional: [Ollama](https://ollama.com) on the host with the reasoning model pulled
  (`ollama pull qwen3:8b`) for the agent's Diagnose step. Without it the pipeline and
  the reuse of past fixes still work; every new kind of failure goes to an engineer.

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
- **silver2-gold-job logs** — one Spark driver running 6 queries:
  - `[silver2_crash1]` standardizes Silver 1 -> `delta/silver2_crash1`, failures -> `delta/silver2_quarantine`
  - `[gold]` streams the UNION ALL of every Silver 2 table into `delta/gold`
  - `[gold_metrics]` refreshes the `gold_*` business tables every minute ([docs/gold-metrics.md](docs/gold-metrics.md))
- **lineage-consumer logs** — `[lineage] write_gold COMPLETED (n of m events written)`
- **Neo4j browser** — http://localhost:7474 (user `neo4j`, password `NEO4J_PASSWORD`, default
  `selfhealing`) — try `MATCH p=(:Table)-[:FEEDS*]->(:Table) RETURN p`
- **lookup-scanner / deterministic-lookup logs** — failures found in the quarantine and
  the ranked stack published to `recovery.lookup`
- **recovery-agent logs** — one line per failure with its path through the graph and
  how it ended (`RESOLVED`, or `ESCALATED ... waiting for an engineer`); open cases with
  `docker compose exec recovery-agent python -m agent.review`

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
  `GOLD_TRANSFORM_SQL` for business-ready fields; `gold_metrics.py` builds the `gold_*`
  aggregate tables on it ([docs/gold-metrics.md](docs/gold-metrics.md)).
- **`spark-jobs/sources.py`, `ref_mappings.py`, `schema_guard.py`** — the per-source
  table layout, the reference mappings (`ref_canonical_mappings`, seeded on start) with
  Damerau-Levenshtein fuzzy matching, and Silver 1's check of the payload's columns.
- **`qwen_classifier/`** — optional Qwen triage of Spark exceptions and schema
  mismatches into incidents (`QWEN_ENABLED`, see [qwen_classifier/README.md](qwen_classifier/README.md)).
- **`lineage/`** — pipeline lineage in Neo4j. See [Lineage](#lineage) below.
- **`lookup/`** — quarantine scanner and Deterministic Lookup. See
  [Deterministic Lookup](#deterministic-lookup).
- **`agent/`** — the recovery agent. See [Recovery agent](#recovery-agent-langgraph-loop).
- **`tools/`** — replay tool and fault injector; `docker-compose.eval.yml` is the
  separate evaluation stack the injector runs in. See
  [Replay and fault injection](#replay-and-fault-injection).
- **`evaluation/`** — the evaluation harness and the "Jule" demo on the evaluation
  stack ([docs/evaluation.md](docs/evaluation.md), [docs/demo.md](docs/demo.md)).
- **`delta/`** — every Delta table: per source `bronze_<source>`, `silver1_<source>`,
  `silver2_<source>`; shared `silver1_dead_letter`, `silver2_quarantine`,
  `silver2_dead_letter`, `silver2_fuzzy_audit`, `ref_canonical_mappings`, `gold` and the
  `gold_*` aggregates; the agent's `remediation_audit`. Inspect with `verify_delta.py`.
- **`checkpoints/`** — Structured Streaming's checkpoint state per job.
- **Docker volumes** — `neo4j-data` (lineage and incidents), `agent-state` (the agent's
  saved workflows), `lookup-state` (the scanner's progress), `ivy-cache` (Spark jars).

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

**When Neo4j is down, nothing is lost.** `lineage-consumer` commits a Kafka offset only
after Neo4j accepted the event. While Neo4j is down, restarting, busy or refusing the
password, the consumer keeps the event and tries it again: the neo4j driver itself
retries for 30 s, then the consumer waits 1, 2, 4 ... up to 30 s between attempts and
logs `[lineage] Neo4j unavailable (ServiceUnavailable); retrying in 4s`. Events that
pile up in Kafka meanwhile are written once Neo4j is back. A wrong `NEO4J_PASSWORD`
looks the same in the log (`AuthError`): fix the password and the consumer carries on
from where it stopped. Only an event Neo4j itself rejects is logged
(`Skipping event at offset ...`) and skipped, so one bad event cannot stop the stream.
Every write is a `MERGE`, so an event written twice changes nothing.

**Turning lineage off.** The listener is added only when `OPENLINEAGE_ENABLED` is
`true` (the default) and `OPENLINEAGE_KAFKA_BOOTSTRAP_SERVERS` is set; any other value,
or no Kafka address, turns it off. Compose does not pass `OPENLINEAGE_ENABLED` to the
Spark services, so add `OPENLINEAGE_ENABLED: "false"` to their `environment` (or in a
`docker-compose.override.yml`) and recreate them. Without the listener:

- the jobs run exactly the same (every Spark test in this repository runs with it off);
- no new lineage reaches Neo4j, and what is there stays;
- for a table the graph does not know, the Deterministic Lookup falls back to the fixed
  chain bronze → silver1 → silver2 → gold for the blast radius and marks the failure
  `lineage_known: false` (see [lookup/README.md](lookup/README.md)).

Neo4j only applies `lineage/schema.cypher` when `neo4j-init` runs. If your Neo4j
container already exists, apply the new constraints once with
`docker compose up neo4j-init`.

Tests: `cd lineage && python -m pytest tests`. They need neither Kafka nor Neo4j:
`test_normalize.py` runs on real captured Spark events, `test_consumer.py` covers the
retry and commit order above with a fake Kafka consumer and the real neo4j driver
against a closed port. With a Neo4j running, two more tests use it: Neo4j coming back
while the consumer waits, and a wrong password:

```bash
docker run -d --name neo4j-test -p 7687:7687 -e NEO4J_AUTH=neo4j/selfhealing neo4j:5.26-community
cd lineage && NEO4J_TEST_URI=bolt://localhost:7687 python -m pytest tests
```

`spark-jobs/test_openlineage_config.py` checks what `OPENLINEAGE_ENABLED` does to the
Spark session without starting one.

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

## Replay and fault injection

`tools/` has two one-off commands that run the pipeline's own Spark code
([tools/README.md](tools/README.md)):

- `python -m tools.replay` reprocesses one dead-lettered micro-batch (Silver 1 after a
  column rename, Silver 2 after a crash) or a quarantine group that the current
  reference table now fixes, without re-running the dataset (spec RM1).
- `python -m tools.fault_injector` copies clean records, breaks one thing in each
  (typos, ambiguous values, unseen variants and formats, schema changes, broken
  messages), publishes them to Kafka, keeps the right answer for each in
  `fault_injections`, and `status` follows every record through the pipeline. It
  only runs in the separate evaluation stack (`docker-compose.eval.yml`: its own
  Delta folder `./eval`, Kafka and Neo4j), so the agent never learns fixes from
  test records in the real pipeline.

```bash
docker compose run --rm tools python -m tools.replay list
docker compose -f docker-compose.yml -f docker-compose.eval.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.eval.yml run --rm tools python -m tools.fault_injector inject jule
```

`python -m evaluation` runs a whole experiment on that stack (a fixed fault scenario,
an oracle engineer, two rounds) and writes the metrics: see
[docs/evaluation.md](docs/evaluation.md). [docs/demo.md](docs/demo.md) walks through
one value, "Jule", from injection to promotion.

## Recovery agent (LangGraph loop)

`agent/` reads `recovery.lookup` and runs one LangGraph workflow per failure:
deterministic lookup, diagnose, policy gate, remediate, validate, escalate, learn.
The state of every workflow is saved after each step (SQLite, volume `agent-state`),
so a crashed workflow resumes and a redelivered one is skipped. A later batch of the
same problem joins the workflow still open for it instead of starting another. See
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

A past fix the lookup found for the same value is reused without asking the model,
also when it is borrowed from a merely similar incident. An optional setting, off by
default, makes such a borrowed fix on a P1 failure wait for an engineer's approval
(`APPROVE_BORROWED_FIX_TIERS=P1`; default `NONE`).

Remediate executes an approved fix exactly once: it checks the audit table first,
stages the failure's quarantined records with the proposed mapping, and writes one
audit record (action, parameters, affected records, timestamp) to the Delta table
`delta/remediation_audit`. Staging changes nothing in Silver 2, the quarantine or the
reference table. Print the records with
`docker compose exec recovery-agent python -m agent.audit`.

Validate replays the staged records through the Silver 2 job's own code in a small
Spark session inside the agent and checks the result against every record (including
`crash_month` / `crash_day_of_week` against `crash_date`). Only if every check passes
are the records appended to their `silver2_<source>` table, so Gold picks them up, and
marked `RESOLVED` in the quarantine.

Everything the agent cannot fix safely becomes a **case for an engineer**: the workflow
pauses (it survives restarts) until someone decides with `python -m agent.review`
(`approve`, `fix VALUE`, `retry`, `resolved`, `dismiss`). An engineer's fix is staged and
validated like any other.

**Learn** stores every outcome as an incident in Neo4j. A variant joins
`ref_canonical_mappings` only when an engineer approves it: at once when an engineer's
fix held, and for a fix the agent made on its own (reused from memory, never promoted
by itself) when an engineer runs `python -m agent.review promotions` and
`python -m agent.review promote <incident_id> --by NAME`. Records still waiting for a
promoted value are released with `python -m tools.replay quarantine`.

A workflow ends `RESOLVED`, `RESOLVED_BY_ENGINEER`, `DISMISSED` or `FAILED`, and is
`ESCALATED` while it waits; a failure that joined an open workflow ends `ATTACHED`.
Everything a workflow did is printed by
`docker compose exec recovery-agent python -m agent.show <failure_id>`. Failures the
agent handled while its steps were still stubs can be run again:
[agent/README.md](agent/README.md#failures-seen-while-steps-were-still-stubs).

## Tests and CI

Every pull request and every push to `main` and `Golf` runs all suites on GitHub Actions
(`.github/workflows/tests.yml`, four jobs in parallel, Python 3.11 like the service
images, Java 17 for Spark, a Neo4j service for the tests that need a real one). None of
them needs Kafka, Ollama or the Docker images. To run them locally, from the repo root:

| Suite | Install | Run | Needs |
|---|---|---|---|
| Lineage consumer | `pip install -r lineage/requirements.txt pytest` | `cd lineage && python -m pytest tests` | nothing (real-Neo4j tests with `NEO4J_TEST_URI`) |
| Deterministic Lookup | `pip install -r lookup/requirements.txt pytest pyyaml` | `python -m pytest lookup/tests` | nothing |
| Qwen triage, handoff, schema guard | `pip install -r lineage/requirements.txt pytest` | `python -m pytest tests/test_qwen.py tests/test_handoff.py tests/test_schema_guard.py` | nothing (the model is faked) |
| Spark jobs | `pip install -r spark-jobs/requirements.txt pytest` | `cd spark-jobs && python -m pytest test_sources.py test_date_order.py test_openlineage_config.py test_damerau_levenshtein.py -k "not test_spark"`, then `python -m pytest test_per_source_pipeline.py`, `python -m pytest test_gold_metrics.py` and `python test_damerau_levenshtein.py`, each in its own process (a Spark session left by one would be reused by the next) | Java 17+ |
| Recovery agent | `pip install -r agent/requirements.txt pytest pyyaml` | `python -m pytest agent` | Java for `test_validate_spark.py`, `NEO4J_TEST_URI` for `test_incidents_neo4j.py` (both skipped without) |
| Replay tool, fault injector | `pip install -r spark-jobs/requirements.txt deltalake pyarrow pytest` | `python -m pytest tools/tests` | Java, `docker compose` for `test_eval_stack.py` |
| Evaluation harness | `pip install pytest` | `python -m pytest evaluation/tests` | nothing (standard library) |

The Spark suites build real Delta tables with the pipeline's own jobs from rows of
`data/` and download the Delta and Kafka jars on the first run (a few minutes in all).
Two scripts are not part of CI because they need the running stack:
`spark-jobs/test_delta_pipeline.py` (reads `/data/delta` inside a Spark container) and
`tests/integration_qwen_conditions.py` (needs Kafka and Ollama).

## Adding a data folder

The four folders `data/Crash1` .. `data/Crash4` are the team's dirtied copy of an
official crash dataset (~1M records). A new folder, say `data/Crash5`, needs the same
49 columns and:

1. `TABLES` on the producer and `SOURCES` on the Spark services (`.env`) both list it
   (`Crash5` / `crash5`).
2. If its slash dates are day first, add it to `DAY_FIRST_SOURCES`.
3. The recovery agent needs a writable mount for `silver2_crash5` next to the other
   `silver2_<source>` lines in `docker-compose.yml` (and in `docker-compose.eval.yml` to
   evaluate it); `agent/tests/test_compose.py` fails until it is there.

Nothing else is per source: Gold, the quarantine, the lookup and the agent pick it up
by name.

## Known limitations

- **Gold** is the union of the Silver 2 tables with a few derived fields, plus five
  business aggregate tables (`spark-jobs/gold_metrics.py`,
  [docs/gold-metrics.md](docs/gold-metrics.md)). The two-table Silver join is not built.
- **Weather values without a canonical target** (`OTHER`, `FREEZING RAIN/DRIZZLE`,
  `FOG/SMOKE/HAZE`, `SLEET/HAIL`, `BLOWING SNOW`, ...) stay quarantined until canonical
  values are chosen for them. The agent can only map a value to a canonical value that
  is already in `ref_canonical_mappings`.
- **Schema fixes** (`RENAME_COLUMN`) are never executed by the agent: it has no executor,
  so a renamed column is fixed by hand, closed with `agent.review resolved`, and the
  dead-lettered batch replayed with `tools.replay dead-letter silver1 --rename`.
- **Diagnose has not been verified with the real model** in this repository's tests
  (the model is faked). Run `python -m agent.check_model` once against your Ollama. The
  confidence thresholds (0.85 / 0.95) and the lookup's similarity threshold (0.85) are
  not calibrated yet; that is the evaluation's job.
- **One agent process.** The service has a fixed `container_name`; two agents on the
  same failure could both stage it. `recovery.decisions` is not authenticated: whoever
  can write to Kafka can decide a case.
- **A problem an engineer dismissed opens a new case on its next batch.** A value that
  keeps arriving attaches to its open workflow (one case, one model call); a new
  workflow starts only after the previous one reached Validate or ended, and a
  dismissed workflow has ended. Checkpoints of finished workflows are pruned and the
  audit table is compacted by `agent/upkeep.py`.
- Not tested on Docker Desktop for Windows: the agent's `deltalake` writes onto the
  bind-mounted `./delta` (audit table, promotions).

## Notes

- Kafka runs in KRaft mode (no ZooKeeper) via the official `apache/kafka`
  image.
- `pyspark==3.5.3` / `delta-spark==3.3.1` is a verified-compatible pairing.
- Each Spark service's `depends_on: condition: service_started` only ensures
  container start order for readable logs -- the real safety comes from
  `wait_for_delta_table()` in `common.py`, which polls until the upstream
  table actually exists before starting a stream read on it.
- The data in `data/` was made messy on purpose (misspelt months and weekdays, mixed
  date formats, number words, renamed and missing columns in some partitions), so every
  path of the pipeline and the agent has real cases to work on.

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
