# Replay tool and fault injector

Two command-line tools next to the pipeline. Both run the pipeline's own code from
`spark-jobs/` (Silver 1's SQL, Silver 2's `standardize_silver2` and write stage, the
reference mappings), never a copy of it.

| Tool | For | Spec |
|---|---|---|
| `python -m tools.replay` | reprocess one failed micro-batch, or one quarantine group, without re-running the dataset | RM1 |
| `python -m tools.fault_injector` | reproduce failures on demand, keep the right answer for each injected record, and follow each record afterwards | evaluation (Task 111), demo |

## Running them

They are one-off commands in the `tools` service (the Spark jobs' image, the repo
mounted read-only at `/repo`, `./delta` writable). It has a Compose profile, so
`docker compose up` does not start it:

```bash
docker compose run --rm tools python -m tools.replay list
docker compose run --rm tools python -m tools.fault_injector inject jule
```

Each command starts its own small Spark driver (`TOOLS_SPARK_DRIVER_MEMORY`, 1g)
next to the running jobs, so it takes 15 to 30 s before it prints anything. It
writes to the same Delta tables as the streaming jobs; Delta keeps the writers apart
and a write that loses a race is retried.

Outside Docker, run them from the repo root with the same environment the Spark jobs
get (`DELTA_ROOT`, `SOURCES`, `DAY_FIRST_SOURCES`, `KAFKA_BOOTSTRAP_SERVERS`, ...) and
`pyspark==3.5.3`, `delta-spark==3.3.1`, `confluent-kafka` installed.

## Replay tool (`tools/replay.py`, spec RM1)

Every record that did not reach Silver 2 is still kept, with what is needed to run it
again. The replay tool runs it again from there:

| Waiting in | What is kept | `replay` command | What it does |
|---|---|---|---|
| `silver1_dead_letter` | the raw Kafka message | `dead-letter silver1 --source S --batch N [--rename OLD=NEW] [--drop COL]` | fixes the payload's column names, runs Silver 1's own cleanup into `silver1_<source>`; Silver 2's stream takes it from there |
| `silver2_dead_letter` | the Silver 1 row | `dead-letter silver2 --source S --batch N` | runs Silver 2's two stages again (standardize, then write): valid rows to `silver2_<source>`, failing rows to the quarantine with the original batch id |
| `silver2_quarantine` | the Silver 1 row (`raw_record`) | `quarantine --source S [--column C] [--value V] [--batch B]` | re-runs the waiting rows with the **current** `ref_canonical_mappings`; rows that now pass go to `silver2_<source>` and are marked `RESOLVED` |

Every command is a **dry run** until you add `--apply`: it prints what would happen
(rows that pass, rows still failing and on which column, problems), and writes
nothing. `python -m tools.replay list` shows what is waiting: dead-lettered batches
per source and batch id, and the largest quarantine groups still `PENDING`.

```bash
docker compose run --rm tools python -m tools.replay list --source crash1
docker compose run --rm tools python -m tools.replay dead-letter silver1 --source crash1 --batch 12 --rename CrashDate=crash_date
docker compose run --rm tools python -m tools.replay dead-letter silver1 --source crash1 --batch 12 --rename CrashDate=crash_date --apply
```

### When to use which

- **A schema change upstream** (a column renamed): Silver 1's schema guard parks the
  records, Qwen classifies the exception, and the agent escalates it (`RENAME_COLUMN`
  is engineer-only and has no automated executor). The engineer fixes the source or
  decides the rename, replays the parked batch with `--rename CrashDate=crash_date`
  (and `--drop` for a column the pipeline does not know), then closes the case with
  `python -m agent.review resolved <failure_id> --by <name> --note "renamed and replayed"`.
  The batch is replayed whole or not at all: if any record of it still does not match
  the column contract after the renames, nothing is written and the problems are
  listed. A message that is not valid JSON (`CORRUPT_RECORD`) cannot be replayed.
- **A Silver 2 micro-batch that crashed** (`silver2_dead_letter`, e.g. after a code
  fix): `dead-letter silver2`. If the transform still fails, nothing is written and
  the batch stays parked.
- **Rows that became fixable after the fact**, without the agent:
  - a variant Learn promoted into `ref_canonical_mappings` (a record that failed two
    columns stays quarantined until both fixes are known; see agent/README.md, Validate),
  - canonical values an engineer added (for example the weather values that have no
    canonical target yet),
  - a fix in Silver 2's code.

  `quarantine --source crash1 --column weather_condition --value "FREEZING RAIN"`.
  Rows that still fail stay `PENDING`, untouched. No overlay is used and nothing is
  promoted: the replay trusts `ref_canonical_mappings` exactly as the Silver 2 job does.

**Records the agent is working on are left out.** A waiting row that Remediate staged
(a `SUCCESS` execution in `remediation_audit` lists it) belongs to an open workflow:
being validated, waiting for an engineer's approval, or a fix that did not hold.
Releasing it here would make that workflow's Validate stop at `rows_conserved` and
open another round for rows already fixed, so the command skips them and reports
`held_by_agent` with the execution ids (`python -m agent.audit <execution_id>`).
`--force` takes them anyway, for example after the engineer dismissed the case. By
default only `PENDING` rows are taken; `--include-in-progress` adds `IN_PROGRESS`.

Rows released here that another column was fuzzy-fixed in on the way get their
`silver2_fuzzy_audit` rows, as in the stream.

### Safe to run again

Every write is an idempotent Delta append with its own `txnAppId` at version 0:
`replay:silver1:<source>:<batch>_clean`, `replay:silver2:<source>:<batch>:valid|quarantine|fuzzy_audit`,
`replay:<hash of the released record ids>:release`. Delta skips an append that already
landed, so a replay that stopped half-way and is run again never writes a row twice.
What was replayed is marked so it is not selected again:

| Table | Mark |
|---|---|
| `silver1_dead_letter`, `silver2_dead_letter` | `healing_status = REPLAYED`, `replayed_by` (the replay id), `replayed_at` (columns added the first time) |
| `silver2_quarantine` | `healing_status = RESOLVED`, `resolved_by = "replay:<id>"`, `resolved_at` (the same columns Validate uses; Validate writes the execution id) |

A record that is already in Silver 2 is not appended again; its quarantine row is
only marked.

## Fault injector (`tools/fault_injector.py`, `tools/faults.py`)

It takes records that reached Silver 2 cleanly (never quarantined, never injected
before), corrupts one thing in each, and publishes them to the source's Kafka topic
(`crashes.<source>`) in the producer's envelope. From there they meet the real
Bronze, Silver 1, Silver 2, quarantine, lookup and recovery agent.

### What it can break (`python -m tools.fault_injector kinds`)

| Kind | Columns | Example | Should end up | Correct recovery (`expected_action`) |
|---|---|---|---|---|
| `typo` | weather, lighting, street direction, month, weekday | `JUNE` -> `Jnue` | fuzzy-fixed in Silver 2 | `NONE` (Silver 2 fixes it) |
| `ambiguous` | crash_month, crash_day_of_week | `JUNE` -> `Jule`, `MAY` -> `Ma`, `SUNDAY` -> `S` | quarantine | `ADD_VARIANT` to the true value |
| `unseen_variant` | the vocabulary columns | `RAIN` -> `RAINING`, `JULY` -> `Julio` | quarantine | `ADD_VARIANT` to the true value |
| `unseen_format` | crash_date, date_police_notified, crash_hour, posted_speed_limit | `2021-06-22` -> `22nd of June 2021` | quarantine | `ESCALATE` (no canonical set) |
| `unknown_value` | weather, lighting, damage | `MISTY DARK` | quarantine | `ESCALATE` (no right answer) |
| `missing_mandatory` | crash_date | empty | quarantine (`MISSING_MANDATORY_VALUE`) | `ESCALATE` |
| `schema_rename` | any | `crash_date` -> `CrashDate` | Silver 1 dead letter | `RENAME_COLUMN`, then `tools.replay` |
| `schema_missing` | any | `lane_cnt` dropped | Silver 1 dead letter | `ESCALATE` |
| `schema_extra` | a new name | `vehicle_color: RED` | Silver 1 dead letter | `ESCALATE` (or replay with `--drop`) |
| `corrupt_record` | | the message cut in half | Silver 1 dead letter (`CORRUPT_RECORD`) | `ESCALATE` |

The true value of a record is what Silver 2 made of the clean original, so it is never
guessed. For `ambiguous`, base records are chosen whose true value the spelling can
stand for (`Jule` only in June or July records), so the record's own `crash_date`
decides, as in the spec's example. Generated typos are only used when Silver 2's fuzzy
guardrails accept them (`MARCH` -> `Marh` ties with `MAY` and is skipped).

### Scenarios (`data/faults/*.json`)

```json
{"name": "jule", "description": "...",
 "faults": [{"kind": "ambiguous", "source": "crash1", "column": "crash_month",
             "value": "Jule", "base": "JUNE", "count": 4, "batches": 3}]}
```

| Field | Meaning |
|---|---|
| `kind`, `source`, `column` | what to break and where (`source` defaults to `crash1`) |
| `value` | the corrupted value (default: picked from the catalog per record) |
| `base` | only corrupt records whose true value is this (`JUNE`) |
| `to` | `schema_rename`: the new name (default CamelCase) |
| `count`, `batches` | records per batch (5) and batches (1). Batches are sent `--interval` seconds apart (15) so they land in separate micro-batches, which promotion counts (N occurrences across M batches) |

Shipped: `jule` (the spec's worked example, 12 records in 3 batches), `schema-rename`
(3 records, for the rename + replay story), `all-kinds` (2 of every kind in Crash1 and
Crash3, a smoke test of the whole recovery path). One fault without a file:

```bash
docker compose run --rm tools python -m tools.fault_injector inject --kind ambiguous --column crash_month --value Jule --base JUNE --count 5 --batches 3
```

`--dry-run` plans and predicts without sending or writing anything; `--seed` changes
which records are picked; `--run-id` names the run (default `<scenario>-<UTC time>`).

### The ground truth (`delta/fault_injections`)

Written **before** anything is sent, one row per injected record; `published_at` is set
when Kafka acknowledged its batch.

| Column | Meaning |
|---|---|
| `run_id`, `scenario`, `fault_index`, `seq`, `batch` | which run, which fault of the scenario, record number, batch |
| `record_id` | the envelope's `record_id` (uuid5 of run id and seq): the key into every pipeline table |
| `crash_record_id` | `FAULT-<run_id>-<seq>`, so injected copies can be left out of Gold metrics |
| `base_record_id` | the clean record it was copied from |
| `kind`, `source`, `folder`, `target_table`, `kafka_topic`, `column`, `renamed_to` | what was broken where |
| `original_raw`, `injected_raw` | the value before (Silver 1) and the one sent |
| `original_value`, `expected_value` | Silver 2's value of the clean record; the value a correct recovery ends with (null when there is no right answer) |
| `expected_action`, `expected_params` | `ADD_VARIANT {"canonical_value": ...}`, `NONE`, `RENAME_COLUMN {"renames": ...}` or `ESCALATE` |
| `intended_path` | `FUZZY_FIXED`, `QUARANTINED` or `DEAD_LETTER` (from the kind) |
| `predicted_path`, `predicted_value`, `predicted_failures`, `as_intended` | what Silver 1's schema guard and SQL and `standardize_silver2` (with the reference table at that moment) make of the record, computed before sending. `CLEAN` means the value is already mapped, for example after Learn promoted it |
| `planned_at`, `published_at` | timestamps (UTC) |

### Following a run (`status`)

```bash
docker compose run --rm tools python -m tools.fault_injector runs
docker compose run --rm tools python -m tools.fault_injector status jule-20261114t051500 --records
docker compose run --rm tools python -m tools.fault_injector status jule-20261114t051500 --json
```

`status` looks every record up in `silver1_dead_letter`, `silver1_<source>`,
`silver2_quarantine`, `silver2_<source>`, `silver2_fuzzy_audit` and
`remediation_audit`, and reports per record:

| Field | Meaning |
|---|---|
| `outcome` | `NOT_ARRIVED`, `IN_SILVER1`, `DEAD_LETTER`, `QUARANTINED`, `HEALED` (released by the agent's Validate), `HEALED_BY_REPLAY`, `REPLAYED` (a dead-lettered record re-run, now in Silver 2), `FUZZY_FIXED`, `CLEAN` |
| `value_in_silver2`, `correct` | the value it ended with, and whether it equals `expected_value` (null while it is not in Silver 2 or there is no right answer) |
| `quarantine_status`, `quarantine_failures`, `dead_letter_status` | where it waits |
| `resolved_by`, `fix_source`, `fix_action`, `fix_params`, `failure_id` | who fixed it: the execution in `remediation_audit` (`action_source` `INCIDENT` = a reused past fix without the model, `DIAGNOSIS` = the model through the gate, `ENGINEER`), or `REPLAY` |
| `published_at`, `quarantined_at`, `healed_at`, `seconds_to_heal` | timing |

and a summary: outcomes per kind, correct and wrong counts.

### For the evaluation (Task 111)

The injector gives each record's truth and `status` gives each record's fate; the
metrics are Task 111's. A sketch of the mapping:

| Metric | From |
|---|---|
| auto-resolution rate | `outcome = HEALED` and `fix_source` in (`INCIDENT`, `DIAGNOSIS`) over records with `intended_path = QUARANTINED` |
| resolved without the LLM | `fix_source = INCIDENT` (and `CLEAN` for a variant learned earlier) |
| escalation rate | records still `QUARANTINED` whose failure has an open case (`python -m agent.review`, or the agent's checkpoints by `failure_id`) |
| wrong-fix rate | `correct = false` among healed records; also `fix_params` against `expected_params` |
| time to recover | `seconds_to_heal` |

The failure a record belonged to is in `remediation_audit.failure_id` once it was
fixed; for records still waiting, join `silver2_quarantine` on `record_id` and the
scanner's grouping (folder, column, value). `status --json` is the input to build on.

### Things to know

- Injected records are copies of real crashes. They are marked by `crash_record_id
  LIKE 'FAULT-%'`; anything that counts crashes in Gold should leave them out. To
  start clean, wipe `delta/` and `checkpoints/` as usual (the agent's `agent-state`
  volume too, see agent/README.md).
- Base records are drawn from `silver1_<source>` joined with `silver2_<source>`, so the
  pipeline must have run for that source first.
- A run id can be injected once (`run ... was already injected`).
- `predicted_path` is computed with the reference table at injection time. Records
  that will not take the intended path are counted in the output (`as_intended = false`).
- Records still waiting for Silver 2's micro-batch show `IN_SILVER1`; the Spark jobs
  trigger every 10 s.

## Tests

```bash
pip install pyspark==3.5.3 delta-spark==3.3.1 confluent-kafka pytest
python -m pytest tools
```

`tools/tests/test_faults.py` checks the catalog without Spark. `test_tools_spark.py`
builds a small real pipeline with the jobs' own code from the first 400 rows of
`data/Crash1` and `data/Crash3`, injects `all-kinds` (Kafka replaced by appending the
messages to Bronze), runs Silver 1 and Silver 2 on them, follows them with `status`, and
replays a promoted quarantine group, a renamed Silver 1 batch and a crashed Silver 2
batch. It needs Java and takes about 3 minutes; without Java it is skipped.
