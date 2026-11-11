# Recovery agent: the LangGraph loop (LG1)

Coordinates the recovery of one failure from Deterministic Lookup through Learn and
keeps the state of every recovery workflow.

```
lookup/ service --> kafka: recovery.lookup --> recovery-agent --> one LangGraph workflow per failure
                                                    |
                                                    +-- SQLite checkpoints (volume agent-state)
```

The graph, its routing, the saved state, the Kafka consumer, **Diagnose**, the
**policy gate** and **Remediate** are real. Validate and the steps after it are
still stubs that change nothing and fail closed, so today every failure still ends
`ESCALATED`: a fix that passes the gate is staged and audited by Remediate, then
the Validate stub does not pass it. Each stub is replaced by its own task:

| Node | File | Built in | Behaviour today |
|---|---|---|---|
| `deterministic_lookup` | `nodes/deterministic_lookup.py` | Task 102 | real: reads the lookup verdict, sets the route |
| `diagnose` | `nodes/diagnose.py` | Task 103 | real: asks the reasoning model for a cause and one fix |
| `policy_gate` | `nodes/policy_gate.py` | Task 103 | real: confidence threshold and safe-action set |
| `remediate` | `nodes/remediate.py` | Task 104 | real: idempotency check, stages the fix, writes the audit record |
| `validate` | `nodes/validate.py` | Task 105 | stub: does not pass |
| `escalate` | `nodes/escalate.py` | Task 106 | stub: records why it was escalated, asks nobody |
| `learn` | `nodes/learn.py` | Task 107 | stub: writes nothing to Neo4j |

## The graph (`graph.py`)

```mermaid
flowchart TD
    S([recovery.lookup stack item]) --> L[deterministic_lookup]
    L -- verified past fix --> R[remediate]
    L -- no reusable fix --> D[diagnose]
    D --> G[policy_gate]
    G -- confident and SAFE --> R
    G -- rejected --> E[escalate]
    R -- SUCCESS --> V[validate]
    R -- failed --> E
    V -- passed --> N[learn]
    V -- failed --> E
    E -- engineer outcome --> N
    N --> F([end])
```

`python -m agent.graph` prints the diagram of the graph as compiled, and a test
checks these edges.

## State of a workflow (`state.py`)

One workflow handles one item of a `recovery.lookup` stack. Each node returns only
its own keys:

| Written by | Keys |
|---|---|
| consumer | `lookup_id`, `failure_id`, `failure`, `lineage_context`, `incident_context`, `priority`, `lookup_route`, `route_reason` |
| `deterministic_lookup` | `route`, `action` (when a past fix is reused) |
| `diagnose` | `diagnosis` = `{status, hypothesis, confidence, proposed_action, model, allowed_actions, evidence, reasons}` |
| `policy_gate` | `policy` = `{passed, reasons, checks, threshold, safe_actions}`, and `action` when it passes |
| `remediate` | `execution` = `{status: SUCCESS or FAILED, execution_id, action, action_params, source, target_table, overlay, rows_expected, rows_affected, rows_with_other_failures, sample_record_ids, quarantine_version, executed_at, idempotent_replay, audit, reasons}` |
| `validate` | `validation` = `{passed, reasons}` |
| `escalate` | `escalation` = `{status, reason, details}`, `status` |
| `learn` | `learned`, `status` |
| every node | one `trace` entry, so the path can be read back |

`action` (`{action, action_params, source, incident_id}`) is the only thing Remediate
executes. Only two nodes may set it: `deterministic_lookup` (a fix that already
worked) and `policy_gate` (a proposal that passed both checks). A diagnosis alone
never reaches Remediate.

To build a step, rewrite its function in `agent/nodes/` so it returns the keys
above. The routing in `graph.py` does not need to change.

## Diagnose (`nodes/diagnose.py`, spec AP1)

Runs only for failures the lookup could not match to a verified past fix.

1. **Retrieve.** The lineage and the nearest past incidents were read from Neo4j by
   the Deterministic Lookup and arrive in the state (`lineage_context`,
   `incident_context`). Diagnose adds two things from the pipeline's Delta tables
   (`reference.py`, read only, no Spark): the column's canonical values from
   `ref_canonical_mappings`, and up to `DIAGNOSE_SAMPLE_RECORDS` quarantined records
   that carry the failure, as they looked in Silver 1. The records are what lets the
   model tell `Jule` = June from `Jule` = July: the `crash_date` is in the same row.
2. **Generate.** Everything is merged into one JSON context and sent to the reasoning
   model (`llm.py`: Qwen on Ollama, `/api/chat`, temperature 0). The answer is held to
   a JSON schema built for this failure: `action` can only be one of the actions that
   apply to it, `canonical_value` only one of the column's canonical values.
3. **Check the shape.** A reply with a missing hypothesis, an action that was not
   offered or a confidence outside 0..1 is not a diagnosis.

`diagnosis.status` says what happened:

| Status | Meaning | Proposal |
|---|---|---|
| `OK` | the model answered in the asked form | one action with parameters |
| `SKIPPED` | a mandatory field is empty, there is nothing to interpret; no model call | `ESCALATE` |
| `UNAVAILABLE` | Ollama not reachable, model not pulled, or timed out | none |
| `INVALID_OUTPUT` | the model answered something else than the JSON asked for | none |

Diagnose never raises and never sets `action`. With no model running, every new
failure is escalated with the reason `no usable diagnosis (UNAVAILABLE)`.

Text in the data (a raw value, a past diagnosis) is passed as data only, cut to 300
characters, and the system prompt tells the model not to follow it. That is a
request, not a guarantee, which is one reason the gate below trusts nothing the
model writes.

## Actions and the safe set (`actions.py`, spec AP2)

The model can only propose an action from this list. `Safe` means the policy gate
may let it run without an engineer:

| Action | Offered for | `action_params` | Safe | Why |
|---|---|---|---|---|
| `ADD_VARIANT` | a value failure on a column that has canonical values | `{"canonical_value": "JULY"}` | yes | Tried as an in-memory overlay on the quarantined rows of this failure only. The rows stay in quarantine unless Validate passes, and the mapping reaches `ref_canonical_mappings` only through Learn's promotion. |
| `RENAME_COLUMN` | a schema failure with missing and unexpected columns | `{"renames": {"CrashDate": "crash_date"}}` | no | Changes how every later batch of the table is read, not only the failed records. |
| `ESCALATE` | every failure | `{}` | no | Not a fix: the model asks for an engineer. |

To make another action safe, change `actions.py` in a reviewed PR. The setting
`POLICY_SAFE_ACTIONS` can only narrow the set (`NONE` sends every diagnosis to an
engineer); it cannot add to it.

A past fix reused by the Deterministic Lookup does not pass this gate: it goes
straight to Remediate, because it was already validated or approved by an engineer.

## Policy gate (`nodes/policy_gate.py`, spec AP2, AP3, RS1)

Deterministic, no model. The proposal becomes `action` only when every check passes:

| Check | Passes when |
|---|---|
| `safe_action` | the action is in the safe set (spec condition 1) |
| `confidence` | the model's confidence is at least `POLICY_CONFIDENCE_THRESHOLD`, default 0.85 (spec condition 2) |
| `trusted_failure` | the failure itself is not waiting for review (a Qwen classification with `needs_review`) |
| `parameters` | the parameters fit the failure: the canonical value exists for that column (read again from `ref_canonical_mappings`, not taken from the diagnosis), a rename uses the failure's own column names |
| `not_tried_before` | the same fix has not already failed for this failure (`incident_context.previous_attempts`) |

The last three only ever turn a pass into an escalation. `policy.checks` lists every
check with its result, and `policy.reasons` the ones that failed, so an engineer
sees why a proposal was stopped. On a pass, `action_params` is rebuilt from the
checked values: nothing else the model wrote reaches Remediate.

The gate cannot tell whether a confident, safe proposal is *correct*: a model that
says `Jule` is July at 0.95 passes. Catching that is Validate's job (Task 105), and
the threshold should be calibrated on labelled cases in the evaluation (Task 111).

## Remediate (`nodes/remediate.py`, spec RD1, RD2, RD3, TA2)

Runs for an `action` that came from the policy gate or from a reused past fix, in
the fixed order of the spec:

1. **Idempotency check.** `execution_id` is a hash of the failure, the action and
   its parameters. If the audit table already holds a `SUCCESS` for that id, this
   exact fix was executed before: the recorded result is returned
   (`idempotent_replay: true`) and nothing runs again. This is what protects a
   workflow that resumes at Remediate because the agent stopped after executing but
   before LangGraph saved the node. A `FAILED` attempt staged nothing, so it does
   not block the fix: if the same fix comes back (a resumed workflow, or a later
   retry once the cause is gone) it runs again. If the audit table cannot be read,
   nothing is executed.
2. **Execute.** The executor of the action (`executors.py`), see below.
3. **Audit record.** One row in the audit table for every attempt, failed ones too.
   `SUCCESS` is claimed only after that row is committed.

Remediate never raises. Everything that is not `SUCCESS` goes to Escalate with
`execution.reasons`: an action with no executor, parameters that no longer fit, a
reference table or quarantine that cannot be read (reported as that, not as a wrong
value), no record left to fix, records of the group that cannot be found, an audit
record that cannot be written.

### What executing `ADD_VARIANT` means (`executors.py`)

It **stages** the fix, it does not release it:

- The parameters are checked again against `ref_canonical_mappings` (a reused past
  fix never went through the policy gate).
- The failure's group is collected from `silver2_quarantine`: every record still
  waiting (`PENDING` or `IN_PROGRESS`) with the same folder, column and raw value
  (trimmed, upper-cased), from the micro-batches the failure was found in. These are
  exactly the rows the scanner grouped into the failure. The batches are the union
  of `batch_id` and `evidence.quarantine_batches`, because a failure the lookup
  merged from two publishes can carry the full list in only one of them.
- The group is staged whole or not at all. Rows already `RESOLVED` are accounted
  for, but if fewer rows are found than the failure reported, the execution fails:
  a fix applied to part of a group would leave the rest in quarantine with nobody
  looking for it.
- The overlay `{"crash_month": {"JULE": "JULY"}}` is built. It is the shape
  `ref_mappings.overlay_mappings()` takes, so Validate can apply it on top of the
  reference table when it re-runs `standardize_silver2` on the staged records.

Nothing in Silver 2, the quarantine or `ref_canonical_mappings` changes. The agent
mounts `./delta` read only and can write only `delta/remediation_audit`. A wrong
fix is therefore undone by discarding the staged group, which is what makes the
action safe. The records reach Silver 2 only after Validate passes (Task 105), and
the mapping reaches the reference table only through Learn's promotion (Task 107).

`RENAME_COLUMN` has no executor: even as a verified past fix it fails here and goes
to an engineer. A test checks that every safe action has an executor.

### The audit table (`audit.py`, `delta/remediation_audit`)

A Delta table written without Spark, one row and one commit per execution attempt.
An `execution_id` can have several `FAILED` rows and one `SUCCESS`. Only the agent
writes it; Spark and `verify_delta.py remediation_audit` can read it.

| Column | Meaning |
|---|---|
| `execution_id` | idempotency key: hash of failure, action and parameters |
| `failure_id`, `lookup_id` | the workflow it belongs to (`python -m agent.show <failure_id>`) |
| `action`, `action_params` | what was executed, parameters as JSON |
| `action_source`, `incident_id`, `confidence` | `DIAGNOSIS` with the model's confidence, or `INCIDENT` with the past incident |
| `status`, `reasons` | `SUCCESS`, or `FAILED` with why |
| `target_table`, `column_name`, `raw_value` | where the failure is |
| `overlay` | the staged mapping, as JSON |
| `rows_expected`, `rows_affected` | rows the failure reported, rows staged |
| `rows_with_other_failures` | staged rows that also fail another column, so they stay quarantined after this fix |
| `record_ids` | the affected records |
| `quarantine_version` | Delta version of the quarantine that was read |
| `started_at`, `executed_at` | execution timestamp (UTC) |

The row is the only thing Remediate writes and it is one Delta commit, so the
staged group and its audit record cannot exist without each other.

```bash
docker compose exec recovery-agent python -m agent.audit                  # latest 20 executions
docker compose exec recovery-agent python -m agent.audit <execution_id>   # its attempts, with record ids
docker compose exec recovery-agent python -m agent.audit <failure_id>
```

### What the next tasks take from it

- **Validate (105)** reads `execution.overlay` and the staged records:
  `audit.find(execution["execution_id"])["record_ids"]` with `target_table` (the
  state keeps only five sample ids; `find` returns the successful execution). Joining them to the quarantine on
  `record_id` + `target_table` and replaying `raw_record` through
  `standardize_silver2` with the overlay was tried on real tables: the targeted
  column resolves, and rows counted in `rows_with_other_failures` keep their other
  failure. Releasing the valid rows into Silver 2 and marking the quarantine rows
  `RESOLVED` is still to be built, after validation.
- **Escalate (106)** gets `execution.reasons` for a failed execution.
- **Pipeline cleanups (109)**: Remediate does not change `healing_status`. It accepts
  rows the trigger has marked `IN_PROGRESS` and never stages a `RESOLVED` row.

One agent consumes `recovery.lookup`: the service has a fixed `container_name`, so
Compose refuses to scale it. Two agent processes working on the same failure at the
same moment (a `docker compose run` next to the service) could both pass the
idempotency check and both write a `SUCCESS` row. Nothing else is written, and
every reader takes the first row, so they agree on one result; if the agent is ever
scaled out on purpose, the check needs a lock. The table gets one small file per
attempt, which is fine for the number of failure groups here and would need
compaction at a much larger scale.

## How state is kept (`consumer.py`)

LangGraph saves the state after every node in SQLite, under
`thread_id = failure.failure_id`. For each stack item the agent does one of:

- **new**: no saved state, start the workflow.
- **resumed**: the workflow stopped part-way (the agent crashed): continue from the
  last finished node. Nodes that already ran are not run again.
- **duplicate**: the workflow already ended: skip it. Kafka delivers at least once.

The Kafka offset is committed after the whole stack was handled. If a node raises,
the agent exits without committing, Compose restarts it and the message comes back.

## Run it

```bash
docker compose up -d --build recovery-agent
docker compose logs -f recovery-agent
```

```
[agent] lookup 66b035e5-...: 2 failure(s)
[agent] fd95d1c4c9991007 new       P3 crash1:crash_month value='Jule' -> ESCALATED (deterministic_lookup > diagnose > policy_gate > remediate > validate > escalate > learn)
[agent] 289b9d3430ea286e new       P3 crash1:weather_condition value='MISTY DARK' -> ESCALATED (deterministic_lookup > diagnose > policy_gate > escalate > learn)
```

The first failure passed the gate and was staged by `remediate` (it reached
`validate`, still a stub); the second was stopped by the gate. A path that ends
`remediate > escalate` is a failed execution. Diagnose needs Ollama with the model
pulled (`ollama pull qwen3:8b`); without it both lines end at `policy_gate > escalate`.

The first word is the `failure_id`. Print everything saved for that workflow:

```bash
docker compose exec recovery-agent python -m agent.show fd95d1c4c9991007
```

Settings (`config.py`): `LOOKUP_OUTPUT_TOPIC`, `AGENT_CONSUMER_GROUP`,
`AGENT_CHECKPOINT_PATH`, `AGENT_MAX_POLL_INTERVAL_MS`, and for Diagnose and the gate:

| Setting | Default | Meaning |
|---|---|---|
| `DIAGNOSE_MODEL` | `qwen3:8b` | reasoning model, any model Ollama serves (`ollama pull` it first) |
| `DIAGNOSE_BASE_URL` | `http://host.docker.internal:11434` | Ollama |
| `DIAGNOSE_TIMEOUT_SECONDS` | `120` | one model call |
| `DIAGNOSE_SAMPLE_RECORDS` | `3` | quarantined records shown to the model, 0 for none |
| `POLICY_CONFIDENCE_THRESHOLD` | `0.85` | minimum confidence to run without an engineer |
| `POLICY_SAFE_ACTIONS` | empty | empty = the safe set of `actions.py`; `NONE` = review everything |

Compose passes `DIAGNOSE_MODEL`, `DIAGNOSE_BASE_URL` and the two `POLICY_` settings
from `.env` (see `.env.example`). The model's context and reply size are
`DIAGNOSE_NUM_CTX` (8192) and `DIAGNOSE_NUM_PREDICT` (1024).

`REMEDIATION_AUDIT_PATH` (`/data/delta/remediation_audit`) is the audit table;
Compose mounts `./delta/remediation_audit` writable inside the read-only `./delta`.

To start from empty state, remove the volume:
`docker compose rm -sf recovery-agent && docker volume rm self-healing-pipeline_agent-state`.
The audit table is kept, so a failure that comes back is still not executed twice.

The agent's memory is in two places: the workflows in the `agent-state` volume and
the executions in `delta/remediation_audit`. When you reset the pipeline by deleting
`delta/` and `checkpoints/`, the audit table goes with it, so remove the
`agent-state` volume in the same step (command above). Otherwise the reprocessed
data produces the same failure ids and the agent skips them as workflows it has
already finished.

## Tests

No Kafka, Neo4j, Spark or model needed: the model is a fake, and the Delta reader
and the audit table run on small tables written to a temp folder. From the repo root:

```bash
pip install -r agent/requirements.txt pytest
python -m pytest agent
```
