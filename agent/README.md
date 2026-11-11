# Recovery agent: the LangGraph loop (LG1)

Coordinates the recovery of one failure from Deterministic Lookup through Learn and
keeps the state of every recovery workflow.

```
lookup/ service --> kafka: recovery.lookup --> recovery-agent --> one LangGraph workflow per failure
                                                    |
                                                    +-- SQLite checkpoints (volume agent-state)
```

The graph, its routing, the saved state, the Kafka consumer, **Diagnose** and the
**policy gate** are real. Remediate and the steps after it are still stubs that
change nothing and fail closed, so today every failure still ends `ESCALATED`: a
diagnosis that passes the gate reaches the Remediate stub, which reports that it
did nothing. Each stub is replaced by its own task:

| Node | File | Built in | Behaviour today |
|---|---|---|---|
| `deterministic_lookup` | `nodes/deterministic_lookup.py` | Task 102 | real: reads the lookup verdict, sets the route |
| `diagnose` | `nodes/diagnose.py` | Task 103 | real: asks the reasoning model for a cause and one fix |
| `policy_gate` | `nodes/policy_gate.py` | Task 103 | real: confidence threshold, safe-action set and a check against the records |
| `remediate` | `nodes/remediate.py` | Task 104 | stub: executes nothing, reports `NOT_IMPLEMENTED` |
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

**A step that raises does not stop the agent.** The graph retries it
(`AGENT_NODE_MAX_ATTEMPTS`, default 3, waiting `AGENT_NODE_RETRY_SECONDS` and then
twice as long). If it still raises, the error is saved in `error` and the failure
goes to Escalate with reason `STEP_FAILED`. This is also why `deterministic_lookup`
and `diagnose` have an edge to `escalate` that the diagram leaves out. If Escalate
or Learn itself keeps raising, the workflow ends with status `FAILED`. Because of
the retry, **a step must be safe to run more than once**.

## State of a workflow (`state.py`)

One workflow handles one item of a `recovery.lookup` stack. Each node returns only
its own keys:

| Written by | Keys |
|---|---|
| consumer | `lookup_id`, `failure_id`, `failure`, `lineage_context`, `incident_context`, `priority`, `lookup_route`, `route_reason` |
| `deterministic_lookup` | `route`, `action` (when a past fix is reused) |
| `diagnose` | `diagnosis` = `{status, hypothesis, confidence, proposed_action, model, allowed_actions, evidence, reasons}` |
| `policy_gate` | `policy` = `{passed, outcome, reasons, checks, threshold, unverified_threshold, safe_actions}`, and `action` when it passes |
| `remediate` | `execution` = `{status: SUCCESS or FAILED, ...}` |
| `validate` | `validation` = `{passed, reasons}` |
| `escalate` | `escalation` = `{status, reason, details}`, `status` |
| `learn` | `learned`, `status` |
| every node | one `trace` entry, so the path can be read back |
| a stub | its name in `stubbed`. Delete that line when you build the step |
| the graph | `error` = `{node, error, attempts}` when a step kept raising |

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

Qwen3 thinks before it answers unless told not to, and the thinking counts against
`DIAGNOSE_NUM_PREDICT`, so a long one cuts the JSON off (`INVALID_OUTPUT`: "reply was
cut off"). The request therefore carries `think: false` (`DIAGNOSE_THINK`). A model
without a thinking mode that refuses the field is asked once more without it.

Check the model once before relying on it, and again after changing `DIAGNOSE_MODEL`:

```bash
docker compose exec recovery-agent python -m agent.check_model
```

It sends one fixed example (`Jule` in three records dated in June) through the real
Diagnose and gate, prints the diagnosis, the gate's verdict and the time it took, and
exits 0 when the model answered in the asked form. It needs only Ollama.

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

A past fix reused by the Deterministic Lookup does not pass the gate below: it was
already validated or approved by an engineer. It is still checked against the failure
with the gate's own `parameters` check (`actions.check_params`) before it becomes
`action`, so a fix whose canonical value has since left `ref_canonical_mappings`, or
that does not fit this failure, is sent to Diagnose instead of to Remediate.

## Policy gate (`nodes/policy_gate.py`, spec AP2, AP3, RS1)

Deterministic, no model. The proposal becomes `action` only when every check passes:

| Check | Passes when |
|---|---|
| `safe_action` | the action is in the safe set (spec condition 1) |
| `confidence` | the model's confidence is at least `POLICY_CONFIDENCE_THRESHOLD`, default 0.85 (spec condition 2) |
| `trusted_failure` | the failure itself is not waiting for review (a Qwen classification with `needs_review`) |
| `parameters` | the parameters fit the failure: the canonical value exists for that column (read again from `ref_canonical_mappings`, not taken from the diagnosis), a rename uses the failure's own column names |
| `evidence` | `ADD_VARIANT` only: the quarantined records do not contradict the value, see below |
| `not_tried_before` | the same fix has not already failed for this failure (`incident_context.previous_attempts`) |

The last four only ever turn a pass into an escalation. On a pass, `action_params` is
rebuilt from the checked values: nothing else the model wrote reaches Remediate.

### Evidence: is the value right, not only valid? (`evidence.py`)

A confident model can be wrong, and `parameters` only proves that the value exists.
For two columns the record itself carries the answer, so the gate reads the failure's
sample records from quarantine again (all of `sample_record_ids`, not the three shown
to the model) and compares:

| Column | Must agree with |
|---|---|
| `crash_month` | the month of `crash_date` |
| `crash_day_of_week` | the weekday of `crash_date` |

| What the records say | Result |
|---|---|
| at least one decides the value, all for the proposal | passes at the normal threshold |
| at least one rules the proposal out (`Jule` proposed as JULY, `crash_date` in June) | rejected, whatever the confidence |
| nothing decides: another column (weather, lighting, ...), or no readable date | passes only at `POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD` (0.95), where the model's confidence is all there is. `NEVER` sends all of these to an engineer. |

A date like `04/07/2019` is read both ways (7 April or 4 July) because the order
depends on the source folder. It confirms a month only when both readings agree or
only one is a real date (`22/06/2024`), but it still rules out every other month.

This narrows the gap, it does not close it. The gate sees a handful of sample
records, so agreement across every record of the failure stays Validate's check
(Task 105), and both thresholds are uncalibrated until the evaluation (Task 111).

### What the gate writes

`policy.outcome` says what happened, `policy.checks` lists every check with its
result and `policy.reasons` the ones that failed:

| `outcome` | Meaning |
|---|---|
| `PASSED` | every check passed; the proposal is now `action` |
| `REJECTED` | a fix was proposed and a check stopped it |
| `ENGINEER_REQUESTED` | Diagnose itself proposed `ESCALATE` (the evidence did not single out a fix, or a mandatory field is empty); no fix was blocked |
| `NO_DIAGNOSIS` | there was nothing to judge: model unreachable or its reply unusable |

The escalation still reads `POLICY_GATE_REJECTED` for the last three; wording it per
outcome belongs to Escalate (Task 106), which can read `policy.outcome`.

## How state is kept (`consumer.py`)

LangGraph saves the state after every node in SQLite, under
`thread_id = failure.failure_id`. For each stack item the agent does one of:

- **new**: no saved state, start the workflow.
- **resumed**: the workflow stopped part-way (the agent was killed): continue from
  the last finished node. Nodes that already ran are not run again.
- **duplicate**: the workflow ended and every step it went through was real: skip
  it. Kafka delivers at least once.
- **rerun**: the workflow ended on a stub (`stubbed` is not empty). Nothing was
  really done, so the saved state is dropped and the failure runs again.

The Kafka offset is committed after the whole stack was handled. While a stack is
being worked the agent pauses the partition and polls Kafka after every workflow,
so a long stack does not get it dropped from the consumer group.

### Failures seen while steps are still stubs

A failure that passes through today ends `ESCALATED` on the stubs, and nobody is
told. It is not lost for good: it stays `PENDING` in `silver2_quarantine`, and
because of **rerun** it is handled properly the next time it is published. After a
step is built, publish the open failures again by resetting the scanner:

```bash
docker compose rm -sf lookup-scanner
docker volume rm self-healing-pipeline_lookup-state
docker compose up -d lookup-scanner
```

## Run it

```bash
docker compose up -d --build recovery-agent
docker compose logs -f recovery-agent
```

```
[agent] lookup 66b035e5-...: 2 failure(s)
[agent] fd95d1c4c9991007 new       P3 crash1:crash_month value='Jule' -> ESCALATED (deterministic_lookup > diagnose > policy_gate > remediate > escalate > learn) [stubs: remediate, escalate, learn]
[agent] 289b9d3430ea286e new       P3 crash1:weather_condition value='MISTY DARK' -> ESCALATED (deterministic_lookup > diagnose > policy_gate > escalate > learn) [stubs: escalate, learn]
```

The first failure passed the gate (it reached `remediate`, still a stub); the
second was stopped by it. Diagnose needs Ollama with the model pulled
(`ollama pull qwen3:8b`); without it both lines end at `policy_gate > escalate`.

The first word is the `failure_id`. Print everything saved for that workflow:

```bash
docker compose exec recovery-agent python -m agent.show fd95d1c4c9991007
```

Settings (`config.py`): `LOOKUP_OUTPUT_TOPIC`, `AGENT_CONSUMER_GROUP`,
`AGENT_CHECKPOINT_PATH`, `AGENT_MAX_POLL_INTERVAL_MS`, `AGENT_NODE_MAX_ATTEMPTS`,
`AGENT_NODE_RETRY_SECONDS`, and for Diagnose and the gate:

| Setting | Default | Meaning |
|---|---|---|
| `DIAGNOSE_MODEL` | `qwen3:8b` | reasoning model, any model Ollama serves (`ollama pull` it first) |
| `DIAGNOSE_BASE_URL` | `http://host.docker.internal:11434` | Ollama |
| `DIAGNOSE_TIMEOUT_SECONDS` | `120` | one model call |
| `DIAGNOSE_SAMPLE_RECORDS` | `3` | quarantined records shown to the model, 0 for none |
| `DIAGNOSE_THINK` | `false` | `false` = answer directly, `true` = think first (raise `DIAGNOSE_NUM_PREDICT`), `default` = leave it to the model |
| `POLICY_CONFIDENCE_THRESHOLD` | `0.85` | minimum confidence to run without an engineer, 0.5 to 1 |
| `POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD` | `0.95` | minimum confidence when no record can confirm the value; 0.5 to 1 and never below the threshold above, or `NEVER` |
| `POLICY_SAFE_ACTIONS` | empty | empty = the safe set of `actions.py`; `NONE` anywhere in the list = review everything |

A threshold outside its range (0, 85, a typo) is refused: the agent stops at start
with the reason in its log instead of running with a gate that passes everything.

Compose passes `DIAGNOSE_MODEL`, `DIAGNOSE_BASE_URL`, `DIAGNOSE_THINK` and the three
`POLICY_` settings from `.env` (see `.env.example`). The model's context and reply
size are `DIAGNOSE_NUM_CTX` (8192) and `DIAGNOSE_NUM_PREDICT` (1024).

To start from empty state, remove the volume:
`docker compose rm -sf recovery-agent && docker volume rm self-healing-pipeline_agent-state`.

## Tests

No Kafka, Neo4j or model needed: the model is a fake, and the Delta reader runs on
small tables written to a temp folder. From the repo root:

```bash
pip install -r agent/requirements.txt pytest
python -m pytest agent
```
