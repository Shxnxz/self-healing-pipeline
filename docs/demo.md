# Demo: "Jule", end to end

The spec's worked example, live: a misspelled month that fuzzy matching must not
guess, quarantine, the recovery agent, an engineer's decision, healing, learning, and
the next "Jule" handled at Silver 2 with no quarantine and no model. About 10 minutes
once the evaluation stack is warm.

It runs on the **evaluation stack** (`docker-compose.eval.yml`): its own Kafka, Delta
folder (`./eval`) and Neo4j, so nothing the demo teaches the agent reaches the real
pipeline.

## Before the audience arrives

```bash
# 1. a clean evaluation stack (forget earlier demos)
docker compose -f docker-compose.yml -f docker-compose.eval.yml down -v
rm -rf eval                                   # Windows: Remove-Item -Recurse -Force eval
docker compose -f docker-compose.yml -f docker-compose.eval.yml up -d --build

# 2. wait until the producer has exited (2000 clean rows of Crash1 and Crash3)
docker compose -f docker-compose.yml -f docker-compose.eval.yml ps producer

# 3. optional: the reasoning model, so step 2 shows a real diagnosis
docker compose -f docker-compose.yml -f docker-compose.eval.yml exec recovery-agent python -m agent.check_model
```

Without Ollama the demo still works: the diagnosis says `UNAVAILABLE` and the case goes
to the engineer, which is the safe behaviour the spec asks for.

Open in a browser: Neo4j at http://localhost:17474 (connect to `bolt://localhost:17687`,
user `neo4j`, password `selfhealing`) to show lineage and the incident Learn writes.

## Run it

```bash
python -m evaluation demo            # pauses before each step: press Enter
python -m evaluation demo --auto     # no pauses (recording)
```

Python 3.8+ on the host, from the repo root; it only calls `docker compose`. Every step
prints the command it runs, so it can also be done by hand (below).

## The story, step by step

| Step | What the audience sees | Command (prefix `docker compose -f docker-compose.yml -f docker-compose.eval.yml`) |
|---|---|---|
| 1. A typo nobody can guess | 12 June records arrive with `crash_month = "Jule"` in 3 micro-batches. Status shows them `QUARANTINED`: one edit from JUNE and from JULY, so fuzzy matching refuses and Silver 2 keeps them out. | `run --rm tools python -m tools.fault_injector inject jule` then `... status <run_id> --records` |
| 2. The agent picks it up | Lookup: no past incident. Diagnose asks the model; the policy gate checks confidence, the safe-action list and the records' `crash_date`. Without a confident, checked answer, the engineer gets the whole case (failure, blast radius, lineage, past incidents, diagnosis, samples). | `exec recovery-agent python -m agent.review list` and `... show <failure_id>` |
| 3. The engineer decides | The records' dates are in June, so: JUNE. Remediate stages the fix and audits it, Validate replays the records through Silver 2's own code and releases them, Learn stores the incident in Neo4j and promotes `JULE -> JUNE`. Status shows `HEALED`, correct. | `exec recovery-agent python -m agent.review fix <failure_id> JUNE --by <name>` |
| 4. It learned | "Jule" arrives again in 12 new records: status `CLEAN`. Silver 2 maps it from `ref_canonical_mappings`: no quarantine, no model, no engineer. | `run --rm tools python -m tools.fault_injector inject jule` then `status` |
| 5. Gold | The healed records are in `gold` (`is_injected = true`), and the business tables leave them out. | `run --rm tools python -m evaluation.peek gold --where "crash_record_id LIKE 'FAULT-%'"`, `run --rm tools python spark-jobs/gold_metrics.py` |

If the model is running and confident, step 2 can end without an engineer: the gate
lets `ADD_VARIANT JUNE` through because the records' dates confirm it, and step 3
happens on its own. That is the autonomous path; both are worth showing.

## Talking points

- **Refuses to guess.** "Jule" is never written to Silver 2 as a wrong month (spec:
  "We refuse to guess").
- **Context, not an alarm.** The engineer's case already holds the blast radius,
  lineage, similar incidents and the model's hypothesis (`agent.review show`).
- **Safe by construction.** A fix is staged, replayed and checked (rows conserved, no new
  failures, month agrees with `crash_date`) before any record is released.
- **Solved once.** The second "Jule" never reaches quarantine or the model.
- **Measured.** The same loop on 41 faults of 10 kinds, with the numbers: docs/evaluation.md.

## Afterwards

```bash
docker compose -f docker-compose.yml -f docker-compose.eval.yml down -v   # and delete ./eval
```
