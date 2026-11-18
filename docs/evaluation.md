# Evaluation

How well does the self-healing loop work? This page says what is measured, how, and
what the measurements were. The harness is `python -m evaluation` (folder
`evaluation/`); the numbers below can be reproduced with the commands in
[Reproduce](#reproduce).

## What is measured

From the team plan (Action Plan 5.3) and the spec's claims (detect bad data in every
batch, reuse known fixes without the model, act only when confident and safe, escalate
the rest with context, learn so the next occurrence is caught earlier, cut MTTD and MTTR):

| Metric | Definition | Spec |
|---|---|---|
| Handled right on entry | share of injected records whose first stop is the intended one (quarantine, Silver 1 dead letter, fuzzy fix), or that were mapped correctly at Silver 2 because an earlier run taught it the value | IQ3-IQ7, IC4 |
| Silent errors | records that reached Silver 2 with a wrong value without quarantine or a recorded fix | IQ5, RS1 |
| MTTD | seconds from the record's publish to Kafka to its quarantine row | problem statement 3.2 |
| Auto-resolution rate | records healed by the agent with no engineer (`fix_source` `INCIDENT` or `DIAGNOSIS`), over quarantined records that have a right answer | DL2, AP3, RD/VD |
| Resolution rate | the same, engineer fixes included | EL1 |
| Resolved without the model | records healed by a reused past fix, or mapped at Silver 2 because of an earlier lesson, over all records resolved | DL2, "reserving LLM inference for unseen incidents" |
| Wrong-fix rate | healed records whose final value is not the right one, over all healed records; a value with no right answer that the agent mapped anyway counts as wrong (see below) | VD3 |
| Unsafe autonomous fixes | wrong fixes no engineer approved, plus autonomous "fixes" of values that have no right answer (each record counted once); the Recovery section of each round breaks it down | RS1, AP2 |

Records with no right answer (`expected_action` other than `ADD_VARIANT`: an unknown
value, a missing mandatory value, a broken record) have no true value to compare with,
so the injector leaves `correct` empty for them. The metrics count them explicitly:

- **Unsafe autonomous fixes** includes every such record the agent healed on its own
  (`fix_source` `INCIDENT` or `DIAGNOSIS`), whatever value it chose: the right outcome
  was an engineer's decision, and the agent took it instead.
- **Wrong-fix rate** counts them as wrong fixes too, so that a model that confidently maps
  "MISTY DARK" to FOG cannot show 0% wrong fixes. Two exceptions: an agent fix that lands
  on the record's true value (when the injector knows it) is not wrong, only unsafe; and an
  engineer's fix of such a value is not counted as wrong, since only an engineer could
  decide it and the ground truth has nothing to judge that decision by. (The oracle
  engineer dismisses these values, so this only matters with a human engineer.)
| Time to recover (MTTR) | seconds from publish to release into Silver 2, per fix source | problem statement 3.2 |
| Escalation rate and reasons | recovery workflows handed to an engineer, and why | EL1 |
| Model asked | workflows that called the reasoning model (answered or not) | AP1 |
| Policy gate against the truth | model proposals the gate let through or stopped, each checked against the right answer | AP2, RS1 |
| Promotions | fixes added to `ref_canonical_mappings` | EL2 |

Every number comes from two things the system records anyway, so the measurement does
not change what it measures:

- **Per record**, `python -m tools.fault_injector status <run> --json` (Task 108): the
  ground truth written before injection (true value, right action, intended path) and
  where the record is now (outcome, value in Silver 2, which execution fixed it).
- **Per recovery workflow**, `python -m agent.workflows --json` (new): path through the
  graph, whether the model was asked, the gate's verdict, escalation reasons and the
  engineer's decision, what Learn stored and promoted, timestamps. Read only.

`evaluation/metrics.py` turns them into the metrics; it is pure Python and unit-tested
(`evaluation/tests`).

## How an experiment runs

1. **A separate stack.** Faults go only into the evaluation stack
   (`docker-compose.eval.yml`: own Kafka, `./eval` Delta folder, Neo4j), loaded with a
   clean sample of 2000 rows each of Crash1 and Crash3. Nothing it learns reaches the
   real pipeline.
2. **A fixed scenario.** `data/faults/evaluation.json`: 50 records, 11 faults, one fixed
   value each so each becomes one recovery case, spread over 3 micro-batches where the
   promotion rule (5 rows in 3 batches) should be reachable:

   | Fault | Records | Right outcome |
   |---|---|---|
   | `crash_month` "Jule" in June records (Crash1) | 12 | quarantine, then JUNE |
   | `crash_day_of_week` "S" in Sunday records (Crash3, day-first dates) | 6 | quarantine, then SUNDAY |
   | `weather_condition` "RAINING" (Crash1) | 6 | quarantine, then RAIN |
   | `crash_month` "Abr." in April records (Crash3) | 6 | quarantine, then APRIL |
   | `lighting_condition` "LIGHT" in daylight records (Crash1): a fuzzy trap | 4 | quarantine, then DAYLIGHT |
   | typos in weather (Crash1) and month (Crash3) | 6 | fixed by Silver 2's fuzzy match |
   | `weather_condition` "MISTY DARK" | 4 | quarantine, no right value: escalate |
   | `crash_date` in words ("11th of April 2021") | 2 | quarantine, escalate (no parser for it) |
   | `crash_date` empty | 2 | quarantine, escalate |
   | `crash_date` column renamed | 2 | Silver 1 dead letter |

   The other fixable values were chosen out of fuzzy matching's reach, so on their own
   they cannot show the one silent error fuzzy matching can make: mapping a value to the
   wrong canonical value on entry. "LIGHT" is there for that. It is one edit from NIGHT,
   an accepted spelling of DARKNESS, and far from every daylight spelling, so a fuzzy
   match that trusts short accepted spellings turns daylight crashes into DARKNESS with
   no quarantine. If that happens, the report counts the records as silent errors.

3. **An oracle engineer.** An escalated case waits for a person. The harness answers each
   case of the run the way a careful engineer would, from the ground truth
   (`evaluation/oracle.py`): approve the proposed fix when it is right, otherwise `fix`
   with the true value; `dismiss` values that have no right answer; leave schema changes
   and anything that is not part of the run open. It answers at once, so engineer-path
   times are a lower bound (they leave out a person's reading time).
4. **Two rounds.** The same scenario is injected twice into the same stack. Round 1 meets
   every value for the first time; round 2 measures what round 1 taught.
5. **Wait until nothing moves**: every record past Silver 2, no workflow running, no case
   the oracle can answer, every quarantined value picked up by a workflow, and the same
   state for 60 s. Then `status` and the workflow export are saved and the report
   written to `eval/results/<label>/`.

## Results

Measured 2026-11-16 in a cloud container (Docker, 4 CPUs) on Golf `3738312` plus this
branch. **No reasoning model was reachable there** (no Ollama), so every diagnosis is
`UNAVAILABLE` and the policy gate never saw a proposal: these runs measure the
deterministic parts, the safety behaviour without a model, the engineer path and
learning. The model's share (gate pass rate, precision, autonomous diagnosis) needs a
run with Qwen: see [Still to measure](#still-to-measure).

### Run `cloud-1`: engineer fixes promoted (the default)

(This run used "Apr." for the April records. Task 109 widens fuzzy matching to the
accepted spellings, which corrects "Apr." to APRIL on entry, so the scenario now uses
"Abr.", which neither version corrects.)

| Metric | Round 1 | Round 2 |
|---|---|---|
| Injected records | 46 | 46 |
| Handled right on entry | 100% | 100% |
| Mapped at Silver 2 thanks to round 1 (no quarantine) | 0 | 30 |
| Silent errors | 0 | 0 |
| MTTD median / p90 (s) | 30 / 34 | 34 / 41 |
| Quarantined records | 38 | 8 |
| Auto-resolution rate | 0% | (nothing left to fix) |
| Resolution rate incl. engineer | 100% (30 of 30) | (nothing left to fix) |
| Resolved without the model | 0% | 100% (30 of 30) |
| Wrong-fix rate / unsafe fixes | 0% / 0 | - / 0 |
| Time to recover median / p90 (s) | 192 / 244 | - |
| Recovery workflows | 17 | 5 |
| Workflows that asked the model | 16 | 4 |
| Escalation rate | 100% | 100% |
| Promotions into `ref_canonical_mappings` | 4 | 0 |

What it shows:

- **Nothing wrong got through.** Every fault stopped where it should: 38 records
  quarantined, 6 typos fixed by fuzzy matching, 2 renamed-column records parked in the
  Silver 1 dead letter. No wrong value reached Silver 2 in either round.
- **Without a model, nothing is fixed blindly.** All 16 workflows that needed a diagnosis
  went to the engineer as `NO_DIAGNOSIS`, and the empty date as `ENGINEER_REQUESTED`
  (Diagnose skips the model when a mandatory field is empty).
- **The engineer path heals correctly.** The oracle's 12 fixes healed all 30 fixable
  records, all with the right value, about 3 minutes from publish to release (most of it
  Validate replaying the group in Spark); 5 cases with no right answer were dismissed and
  their 8 records stay quarantined, as they should. (One of the 5, the empty date, was
  dismissed by hand with the oracle's rule: the harness first grouped it with the
  renamed-column records, a matching bug fixed and unit-tested before round 2.)
- **It learned.** The 4 engineer-approved variants were promoted at once (EL2). In round
  2 the 30 records with those values went straight to Silver 2 with the right value: no
  quarantine, no workflow, no model, no engineer. Only the 8 records with no right answer
  came back (and were escalated again, which is correct: there is still no answer).
- **One case per micro-batch.** 4 fixable values in 3 batches made 12 workflows, so the
  engineer was asked 12 times for 4 decisions. This is the known behaviour Task 109 fixes
  (a later batch attaches to the open workflow, PR #37); expect 4 cases after it merges.
  Confirmed by run `combined-1` below.

### Run `combined-1`: with Task 109 (later batches attach)

Measured 2026-11-16 in a cloud container (Docker, 4 CPUs, still no reasoning model) on
Golf `3738312` with PRs #35, #36, #37 and #38 merged together, same scenario and
command (`python -m evaluation run --label combined-1`):

| Metric | Round 1 | Round 2 |
|---|---|---|
| Handled right on entry / silent errors | 100% / 0 | 100% / 0 |
| Mapped at Silver 2 thanks to round 1 | 0 | 30 |
| MTTD median / p90 (s) | 34 / 38 | 32 / 38 |
| Quarantined records | 38 | 8 |
| Resolution rate incl. engineer | 100% (30 of 30) | (nothing left to fix) |
| Wrong-fix rate / unsafe fixes | 0% / 0 | - / 0 |
| Time to recover median / p90 (s) | 136 / 174 | - |
| Recovery workflows (later batches attached) | 8 (9) | 4 (1) |
| Workflows that asked the model | 7 | 3 |
| Engineer decisions | 8 (4 fixes, 4 dismissals) | 4 dismissals |
| Promotions into `ref_canonical_mappings` | 4 | 0 |

The 4 fixable values now make 4 cases instead of 12: the 9 later batches joined the
workflow already open for their value, so the engineer decided each value once and
the fix healed all its batches together. The median time to recover also fell from
192 s to 136 s, probably because fewer workflows queue for the one agent. Everything
else matches `cloud-1`.

### Still to measure

- **Reuse of a past fix without promotion.** `--no-promote` keeps engineer fixes out of
  `ref_canonical_mappings`, so round 2 quarantines again and the deterministic lookup
  should reuse the stored incident (`fix_source` `INCIDENT`) with no model and no
  engineer. Not run yet (the cloud container lost Docker before it could).
- **With the reasoning model.** Run the same commands on a machine with Ollama and
  `qwen3:8b` (`agent.check_model` first). That adds the autonomous diagnosis path: the
  gate's pass rate, how often a passed proposal is right (`policy_gate.passed_right`),
  how often a right proposal is stopped (thresholds 0.85 and 0.95 are still
  uncalibrated), and MTTR without an engineer.
- **Schema changes.** The renamed column was detected (dead letter) but not recovered:
  that path needs Qwen's classification (`QWEN_ENABLED=true`, `QWEN_KAFKA_ENABLED=true`)
  and an engineer's `tools.replay` with `--rename` (docs/demo.md).
- **The fuzzy trap.** `cloud-1` and `combined-1` ran before "LIGHT" joined the
  scenario (46 records, 10 faults), so their "silent errors: 0" only covers values
  outside fuzzy matching's reach. With Task 109's accepted spellings as of PR #37,
  "LIGHT" scores 1 edit to DARKNESS (via NIGHT) and 3 to the runner-up, so the next run
  should show 4 silent errors until the matcher stops trusting that short spelling.
- **Scale.** 2000 rows per source and 46 to 50 faulty records. The main stack's full data has not been
  measured.

## Reproduce

```bash
# a fresh evaluation stack
docker compose -f docker-compose.yml -f docker-compose.eval.yml down -v
rm -rf eval                                      # Windows: Remove-Item -Recurse -Force eval
docker compose -f docker-compose.yml -f docker-compose.eval.yml up -d --build
# wait until the producer has exited (ps producer), then, from the repo root:
python -m evaluation run --label my-run                   # 2 rounds, oracle engineer
python -m evaluation run --label my-run-np --no-promote   # engineer fixes heal only
python -m evaluation run --label my-run-auto --engineer none   # nobody answers
python -m evaluation report eval/results/my-run           # rebuild report.md from the saved JSON
```

Python 3.8+ on the host (standard library only) and Docker Compose 2.24.4+. Each round
takes 5 to 15 minutes, most of it Spark start-up in the `tools` container for `status`.
Results: `eval/results/<label>/report.md`, `metrics.json` and the raw `round-N.json`.
Learning carries over between runs on the same stack, so reset it between experiments
that must be independent.

## Contracts this relies on

The evaluation reads, and would need updating if these change:

- `tools.fault_injector status --json` fields: `outcome` values (`QUARANTINED` covers
  `PENDING` and `IN_PROGRESS`), `quarantine_failures`, `fix_source`, `failure_id`,
  `seconds_to_heal`.
- Workflow `status` values `RESOLVED`, `RESOLVED_BY_ENGINEER`, `DISMISSED`, `FAILED`,
  `ESCALATED`, and `ATTACHED` (Task 109: a later batch joined an open workflow; counted as
  part of that case, not as another one).
- A failure's (`folder`, `column_name`, `raw_value`) is how the scanner groups quarantine
  rows; an empty mandatory value is `MISSING_MANDATORY_VALUE` in the quarantine and no
  value in the failure.
- `silver2_fuzzy_audit` is only used to tell `FUZZY_FIXED` from `CLEAN` (one row or two per
  correction gives the same answer), so the duplicate fuzzy-audit rows that PR #35 removes
  do not change any metric here.
