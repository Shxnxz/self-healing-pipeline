# Gold: business fields and metrics

Gold is the layer analysts and data scientists read (spec, "Gold Layer"). It has two
parts, both written by the `silver2-gold-job` container:

| Table | Written by | Grain | For |
|---|---|---|---|
| `gold` | `spark-jobs/gold_job.py` (stream, every 10 s) | one row per crash, UNION ALL of every `silver2_<source>` | row-level analysis, joins |
| `gold_daily_summary` | `spark-jobs/gold_metrics.py` | one row per `crash_date` | trend charts: crashes, injury and fatal crashes, injuries, hit and run |
| `gold_conditions_summary` | same | weather x lighting x road surface | which conditions are dangerous: counts, injury and fatal rates |
| `gold_time_pattern` | same | day of week x hour | heat map of when crashes happen and how severe |
| `gold_cause_summary` | same | primary contributory cause | causes ranked by count and share, injury rate |
| `gold_beat_monthly` | same | police beat x month | feature table for forecasting crashes per beat (counts, night / weekend / adverse weather / hit-and-run shares, mean speed limit) |

## Fields added to `gold`

| Column | Meaning |
|---|---|
| `crash_year` | year of `crash_date` |
| `is_weekend` | `crash_day_of_week` is SATURDAY or SUNDAY |
| `time_of_day` | MORNING 06-11, AFTERNOON 12-16, EVENING 17-20, NIGHT 21-05 (from `crash_hour`) |
| `is_injury_crash` | `injuries_total > 0` |
| `is_fatal_crash` | `injuries_fatal > 0` |
| `is_injected` | a fault-injected copy (`crash_record_id` `FAULT-...`, evaluation stack only) |

An existing `gold` table gets these columns when the job starts (`ALTER TABLE ... ADD
COLUMNS`), so no reset is needed: rows written before read `null` there, and the
metrics derive the values again for them.

## What the metric tables count

- **Real crashes only.** Fault-injected copies (`is_injected`, or a `FAULT-` key) are
  left out, so the evaluation stack's Gold numbers are not inflated by the experiment.
- **Each crash once.** Rows are de-duplicated on `crash_record_id`, the business key.
- **Healed records where they belong.** Every refresh recomputes from the whole `gold`
  table, so a record the recovery agent releases hours later is counted on its own
  `crash_date`, not on the day it was healed.
- **Freshness.** Every row carries `gold_version` (the Delta version of `gold` it was
  computed from) and `refreshed_at`.

## How they are kept current

`gold_job.start` also starts a streaming query named `gold_metrics` on `gold`. Every
`GOLD_METRICS_INTERVAL` (default `60 seconds`), if `gold` received rows, it recomputes
all five tables and overwrites each in one Delta commit, so a dashboard reads either the
previous version or the new one. About 1M rows take a few seconds per refresh.
`GOLD_METRICS_ENABLED=false` turns it off.

A one-off refresh, for example after a reset:

```bash
docker compose run --rm tools python spark-jobs/gold_metrics.py
docker compose run --rm tools python -m evaluation.peek gold_daily_summary --order crash_date
```

## Lineage

The metric tables are written by Spark, so OpenLineage records `gold -> gold_*` edges in
Neo4j. The Deterministic Lookup's blast radius (lookup/blast_radius.py) counts every
`gold_*` table as a Gold consumer (weight 5), so a failure that reaches Gold now has a
larger reach than before these tables existed, and its priority score is higher. That is
the intended meaning (a bad value now spoils five reports, not one table), but it can move
a failure from P2 to P1, where a borrowed past fix needs an engineer's approval
(`APPROVE_BORROWED_FIX_TIERS`).

## Tests

`spark-jobs/test_gold_metrics.py` (real Spark, about 2 minutes): an old `gold` table
gets the new columns, the derived values are right, the metric tables count each crash
once without injected copies, and a late record lands in its own day on the next refresh.
