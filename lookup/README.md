# Deterministic Lookup (DL1, DL2)

Decides, without an LLM, **which failure to fix first** and **whether a past fix
can be reused**. Its output is a ranked stack that the remediate / diagnosis
nodes pop from the top.

```
Spark jobs --Qwen--> recovery.incidents --+
                                          +--> deterministic-lookup --> recovery.lookup
silver2_quarantine --lookup-scanner--> recovery.failures --+       |
                                                                   +-- Neo4j: lineage graph + incident memory
```

| Service | Reads | Writes |
|---|---|---|
| `lookup-scanner` (`scanner.py`) | new `PENDING` rows in `delta/silver2_quarantine` | one message per (folder, column, value) group to `recovery.failures` |
| `deterministic-lookup` (`consumer.py`) | `recovery.incidents` + `recovery.failures` | one ranked stack per window to `recovery.lookup` |

Qwen incidents reach Kafka only with `QWEN_ENABLED=true` **and**
`QWEN_KAFKA_ENABLED=true` (see `.env.example`). Without them the lookup still
works on quarantine failures.

## 1. Structured failure (`failures.py`)

Both inputs become one `StructuredFailure`. Two table roles are kept apart:

- **origin layer**: the last table that still holds the records intact. Blast radius
  is everything downstream of it, because that is where the records are missing.
  Quarantine → `silver1`; Silver 1 schema guard → `bronze`; Silver 2 crash → `silver1`.
- **detected layer**: the table whose job raised the failure. Used in the incident
  `signature_key`, so it must match what the Learn node writes.

Folder branches are resolved against the lineage graph: `silver2_crash1` when that
`(:Table)` exists, otherwise `silver2` (the single-table layout on main today).
Qwen output with `classification_status != OK` is marked `trusted: false` and is
never auto-remediated.

## 2. Compute blast radius (`blast_radius.py`)

It doesn't just list the affected layers. It scores impact 0–100 from five
factors. Every factor appears in `breakdown`, so the ranking can be explained:

| Factor | Weight | Meaning |
|---|---|---|
| severity | 0.30 | share of a batch the failure type stops: schema 1.0, corrupt 0.9, value 0.4 (+0.3 when the column is mandatory) |
| reach | 0.25 | downstream tables weighted by layer (bronze 1, silver1 2, silver2 3, gold 5; quarantine/audit/dead-letter 0), saturating `r/(r+5)` |
| volume | 0.20 | rows affected, `log10(1+rows)/log10(1+10000)` |
| propagation | 0.15 | is the failing column still in downstream schemas (`SchemaVersion.fields`)? A column dropped before gold matters less |
| blocks | 0.10 | other failures in the same window that sit downstream of this one; fixing it first may clear them |

Tiers: P1 ≥ 70, P2 ≥ 40, else P3. If the graph doesn't know a table yet
(fresh stack, lineage off), it falls back to the chain
bronze → silver1 → silver2 → gold, and `lineage_known` is set to `false`.

## 3. Check incident pattern (`patterns.py`)

1. **Exact**: `incidents.find_exact` on `signature_key`, then score 1.0.
2. **Structured similarity** against verified incidents with the same column or
   the same failure family:

   ```
   score = 0.25*failure_type + 0.25*column + 0.15*table + 0.35*value
   failure_type: 1 same, 0.5 same family    table: 1 same, 0.7 same layer other folder
   value: value failures  -> 1 - DamerauLevenshtein / max length (normalised)
          schema failures -> Jaccard of expected_only / source_only column sets
   ```
3. **Embeddings (optional)**: `LOOKUP_EMBED_MODEL=nomic-embed-text` adds vector
   search *recall*. Candidates found this way are scored with the same formula,
   so the threshold keeps one meaning.

**Route to remediate** only if all of these hold: the failure is trusted, the
score is ≥ `LOOKUP_SIMILARITY_THRESHOLD` (0.85), and the fix **transfers**:

- value failures: same column and the same value after normalisation (case,
  spaces, punctuation). `Juen` scores 0.91 against `JUNE`, but its canonical
  value is not guaranteed, so it goes to diagnosis with the match as a hint.
- schema failures: the column sets are identical, compared case-sensitively,
  and the folder can be different. A rename is a rename.
- a fix that already failed for the same signature (`previous_attempts`) is
  never repeated.

Everything else goes to **diagnose**.

## Output (`recovery.lookup`)

```jsonc
{
  "lookup_id": "...", "created_at": "...",
  "summary": {"failures": 7, "routes": {"remediate": 2, "diagnose": 5}, "tiers": {"P1": 2, "P2": 5}},
  "stack": [                         // highest priority first
    {
      "rank": 1, "route": "remediate", "route_reason": "structured match 0.95 -> reuse RENAME_COLUMN",
      "priority": {"score": 71.7, "tier": "P1"},
      "failure":          { "failure_id", "source", "failure_type", "folder", "column_name",
                            "signature_value", "rows_affected", "trusted", "evidence", ... },
      "lineage_context":  { "origin_table", "detected_table", "downstream", "upstream", "recent_runs",
                            "blast_radius": {"score", "tier", "breakdown", "affected_layers",
                                             "column_reaches", "blocks_failures", ...} },
      "incident_context": { "signature_key", "reusable", "reason", "match",
                            "candidates": [top 3 with score + components], "previous_attempts" }
    }
  ]
}
```

## Consuming the output

The next step of the healing loop (e.g. a LangGraph agent) reads `recovery.lookup`
and works through `stack` from the top: items with `route: remediate` carry the
past fix in `incident_context.match` (`action`, `action_params`); items with
`route: diagnose` carry the lineage and nearest incidents for the reasoning LLM.
Consumers should deduplicate on `failure.failure_id` (delivery is at-least-once).

## Tuning

All weights and thresholds are environment variables (`config.py`), e.g.
`LOOKUP_SIMILARITY_THRESHOLD`, `BLAST_SCORE_WEIGHTS='{"severity":0.3,...}'`,
`BLAST_LAYER_WEIGHTS`, `LOOKUP_WINDOW_SECONDS`.

To calibrate the threshold once the incident corpus has examples: label pairs
(new failure, past incident) as "same fix" or "different fix", compute
`patterns.similarity` for each, and choose the lowest threshold that keeps
precision near 1.0. A wrong auto-fix costs more than an extra diagnosis.

## Tests

```bash
pip install pytest neo4j
python -m pytest lookup/tests
```
