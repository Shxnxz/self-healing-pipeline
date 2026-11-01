# Qwen exception classifier (IC2)

Python standard library only; Qwen runs locally using Ollama.
This module classifies unstructured exceptions. It does not implement lookup,
Neo4j writes, remediation, or classify already-structured quarantine failures.

## Quick demo (from project root)

```sh
ollama pull qwen3:4b-instruct
python3 -m qwen_classifier --error-file qwen_classifier/example_error.txt --job-name Silver2 --batch-id 17
python3 -m unittest discover -s tests -p 'test_qwen.py' -v
```

## Python interface

```python
from qwen_classifier import QwenClassifier
failure = QwenClassifier().classify(error_text, job_name='Silver2', batch_id=17)
```

Output: schema_version, source, failure_type, table, column, error_class,
job_name, batch_id, needs_review, classification_status, model, input_truncated.
Types: MISSING_COLUMN, TYPE_MISMATCH, PARSE_ERROR, CORRUPT_RECORD,
SCHEMA_MISMATCH, UNKNOWN. Missing identifiers are null.
OK means output passed structural/evidence checks, not proven semantic correctness.
UNAVAILABLE and INVALID_OUTPUT require review. No output authorizes a repair.
Long input is bounded to the first/last 8,000 characters and requires review.
An identifier appearing in input is necessary but not sufficient evidence that it
is the correct affected field; measure extraction accuracy separately.

## Pipeline integration

Silver1 validates source payload column names before its fixed-schema JSON parse.
Rows with missing, renamed, case-changed, or extra source keys are preserved with
raw Kafka metadata in silver1_dead_letter. Qwen classifies one exception per schema
signature after the dead-letter write succeeds. Compatible rows continue through
the existing cleaning SQL; Delta transaction IDs prevent duplicate replay writes.
A null value in a present field is not treated as a missing column.

Silver2 calls Qwen after its existing dead-letter write succeeds. The raw batch
remains in the existing Delta dead-letter table. The classification and traceback
are stored in incidents/ with the dead-letter path and batch ID for handoff.
The combined run_layers entry point also classifies uncaught job exceptions and
re-raises the original error. Direct layer scripts have no general entry wrapper.
Classification failure does not change the existing batch/retry behavior.
Incident files are a handoff artifact; no recovery consumer or lookup is added.

Optional integration is disabled by default. To enable:

```sh
QWEN_ENABLED=true docker compose up --build
```

Compose mounts the package into both Spark drivers. Host Ollama must be reachable
at http://host.docker.internal:11434 (Docker Desktop); configure QWEN_BASE_URL for
your deployment. Do not expose Ollama publicly. Host CLI defaults to localhost.
Other settings: QWEN_MODEL, QWEN_INCIDENT_DIR. See .env.example for Docker values.

The Spark Dockerfile is unchanged: the package is provided by Compose's bind mount.
Smoke tests of local inference are not a full running Spark/Docker integration test.

### Incident JSON and Kafka handoff

`classifier.py` extracts and validates model output. `handoff.py` builds the
versioned message, saves JSON atomically, and optionally publishes it to Kafka.
`integration.py` coordinates these steps. No lookup or remediation is performed.

Set `QWEN_ENABLED=true` and `QWEN_KAFKA_ENABLED=true` to publish to
`QWEN_INCIDENT_TOPIC` (default `recovery.incidents`). Rebuild the Spark image to
install `confluent-kafka`. Inside Docker use `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`;
from the host use `localhost:29092`. Both Spark services receive these settings.
Kafka forwarding is disabled by default to allow teams to agree on the contract.

The message includes `incident_id`, `failure_type`, source `table`, `source_topic`,
`affected_dataset` (the graph dataset, separate from the source table),
`affected_columns` (`expected_only`, `source_only`, `reported_column`),
`affected_record_count`, `batch_id`, `dead_letter_ref`, `classification_status`,
`needs_review`, and `handoff_status`. Missing context is null, not invented.
`READY` means classification is available for lookup, not permission to repair.
`NEEDS_REVIEW` includes unavailable/invalid/unknown model output.

Messages are grouped per topic, source table, batch and issue signature, rather
than one model call per record. `dead_letter_ref` gives the path and filters
needed to select the original rows. The consumer must have access to the same
Delta storage; a Docker path alone is not a portable storage URI.

Delivery is **at least once**. A batch fails if persistence or broker delivery
fails, so its checkpoint does not advance. The local JSON remains after a broker
failure. Batch retry can resend the same incident: consumers must deduplicate
using `incident_id` (also the Kafka key). This is not exactly-once recovery.
Standalone job wrappers preserve the original exception if incident recording
also fails. When Qwen is disabled, invalid rows are still parked but no incident
is generated or published.

### New input conditions

The guard accepts both payload envelopes and flat JSON objects, detects missing
or extra columns, and separates malformed JSON/non-object payloads as
`CORRUPT_RECORD`. Raw records remain unchanged in Delta. Set
`SILVER1_OPTIONAL_COLUMNS='["location"]'` to permit a known nullable column to
be absent. Names outside the existing canonical schema are rejected as config
errors. Extra columns remain a mismatch rather than silently disappearing.

The downstream cleaning SQL still targets the Crash schema. A new business
schema requires its own approved schema and transformation; this change does
not automatically make arbitrary datasets compatible.

Keep `SILVER1_TXN_APP_ID` stable when restarting an existing checkpoint. Change
it when creating a new checkpoint against existing Delta output. Prefer separate
output and checkpoint paths for isolated tests.

Run unit tests: `python3 -m unittest discover -s tests -v`.

Opt-in integration test (running Ollama and Kafka required, creates only temporary
Delta outputs and `test.recovery.incidents`):

```sh
docker compose run --build --rm --no-deps \
  -v "$PWD/tests/integration_qwen_conditions.py:/tmp/integration_qwen_conditions.py:ro" \
  bronze-silver1-job python /tmp/integration_qwen_conditions.py
```

This exercises valid, missing, renamed, extra and malformed inputs, optional
columns, actual Ollama classification, acknowledged Kafka delivery and stable
incident IDs across batch replay. The test topic may contain repeated test runs;
records are filtered using each run's unique dead-letter path.
