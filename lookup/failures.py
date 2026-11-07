"""StructuredFailure: the one shape every Deterministic Lookup input is turned into.

Two producers feed the lookup, and they describe failures differently:

  * Qwen incidents (Kafka `recovery.incidents`, built by qwen_classifier/handoff.py):
    one message per exception signature, e.g. a Silver 1 schema mismatch or a
    Silver 2 transform crash. The failure type and column come from an LLM, so
    the message says whether it can be trusted (`classification_status`).
  * Quarantine groups (Kafka `recovery.failures`, built by scanner.py): rows in
    `silver2_quarantine` grouped by (folder, column, value). These are found by
    deterministic rules in silver2_job.py, so they are always trusted.

Two table roles are kept apart on purpose:

  * origin layer   - the last table that still holds the records intact. Blast
                     radius is "everything downstream of the origin", because
                     that is where the records are now missing.
  * detected layer - the table whose job raised the failure. This is the table
                     in the incident signature, so it must match what the Learn
                     node writes to (:Incident {table_name}).

Folder branches (picture: bronze_crash1 -> silver1_crash1 -> silver2_crash1) are
resolved later against the lineage graph: `silver2_crash1` if that node exists,
otherwise the single-table name `silver2` used on main today.
"""
import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import List, Optional, Tuple

QUARANTINE = "QUARANTINE"
QWEN = "QWEN"

SCHEMA_TYPES = {"SCHEMA_MISMATCH", "SCHEMA_DRIFT", "MISSING_COLUMN"}
RECORD_TYPES = {"CORRUPT_RECORD"}
VALUE_TYPES = {"UNRESOLVED_VALUE", "AMBIGUOUS_VALUE", "TYPE_MISMATCH", "PARSE_ERROR"}

MISSING_MANDATORY = "MISSING_MANDATORY_VALUE"  # sentinel silver2_job.py writes for mandatory fields

# Qwen `job_name` -> (origin layer, detected layer).
JOB_LAYERS = {
    "Silver1SchemaValidation": ("bronze", "silver1"),
    "Silver1": ("bronze", "silver1"),
    "Silver2": ("silver1", "silver2"),
    "Gold": ("silver2", "gold"),
}


# Layers with one table per data folder (spark-jobs/sources.py): silver2_crash3.
PER_SOURCE_LAYERS = ("bronze", "silver1", "silver2")
_SHARED_SUFFIXES = {"quarantine"}  # silver2_quarantine is shared, not a folder


def split_table(name: Optional[str]):
    """'silver2_crash3' -> ('silver2', 'crash3'); anything else -> (None, None).
    Source names are letters and digits only (sources.py), so one '_' splits them."""
    if not name:
        return None, None
    name = name.strip().rstrip("/").split("/")[-1]
    layer, sep, rest = name.partition("_")
    if sep and layer in PER_SOURCE_LAYERS and rest and "_" not in rest and rest not in _SHARED_SUFFIXES:
        return layer, rest
    return None, None


def family(failure_type: Optional[str]) -> str:
    ft = (failure_type or "").upper()
    if ft in SCHEMA_TYPES:
        return "SCHEMA"
    if ft in RECORD_TYPES:
        return "RECORD"
    if ft in VALUE_TYPES:
        return "VALUE"
    return "UNKNOWN"


def schema_value(expected_only, source_only) -> str:
    """Stable text for a column-set difference; it is the `raw_value` of schema failures,
    so the same rename seen in another batch has the same signature."""
    return (
        "EXPECTED_ONLY=" + ",".join(sorted(expected_only or ()))
        + ";SOURCE_ONLY=" + ",".join(sorted(source_only or ()))
    )


@dataclass(frozen=True)
class StructuredFailure:
    source: str                       # QUARANTINE | QWEN
    failure_type: str                 # Qwen type, or UNRESOLVED_VALUE for quarantine
    origin_layer: str                 # bronze | silver1 | silver2 | gold
    detected_layer: str
    folder: Optional[str] = None      # producer table_name, e.g. "Crash1"
    column_name: Optional[str] = None
    raw_value: Optional[str] = None
    expected_only: Tuple[str, ...] = ()
    source_only: Tuple[str, ...] = ()
    rows_affected: Optional[int] = None
    batch_id: Optional[str] = None
    mandatory: bool = False
    origin_affected: bool = False     # True when the origin table itself stopped (job crash)
    trusted: bool = True              # False when the LLM classification needs review
    sample_record_ids: Tuple[str, ...] = ()
    evidence: dict = field(default_factory=dict)  # incident_id, dead_letter_ref, error_class, ...

    @property
    def family(self) -> str:
        return family(self.failure_type)

    @property
    def signature_value(self) -> Optional[str]:
        if self.family == "SCHEMA" and (self.expected_only or self.source_only):
            return schema_value(self.expected_only, self.source_only)
        return self.raw_value

    @property
    def failure_id(self) -> str:
        """Same failure seen twice in a window (Kafka redelivery) -> same id."""
        identity = {
            "source": self.source, "type": self.failure_type, "folder": self.folder,
            "detected": self.detected_layer, "column": self.column_name,
            "value": (self.signature_value or "").strip().upper(),
            "batch": self.batch_id, "incident": self.evidence.get("incident_id"),
        }
        return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]

    def to_dict(self) -> dict:
        data = asdict(self)
        data["failure_id"] = self.failure_id
        data["family"] = self.family
        data["signature_value"] = self.signature_value
        for key in ("expected_only", "source_only", "sample_record_ids"):
            data[key] = list(data[key])
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "StructuredFailure":
        known = set(cls.__dataclass_fields__)
        kwargs = {k: v for k, v in data.items() if k in known}
        for key in ("expected_only", "source_only", "sample_record_ids"):
            kwargs[key] = tuple(kwargs.get(key) or ())
        return cls(**kwargs)


def _layers_for_job(job_name: Optional[str], affected_dataset: Optional[str]):
    """Return (origin, detected, origin_affected). `affected_dataset` may be a layer
    ("bronze") or, since the per-folder split, a table ("bronze_crash3")."""
    layer, _ = split_table(affected_dataset)
    affected_dataset = layer or affected_dataset
    if job_name in JOB_LAYERS:
        origin, detected = JOB_LAYERS[job_name]
        return (affected_dataset or origin), detected, False
    if job_name and job_name.startswith("Pipeline-"):
        # run_layers.py crashed: every listed layer stopped writing.
        layers = job_name[len("Pipeline-"):].split("-")
        return layers[0], layers[0], True
    return affected_dataset, affected_dataset, False


def from_qwen_incident(msg: dict) -> Optional[StructuredFailure]:
    """Map a qwen_classifier handoff message. Returns None if it names no table at all."""
    columns = msg.get("affected_columns") or {}
    ref = msg.get("dead_letter_ref") or {}
    origin, detected, origin_affected = _layers_for_job(msg.get("job_name"), msg.get("affected_dataset"))
    if not origin:
        return None
    failure_type = msg.get("failure_type") or "UNKNOWN"
    # The deterministic schema guard knows the class even when Qwen does not.
    detected_class = msg.get("detected_error_class") or ref.get("error_class")
    if failure_type == "UNKNOWN" and detected_class in SCHEMA_TYPES | RECORD_TYPES:
        failure_type = detected_class
    trusted = msg.get("classification_status") == "OK" and not msg.get("needs_review", True)
    batch = msg.get("batch_id")
    # Folder: producer name ("Crash3"), or taken from a per-folder table name
    # ("silver2_crash3" from Silver 2, "bronze_crash3" from the schema guard).
    table = msg.get("table") or ref.get("source_table")
    folder = (split_table(table)[1] or table
              or split_table(msg.get("affected_dataset"))[1])
    return StructuredFailure(
        source=QWEN,
        failure_type=failure_type,
        origin_layer=origin,
        detected_layer=detected,
        folder=folder,
        column_name=columns.get("reported_column") or msg.get("column"),
        raw_value=None,
        expected_only=tuple(sorted(columns.get("expected_only") or ref.get("expected_only") or ())),
        source_only=tuple(sorted(columns.get("source_only") or ref.get("source_only") or ())),
        rows_affected=msg.get("affected_record_count"),
        batch_id=None if batch is None else str(batch),
        origin_affected=origin_affected,
        trusted=trusted,
        evidence={
            "incident_id": msg.get("incident_id"),
            "job_name": msg.get("job_name"),
            "error_class": msg.get("error_class"),
            "classification_status": msg.get("classification_status"),
            "handoff_status": msg.get("handoff_status"),
            "source_topic": msg.get("source_topic"),
            "dead_letter_ref": ref or None,
        },
    )


def group_quarantine_rows(rows: List[dict], sample_size: int = 5) -> List[StructuredFailure]:
    """Group quarantine rows into one failure per (folder, column, normalised value).

    Each row is a dict with `table_name`, `record_id`, `silver2_batch_id`,
    `columns_to_remediate` (column -> raw value, a dict or a list of pairs) and,
    since the per-folder split, `target_table` ("silver2_crash3"), which names the
    folder more reliably than the producer's `table_name`. A row that fails on
    three columns contributes to three groups.
    """
    groups = {}
    for row in rows:
        folder = split_table(row.get("target_table"))[1] or row.get("table_name")
        failing = row.get("columns_to_remediate") or {}
        if not isinstance(failing, dict):
            failing = dict(failing)
        for column, value in failing.items():
            key = (folder, column, (value or "").strip().upper())
            g = groups.setdefault(key, {"rows": 0, "ids": [], "batches": set(), "value": value})
            g["rows"] += 1
            if row.get("record_id") and len(g["ids"]) < sample_size:
                g["ids"].append(row["record_id"])
            if row.get("silver2_batch_id") is not None:
                g["batches"].add(str(row["silver2_batch_id"]))

    failures = []
    for (folder, column, _), g in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        mandatory = g["value"] == MISSING_MANDATORY
        failures.append(StructuredFailure(
            source=QUARANTINE,
            failure_type="UNRESOLVED_VALUE",
            origin_layer="silver1",
            detected_layer="silver2",
            folder=folder,
            column_name=column,
            raw_value=None if mandatory else g["value"],
            rows_affected=g["rows"],
            batch_id=",".join(sorted(g["batches"])) or None,
            mandatory=mandatory,
            trusted=True,
            sample_record_ids=tuple(g["ids"]),
            evidence={"quarantine_batches": sorted(g["batches"])},
        ))
    return failures
