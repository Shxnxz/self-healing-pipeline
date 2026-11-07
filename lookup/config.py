"""Every tunable number of the Deterministic Lookup, in one place.

All values can be overridden with environment variables so they can be calibrated
without a code change (see README "Tuning").
"""
import json
import os


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _json(name: str, default):
    raw = os.environ.get(name)
    return json.loads(raw) if raw else default


# --- Kafka --------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
QWEN_TOPIC = os.environ.get("QWEN_INCIDENT_TOPIC", "recovery.incidents")
FAILURE_TOPIC = os.environ.get("QUARANTINE_FAILURE_TOPIC", "recovery.failures")
OUTPUT_TOPIC = os.environ.get("LOOKUP_OUTPUT_TOPIC", "recovery.lookup")
GROUP_ID = os.environ.get("LOOKUP_CONSUMER_GROUP", "deterministic-lookup")

# Failures that arrive within one window are ranked against each other and
# published together as one stack.
WINDOW_SECONDS = _float("LOOKUP_WINDOW_SECONDS", 10)
MAX_WINDOW_SIZE = int(os.environ.get("LOOKUP_MAX_WINDOW_SIZE", 200))

# --- Quarantine scanner ---------------------------------------------------------
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/data/delta/silver2_quarantine")
SCAN_INTERVAL_SECONDS = _float("SCAN_INTERVAL_SECONDS", 15)
SCANNER_STATE_PATH = os.environ.get("SCANNER_STATE_PATH", "/state/quarantine_scanner.json")
SAMPLE_RECORD_IDS = 5

# --- Neo4j --------------------------------------------------------------------
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "selfhealing")

# --- Blast radius ---------------------------------------------------------------
# How much a downstream table counts. Matched by longest name prefix, so
# "silver2_quarantine" hits its own entry before "silver2". Side tables
# (quarantine, dead letter, audit) are where failures go, not consumers: 0.
LAYER_WEIGHTS = _json("BLAST_LAYER_WEIGHTS", {
    "silver1_dead_letter": 0.0,
    "silver2_dead_letter": 0.0,
    "silver2_quarantine": 0.0,
    "silver2_fuzzy_audit": 0.0,
    "ref_canonical_mappings": 0.0,
    "bronze": 1.0,
    "silver1": 2.0,
    "silver2": 3.0,
    "gold": 5.0,
})
DEFAULT_LAYER_WEIGHT = _float("BLAST_DEFAULT_LAYER_WEIGHT", 0.5)
LAYER_ORDER = ["bronze", "silver1", "silver2", "gold"]

# Reach saturates: reach_norm = reach / (reach + REACH_HALF). With the default
# weights a failure that only reaches gold (5) scores 0.5.
REACH_HALF = _float("BLAST_REACH_HALF", 5.0)
# rows_norm = log10(1 + rows) / log10(1 + ROWS_CAP), capped at 1.
ROWS_CAP = _float("BLAST_ROWS_CAP", 10_000)

# The five factors of the impact score; must sum to 1.
SCORE_WEIGHTS = _json("BLAST_SCORE_WEIGHTS", {
    "severity": 0.30,     # how much of the data the failure type stops
    "reach": 0.25,        # weighted downstream tables (lineage graph)
    "volume": 0.20,       # rows affected
    "propagation": 0.15,  # does the failing column exist in downstream schemas
    "blocks": 0.10,       # other failures in this window that sit downstream of it
})

# 0..1: what fraction of a batch a failure type stops.
SEVERITY = _json("BLAST_SEVERITY", {
    "SCHEMA_MISMATCH": 1.0,
    "SCHEMA_DRIFT": 1.0,
    "MISSING_COLUMN": 1.0,
    "CORRUPT_RECORD": 0.9,
    "TYPE_MISMATCH": 0.8,
    "PARSE_ERROR": 0.7,
    "UNKNOWN": 0.6,
    "AMBIGUOUS_VALUE": 0.4,
    "UNRESOLVED_VALUE": 0.4,
})
MANDATORY_SEVERITY_BONUS = _float("BLAST_MANDATORY_BONUS", 0.3)

# Impact score (0..100) -> priority tier.
TIERS = [("P1", 70.0), ("P2", 40.0), ("P3", 0.0)]

# --- Incident pattern ---------------------------------------------------------
# Structured similarity (0..1) needed to reuse a past fix. Calibrate it on
# labelled incident pairs once the corpus has some (lookup/README.md, "Tuning").
SIMILARITY_THRESHOLD = _float("LOOKUP_SIMILARITY_THRESHOLD", 0.85)
SIMILARITY_WEIGHTS = _json("LOOKUP_SIMILARITY_WEIGHTS", {
    "failure_type": 0.25,
    "column": 0.25,
    "table": 0.15,
    "value": 0.35,
})
MAX_CANDIDATES = int(os.environ.get("LOOKUP_MAX_CANDIDATES", 200))
TOP_K = int(os.environ.get("LOOKUP_TOP_K", 3))

# Optional embedding recall (Ollama). Empty = structured similarity only.
EMBED_MODEL = os.environ.get("LOOKUP_EMBED_MODEL", "")
EMBED_BASE_URL = os.environ.get("LOOKUP_EMBED_BASE_URL", "http://host.docker.internal:11434")
