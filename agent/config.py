"""Settings of the recovery agent, overridable with environment variables."""
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# Same variable the Deterministic Lookup service publishes with (lookup/config.py).
LOOKUP_TOPIC = os.environ.get("LOOKUP_OUTPUT_TOPIC", "recovery.lookup")
GROUP_ID = os.environ.get("AGENT_CONSUMER_GROUP", "recovery-agent")

# LangGraph checkpoints: the saved state of every recovery workflow.
CHECKPOINT_PATH = os.environ.get("AGENT_CHECKPOINT_PATH", "/state/recovery_checkpoints.sqlite")

# One recovery.lookup message can hold many failures, and Diagnose will call an
# LLM for each. Kafka must not take a slow stack for a dead consumer.
MAX_POLL_INTERVAL_MS = int(os.environ.get("AGENT_MAX_POLL_INTERVAL_MS", 30 * 60 * 1000))

# --- Diagnose: the reasoning model (Qwen served by Ollama) --------------------
# The triage model (qwen3:4b-instruct) classifies exceptions; this one reasons about
# the fix, so it can be a larger Qwen. Anything Ollama serves works.
DIAGNOSE_MODEL = os.environ.get("DIAGNOSE_MODEL", "qwen3:8b")
DIAGNOSE_BASE_URL = os.environ.get(
    "DIAGNOSE_BASE_URL", os.environ.get("QWEN_BASE_URL", "http://host.docker.internal:11434"))
DIAGNOSE_TIMEOUT_SECONDS = float(os.environ.get("DIAGNOSE_TIMEOUT_SECONDS", 120))
DIAGNOSE_NUM_CTX = int(os.environ.get("DIAGNOSE_NUM_CTX", 8192))
DIAGNOSE_NUM_PREDICT = int(os.environ.get("DIAGNOSE_NUM_PREDICT", 1024))
# Quarantined records shown to the model as evidence (0 = none).
DIAGNOSE_SAMPLE_RECORDS = int(os.environ.get("DIAGNOSE_SAMPLE_RECORDS", 3))

# Read-only Delta tables the agent takes its evidence from.
REF_MAPPINGS_PATH = os.environ.get("REF_MAPPINGS_PATH", "/data/delta/ref_canonical_mappings")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/data/delta/silver2_quarantine")

# --- Policy gate (spec AP2, AP3) -----------------------------------------------
# A diagnosis is executed without an engineer only when its confidence is at least
# this AND its action is in the safe set (agent/actions.py).
POLICY_CONFIDENCE_THRESHOLD = float(os.environ.get("POLICY_CONFIDENCE_THRESHOLD", 0.85))
# Optional comma-separated list that narrows the safe set; "NONE" means every
# diagnosis is reviewed by an engineer. Unset or empty = every action marked safe
# in agent/actions.py. It cannot add an action to the safe set.
_safe = os.environ.get("POLICY_SAFE_ACTIONS", "").strip()
POLICY_SAFE_ACTIONS = None if not _safe else [a for a in _safe.split(",") if a.strip().upper() != "NONE"]
