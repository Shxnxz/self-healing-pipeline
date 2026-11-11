"""Settings of the recovery agent, overridable with environment variables."""
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# Same variable the Deterministic Lookup service publishes with (lookup/config.py).
LOOKUP_TOPIC = os.environ.get("LOOKUP_OUTPUT_TOPIC", "recovery.lookup")
GROUP_ID = os.environ.get("AGENT_CONSUMER_GROUP", "recovery-agent")

# LangGraph checkpoints: the saved state of every recovery workflow.
CHECKPOINT_PATH = os.environ.get("AGENT_CHECKPOINT_PATH", "/state/recovery_checkpoints.sqlite")

# The agent polls Kafka between the workflows of a stack, so this only has to
# cover the slowest single workflow (its LLM call plus the retries below).
MAX_POLL_INTERVAL_MS = int(os.environ.get("AGENT_MAX_POLL_INTERVAL_MS", 30 * 60 * 1000))

# A step that raises is tried this many times, waiting NODE_RETRY_SECONDS and then
# twice as long each time, before the failure is handed to an engineer.
NODE_MAX_ATTEMPTS = int(os.environ.get("AGENT_NODE_MAX_ATTEMPTS", 3))
NODE_RETRY_SECONDS = float(os.environ.get("AGENT_NODE_RETRY_SECONDS", 2))

# --- Diagnose: the reasoning model (Qwen served by Ollama) --------------------
# The triage model (qwen3:4b-instruct) classifies exceptions; this one reasons about
# the fix, so it can be a larger Qwen. Anything Ollama serves works.
DIAGNOSE_MODEL = os.environ.get("DIAGNOSE_MODEL", "qwen3:8b")
DIAGNOSE_BASE_URL = os.environ.get(
    "DIAGNOSE_BASE_URL", os.environ.get("QWEN_BASE_URL", "http://host.docker.internal:11434"))
DIAGNOSE_TIMEOUT_SECONDS = float(os.environ.get("DIAGNOSE_TIMEOUT_SECONDS", 120))
DIAGNOSE_NUM_CTX = int(os.environ.get("DIAGNOSE_NUM_CTX", 8192))
DIAGNOSE_NUM_PREDICT = int(os.environ.get("DIAGNOSE_NUM_PREDICT", 1024))
# Qwen3 can "think" before it answers. The thinking counts against
# DIAGNOSE_NUM_PREDICT, so a long one cuts the answer off. false = answer directly
# (default), true = think first (raise DIAGNOSE_NUM_PREDICT), default = leave it to
# the model.
DIAGNOSE_THINK = {"false": False, "true": True, "default": None}.get(
    os.environ.get("DIAGNOSE_THINK", "false").strip().lower() or "false", False)
# Quarantined records shown to the model as evidence (0 = none).
DIAGNOSE_SAMPLE_RECORDS = int(os.environ.get("DIAGNOSE_SAMPLE_RECORDS", 3))

# Read-only Delta tables the agent takes its evidence from.
REF_MAPPINGS_PATH = os.environ.get("REF_MAPPINGS_PATH", "/data/delta/ref_canonical_mappings")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/data/delta/silver2_quarantine")

# --- Policy gate (spec AP2, AP3) -----------------------------------------------
def _threshold(name: str, default: float, lowest: float, never: bool = False) -> float:
    """A confidence threshold from the environment. A value that would wave
    proposals through (0, a typo, a leftover from testing) stops the agent at start."""
    raw = os.environ.get(name, "").strip()
    if never and raw.upper() == "NEVER":
        return float("inf")
    try:
        value = float(raw) if raw else default
    except ValueError:
        raise ValueError(f"{name}={raw!r} is not a number") from None
    if not lowest <= value <= 1.0:
        raise ValueError(f"{name}={value} must be between {lowest} and 1.0")
    return value


# A diagnosis is executed without an engineer only when its confidence is at least
# this AND its action is in the safe set (agent/actions.py).
POLICY_CONFIDENCE_THRESHOLD = _threshold("POLICY_CONFIDENCE_THRESHOLD", 0.85, lowest=0.5)
# Stricter threshold for a proposal that nothing in the records can confirm
# (agent/evidence.py), where the model's confidence is all there is. NEVER sends
# every such proposal to an engineer. It is never below the threshold above.
POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD = max(POLICY_CONFIDENCE_THRESHOLD, _threshold(
    "POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD", 0.95, lowest=0.5, never=True))

# Optional comma-separated list that narrows the safe set. NONE anywhere in it means
# every diagnosis is reviewed by an engineer. Unset or empty = every action marked
# safe in agent/actions.py. It cannot add an action to the safe set.
_safe = [a.strip().upper() for a in os.environ.get("POLICY_SAFE_ACTIONS", "").split(",") if a.strip()]
POLICY_SAFE_ACTIONS = None if not _safe else ([] if "NONE" in _safe else _safe)
