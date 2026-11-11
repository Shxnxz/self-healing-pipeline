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
