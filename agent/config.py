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
