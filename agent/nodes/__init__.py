"""Nodes of the recovery graph, one module per step (agent/graph.py wires them)."""
from agent.nodes.deterministic_lookup import deterministic_lookup
from agent.nodes.diagnose import diagnose
from agent.nodes.escalate import escalate
from agent.nodes.learn import learn
from agent.nodes.policy_gate import policy_gate
from agent.nodes.remediate import remediate
from agent.nodes.validate import validate

NODES = {
    "deterministic_lookup": deterministic_lookup,
    "diagnose": diagnose,
    "policy_gate": policy_gate,
    "remediate": remediate,
    "validate": validate,
    "escalate": escalate,
    "learn": learn,
}
