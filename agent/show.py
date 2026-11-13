"""Print the saved state of one recovery workflow as JSON.

  docker compose exec recovery-agent python -m agent.show <failure_id>

The failure_id is the first word of each workflow line in the agent's log.
"""
import json
import sys

from agent import config
from agent.graph import build_graph, open_checkpointer


def saved_state(graph, failure_id: str) -> dict:
    """The workflow's state plus `next`: the nodes still to run (empty when it has ended)."""
    saved = graph.get_state({"configurable": {"thread_id": failure_id}})
    if not saved.values:
        return {}
    return {**saved.values, "next": list(saved.next)}


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: python -m agent.show <failure_id>")
    state = saved_state(build_graph(open_checkpointer(config.CHECKPOINT_PATH)), sys.argv[1])
    if not state:
        sys.exit(f"No workflow saved for failure_id {sys.argv[1]}")
    print(json.dumps(state, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
