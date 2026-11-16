"""Keeping the agent's own storage small (Task 109).

Two things grow with every failure the agent handles:

  checkpoints        LangGraph saves the state after every node, so a finished workflow
                     keeps a dozen checkpoints (and their pending writes) in the SQLite
                     file of the agent-state volume, forever.
  remediation_audit  one Delta commit, and one small Parquet file, per execution attempt.
                     Remediate reads the table on every execution (idempotency check).

Pruning: a FINISHED workflow keeps only its last checkpoint. That checkpoint holds the
whole final state (status, trace, escalation, learned, ...), so `agent.show`, `agent.review`
and the consumer's duplicate check read exactly what they read before. Not pruned:
  - a workflow waiting for an engineer, or stopped part-way (it resumes from them);
  - a workflow that still needs Learn: relearn replays Learn from the checkpoint saved
    just before it (consumer.before_learn).
The agent prunes every workflow it finishes, and at start every finished workflow saved
before (AGENT_PRUNE_CHECKPOINTS=false turns it off).

Compaction: every AGENT_AUDIT_COMPACT_INTERVAL_SECONDS (default one hour) the audit table
is compacted (deltalake optimize: many small files become a few) once it has more than
AGENT_AUDIT_COMPACT_MIN_FILES files, its log is checkpointed, and files that compaction
replaced more than AGENT_AUDIT_VACUUM_RETENTION_HOURS ago (default 168, Delta's own
default) are vacuumed. Compaction rewrites the same rows, so audit.find and Validate read
exactly what they read before; a reader holding an older snapshot still finds its files
until the retention has passed. 0 as the interval turns compaction off.

Both also run on demand:

    docker compose exec recovery-agent python -m agent.upkeep            # prune + compact now
    docker compose exec recovery-agent python -m agent.upkeep --dry-run  # only say what it would do
"""
import argparse
import sqlite3
import time
from typing import Callable, Iterable, List, Optional

from agent import config

PRUNE_WRITES = """
DELETE FROM writes WHERE thread_id = ? AND checkpoint_id < (
  SELECT MAX(c.checkpoint_id) FROM checkpoints c
  WHERE c.thread_id = writes.thread_id AND c.checkpoint_ns = writes.checkpoint_ns)
"""
PRUNE_CHECKPOINTS = """
DELETE FROM checkpoints WHERE thread_id = ? AND checkpoint_id < (
  SELECT MAX(c.checkpoint_id) FROM checkpoints c
  WHERE c.thread_id = checkpoints.thread_id AND c.checkpoint_ns = checkpoints.checkpoint_ns)
"""


def _conn(checkpointer) -> Optional[sqlite3.Connection]:
    conn = getattr(checkpointer, "conn", None)
    return conn if isinstance(conn, sqlite3.Connection) else None


def prunable(saved) -> bool:
    """The workflow ended for good: nothing will resume, relearn or decide it."""
    from agent.consumer import needs_learn   # consumer imports this module

    values = saved.values or {}
    return bool(values) and not saved.next and not saved.interrupts and not needs_learn(saved)


def checkpoint_count(checkpointer, thread_id: str) -> int:
    conn = _conn(checkpointer)
    if conn is None:
        return 0
    try:
        return conn.execute("SELECT COUNT(*) FROM checkpoints WHERE thread_id = ?", (thread_id,)).fetchone()[0]
    except sqlite3.OperationalError:   # no tables yet
        return 0


def prune_thread(graph, thread_id: str, dry_run: bool = False) -> int:
    """Drop every checkpoint of a finished workflow but its last. Returns how many
    checkpoints went (0 when the workflow is not finished or the saver is not SQLite)."""
    conn = _conn(graph.checkpointer)
    if conn is None or not prunable(graph.get_state({"configurable": {"thread_id": thread_id}})):
        return 0
    extra = checkpoint_count(graph.checkpointer, thread_id) - 1
    if extra <= 0 or dry_run:
        return max(extra, 0)
    lock = getattr(graph.checkpointer, "lock", None)
    if lock is not None:
        with lock:
            conn.execute(PRUNE_WRITES, (thread_id,))
            conn.execute(PRUNE_CHECKPOINTS, (thread_id,))
            conn.commit()
    else:
        conn.execute(PRUNE_WRITES, (thread_id,))
        conn.execute(PRUNE_CHECKPOINTS, (thread_id,))
        conn.commit()
    return extra


def prune_all(graph, thread_ids: Iterable[str], dry_run: bool = False) -> dict:
    """Prune every finished workflow. VACUUM gives the space back to the file system."""
    pruned, threads = 0, 0
    for thread_id in thread_ids:
        gone = prune_thread(graph, thread_id, dry_run=dry_run)
        pruned += gone
        threads += 1 if gone else 0
    conn = _conn(graph.checkpointer)
    if pruned and conn is not None and not dry_run:
        conn.execute("VACUUM")
    return {"threads": threads, "checkpoints_removed": pruned}


def compact_audit(path: Optional[str] = None, min_files: Optional[int] = None,
                  retention_hours: Optional[float] = None, dry_run: bool = False) -> dict:
    """Compact the remediation audit table when it has more than `min_files` files."""
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    path = path or config.REMEDIATION_AUDIT_PATH
    min_files = config.AUDIT_COMPACT_MIN_FILES if min_files is None else min_files
    retention = config.AUDIT_VACUUM_RETENTION_HOURS if retention_hours is None else retention_hours
    try:
        table = DeltaTable(path)
    except TableNotFoundError:
        return {"status": "NO_TABLE", "files": 0}
    files = len(table.file_uris())
    result = {"status": "SKIPPED", "files": files, "min_files": min_files}
    if files > min_files and not dry_run:
        metrics = table.optimize.compact()
        table = DeltaTable(path)
        table.create_checkpoint()
        result.update(status="COMPACTED", files_after=len(table.file_uris()),
                      files_removed=metrics.get("numFilesRemoved"), files_added=metrics.get("numFilesAdded"))
    elif files > min_files:
        result["status"] = "WOULD_COMPACT"
    vacuumed = table.vacuum(retention_hours=int(retention), dry_run=dry_run, enforce_retention_duration=False)
    result["vacuumed" if not dry_run else "would_vacuum"] = len(vacuumed)
    if not dry_run:
        table.cleanup_metadata()
    return result


class Upkeep:
    """Prunes finished workflows as they finish and compacts the audit table on a timer."""

    def __init__(self, graph, prune: Optional[bool] = None, interval: Optional[float] = None,
                 clock: Callable[[], float] = time.monotonic, audit_path: Optional[str] = None):
        self.graph, self.clock, self.audit_path = graph, clock, audit_path
        self.prune = config.PRUNE_CHECKPOINTS if prune is None else prune
        self.interval = config.AUDIT_COMPACT_INTERVAL_SECONDS if interval is None else interval
        self.next_at = clock() + self.interval

    def start(self, thread_ids: List[str]) -> dict:
        return prune_all(self.graph, thread_ids) if self.prune else {"threads": 0, "checkpoints_removed": 0}

    def finished(self, thread_id: Optional[str]) -> int:
        """Called after every workflow the agent ran, resumed or decided."""
        if not self.prune or not thread_id:
            return 0
        try:
            return prune_thread(self.graph, thread_id)
        except Exception as exc:   # upkeep never stops the agent
            print(f"[agent] pruning the checkpoints of {thread_id} failed: {exc}", flush=True)
            return 0

    def due(self) -> Optional[dict]:
        if self.interval <= 0 or self.clock() < self.next_at:
            return None
        self.next_at = self.clock() + self.interval
        try:
            return compact_audit(self.audit_path)
        except Exception as exc:   # the table is busy or unreadable: try at the next interval
            return {"status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}


def main(argv: Optional[List[str]] = None) -> int:
    from agent.consumer import thread_ids
    from agent.graph import build_graph, open_checkpointer

    parser = argparse.ArgumentParser(prog="python -m agent.upkeep", description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="only report what would be removed")
    parser.add_argument("--min-files", type=int, help="compact the audit table above this many files")
    args = parser.parse_args(argv)
    graph = build_graph(open_checkpointer(config.CHECKPOINT_PATH))
    pruned = prune_all(graph, thread_ids(graph.checkpointer), dry_run=args.dry_run)
    print(f"checkpoints: {pruned['checkpoints_removed']} {'would be ' if args.dry_run else ''}removed "
          f"from {pruned['threads']} finished workflow(s)")
    print(f"remediation_audit: {compact_audit(min_files=args.min_files, dry_run=args.dry_run)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
