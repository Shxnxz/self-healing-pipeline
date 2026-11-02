"""
Per-source table layout: one Bronze / Silver 1 / Silver 2 branch per data folder.

Each data folder (data/Crash1 .. data/Crash4) is a separate source. It gets its
own Kafka topic and its own Bronze, Silver 1 and Silver 2 Delta tables, so a
problem in one folder stays in that folder's branch. The branches meet only in
Gold, which is a UNION ALL of every Silver 2 table.

    crashes.crash3 -> bronze_crash3 -> silver1_crash3 -> silver2_crash3 -> gold

The side tables stay shared (one copy each). Every row in them carries the
source it came from, and the healing loop reads them from one place:
    silver1_dead_letter, silver2_quarantine, silver2_dead_letter,
    silver2_fuzzy_audit, ref_canonical_mappings

Table folders sit directly under DELTA_ROOT (`/data/delta/silver2_crash3`, not
`/data/delta/silver2/crash3`): lineage/normalize.py only turns a folder directly
under the root into a (:Table) node, so a nested path would vanish from Neo4j.

SOURCES must list the same folders the producer publishes (its TABLES setting,
lower-cased). A source with no data never creates its Bronze table, and the
layers after it wait for that table forever.
"""

import os
import re
from typing import List, Optional

DELTA_ROOT = os.environ.get("DELTA_ROOT", "/data/delta").rstrip("/")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/data/checkpoints").rstrip("/")
KAFKA_TOPIC_PREFIX = os.environ.get("KAFKA_TOPIC_PREFIX", "crashes")
DEFAULT_SOURCES = "crash1,crash2,crash3,crash4"

# Layers that have one table per source. Gold is a single table over all of them.
PER_SOURCE_LAYERS = ("bronze", "silver1", "silver2")

_SOURCE_NAME = re.compile(r"^[a-z][a-z0-9]*$")


def source_names(raw: Optional[str] = None) -> List[str]:
    """Sources from SOURCES (comma separated), lower-cased, in the order given."""
    if raw is None:
        raw = os.environ.get("SOURCES", DEFAULT_SOURCES)
    names: List[str] = []
    for part in raw.split(","):
        name = part.strip().lower()
        if not name:
            continue
        # Letters and digits only: the name is used in paths, query names and
        # txnAppIds, and an underscore would make `silver2_<source>` ambiguous.
        if not _SOURCE_NAME.match(name):
            raise ValueError(f"Invalid source name {part!r}: use letters and digits, e.g. crash1")
        if name not in names:
            names.append(name)
    if not names:
        raise ValueError("SOURCES is empty: list at least one source, e.g. crash1,crash2")
    return names


def kafka_topic(source: str) -> str:
    """Matches producer.get_topic_for_table: Crash3 -> crashes.crash3."""
    return f"{KAFKA_TOPIC_PREFIX}.{source}"


def table_name(layer: str, source: str) -> str:
    if layer not in PER_SOURCE_LAYERS:
        raise ValueError(f"{layer!r} has no per-source tables (per-source layers: {PER_SOURCE_LAYERS})")
    return f"{layer}_{source}"


def table_path(layer: str, source: str) -> str:
    return f"{DELTA_ROOT}/{table_name(layer, source)}"


def checkpoint_path(layer: str, source: str) -> str:
    return f"{CHECKPOINT_ROOT}/{table_name(layer, source)}"


def shared_path(name: str) -> str:
    """A table every source writes to, e.g. shared_path('silver2_quarantine')."""
    return f"{DELTA_ROOT}/{name}"
