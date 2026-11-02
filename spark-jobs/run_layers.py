"""
Run several pipeline layers as streaming queries inside ONE Spark driver.

Bronze, Silver 1 and Silver 2 run one query per source (sources.py), so
`run_layers.py bronze silver1` with four sources starts 8 queries and
`run_layers.py silver2 gold` starts 5 (four Silver 2 plus one Gold over all of
them). Each layer is light (a few hundred rows per micro-batch), so giving every
query its own container + JVM would spend most of the stack's memory on JVM
overhead.

Layers start in the order given. Each per-source query blocks in
wait_for_delta_table() until its own upstream table exists; the matching query
of the layer before it (in this same process) creates it.

Usage:
    python run_layers.py bronze silver1
    python run_layers.py silver2 gold

Each <layer>_job.py still runs on its own too (`python silver2_job.py`).
"""

import os
import sys
from typing import Dict

import bronze_job
import gold_job
import silver1_job
import silver2_job
from common import build_spark
from pyspark.sql.streaming import StreamingQuery
from sources import source_names

PER_SOURCE_STARTERS = {
    "bronze": bronze_job.start,
    "silver1": silver1_job.start,
    "silver2": silver2_job.start,
}
LAYERS = [*PER_SOURCE_STARTERS, "gold"]


def main(layers):
    unknown = [name for name in layers if name not in LAYERS]
    if not layers or unknown:
        sys.exit(
            f"Usage: python run_layers.py <layer> [<layer> ...] "
            f"(layers: {', '.join(LAYERS)}; unknown: {unknown})"
        )

    sources = source_names()
    spark = build_spark(
        "Pipeline-" + "-".join(layers),
        driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "1g"),
    )
    print(f"[run_layers] layers={layers} sources={sources}", flush=True)

    # The previous layer's query per source; empty for the first layer here.
    upstream: Dict[str, StreamingQuery] = {}
    for name in layers:
        if name == "gold":
            gold = gold_job.start(spark, sources, upstreams=upstream)
            upstream = {source: gold for source in sources}
        else:
            start = PER_SOURCE_STARTERS[name]
            upstream = {
                source: start(spark, source, upstream=upstream.get(source))
                for source in sources
            }

    # Raises as soon as any query fails, so `restart: on-failure` restarts the
    # whole container and every query resumes from its own checkpoint.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    from qwen_classifier.integration import run_job
    run_job(lambda: main(sys.argv[1:]), job_name="Pipeline-" + "-".join(sys.argv[1:]))
