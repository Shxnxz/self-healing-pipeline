"""Shared by the replay tool and the fault injector: the pipeline's own Spark modules.

Both tools run the pipeline's code (spark-jobs/), never a copy of it: Silver 1's
SQL, Silver 2's standardize_silver2 and write stage, the reference mappings and the
per-source table layout. In Docker they run in the `tools` service, which is the
Spark jobs' image with the repo mounted at /repo (docker-compose.yml, profile
`tools`). Outside Docker, run them from the repo root with the same environment
variables the Spark jobs get (DELTA_ROOT, SOURCES, DAY_FIRST_SOURCES, ...).
"""
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_JOBS = os.environ.get("SPARK_JOBS_DIR", os.path.join(REPO, "spark-jobs"))

_spark = None


def pipeline_modules() -> None:
    """Make spark-jobs/ (and the repo root, for qwen_classifier) importable, here and
    in Spark's Python workers: the fuzzy-match UDF of ref_mappings runs in a worker."""
    if not os.path.isfile(os.path.join(SPARK_JOBS, "silver2_job.py")):
        raise RuntimeError(f"spark-jobs not found at {SPARK_JOBS} (SPARK_JOBS_DIR)")
    for path in (REPO, SPARK_JOBS):
        if path not in sys.path:
            sys.path.insert(0, path)
    parts = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    missing = [p for p in (SPARK_JOBS, REPO) if p not in parts]
    if missing:
        os.environ["PYTHONPATH"] = os.pathsep.join(missing + parts)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def spark(app_name: str):
    """One Spark session per tool run, built like the pipeline's (common.build_spark)."""
    global _spark
    if _spark is not None and _spark.sparkContext._jsc is not None:
        return _spark
    pipeline_modules()
    from common import build_spark

    _spark = build_spark(app_name, driver_memory=os.environ.get("TOOLS_SPARK_DRIVER_MEMORY", "1g"))
    return _spark


def is_delta(path: str) -> bool:
    return os.path.isdir(os.path.join(path, "_delta_log"))
