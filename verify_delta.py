"""
Quick sanity check, run from the host (not inside a container).

Reads any of the pipeline's Delta tables directly using the lightweight
`deltalake` package -- no Spark/JVM required. Handy for confirming data is
flowing through all three layers while the docker-compose stack runs.

Usage:
    pip install deltalake pandas
    python verify_delta.py bronze_crash1
    python verify_delta.py silver2_crash3
    python verify_delta.py gold
    python verify_delta.py remediation_audit
"""

import sys

from deltalake import DeltaTable

SOURCES = ["crash1", "crash2", "crash3", "crash4"]  # spark-jobs/sources.py

TABLES = {
    # One table per source and layer: bronze_crash1 .. silver2_crash4
    **{
        f"{layer}_{source}": f"./delta/{layer}_{source}"
        for layer in ("bronze", "silver1", "silver2")
        for source in SOURCES
    },
    "silver2_quarantine": "./delta/silver2_quarantine",
    "quarantine": "./delta/silver2_quarantine",
    "ref_mappings": "./delta/ref_canonical_mappings",
    "fuzzy_audit": "./delta/silver2_fuzzy_audit",
    "audit": "./delta/silver2_fuzzy_audit",
    "dead_letter": "./delta/silver2_dead_letter",
    "silver2_dead_letter": "./delta/silver2_dead_letter",
    "remediation_audit": "./delta/remediation_audit",
    "gold": "./delta/gold",
    # Legacy aliases
    "silver_cars": "./delta/silver_cars",
    "gold_car_overview": "./delta/gold_car_overview",
}

if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "gold"
    if name not in TABLES:
        print(f"Unknown table '{name}'. Choose from: {list(TABLES)}")
        sys.exit(1)

    dt = DeltaTable(TABLES[name])
    df = dt.to_pandas()
    print(f"Table: {name}  (version {dt.version()}, {len(df)} rows)")
    print(df.tail(10).to_string())
