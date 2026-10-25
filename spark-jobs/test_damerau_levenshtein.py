"""
Tests for Damerau-Levenshtein fuzzy matching (spec: Silver 1 / Silver 2 Fuzzy Match).

Pure-Python checks run anywhere; the Spark checks need only a local SparkSession
(no Delta tables), so this file can run without the rest of the pipeline.

Run from spark-jobs/:  python test_damerau_levenshtein.py
"""

from pyspark.sql import SparkSession

from ref_mappings import (
    CANONICAL_ANCHORS,
    damerau_levenshtein,
    damerau_levenshtein_udf,
    fuzzy_canonical,
)


def test_distance() -> None:
    assert damerau_levenshtein("JUNE", "JUNE") == 0
    assert damerau_levenshtein("", "JUNE") == 4
    assert damerau_levenshtein("JUNE", "") == 4
    assert damerau_levenshtein(None, "JUNE") is None
    # Insertion, deletion, substitution
    assert damerau_levenshtein("MARCHH", "MARCH") == 1
    assert damerau_levenshtein("OCTOBR", "OCTOBER") == 1
    assert damerau_levenshtein("JULE", "JUNE") == 1
    assert damerau_levenshtein("JULE", "JULY") == 1
    # Adjacent transposition is a single edit (plain Levenshtein charges 2)
    assert damerau_levenshtein("JUEN", "JUNE") == 1
    assert damerau_levenshtein("TEUSDAY", "TUESDAY") == 1
    # Unrestricted variant: CA -> ABC is 2 (OSA would say 3)
    assert damerau_levenshtein("CA", "ABC") == 2
    # Symmetric
    assert damerau_levenshtein("SUNDAY", "SATURDAY") == damerau_levenshtein("SATURDAY", "SUNDAY")
    print("  -> damerau_levenshtein distances: PASSED")


def test_spark(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("JUEN",), ("MARHC",), ("Marchh",), ("JULE",), ("JUNY",), ("JU",), ("  august  ",), (None,)],
        "raw string",
    )
    out = {
        r.raw: r.matched
        for r in df.select(
            "raw", fuzzy_canonical("raw", CANONICAL_ANCHORS["crash_month"]).alias("matched")
        ).collect()
    }
    # Transposition costs 1: MARHC (len 5, limit 1) was out of reach at Levenshtein distance 2
    assert out["MARHC"] == "MARCH", out
    # JUEN is 1 from JUNE but only 2 from JULY, so the margin rule still keeps it in quarantine
    assert out["JUEN"] is None, out
    assert out["Marchh"] == "MARCH", out
    assert out["JULE"] is None, out  # one edit from JUNE and JULY: no single clear winner
    assert out["JUNY"] is None, out
    assert out["JU"] is None, out  # shorter than 4 characters
    assert out["  august  "] == "AUGUST", out
    assert out[None] is None, out
    print("  -> fuzzy_canonical on crash_month anchors: PASSED")

    dist = df.select("raw", damerau_levenshtein_udf("raw", "raw").alias("d")).collect()
    assert all(r.d in (0, None) for r in dist)
    pair = spark.createDataFrame([("JUEN", "JUNE")], "a string, b string").select(
        damerau_levenshtein_udf("a", "b").alias("d")
    ).first()
    assert pair.d == 1, pair
    print("  -> damerau_levenshtein_udf: PASSED")


if __name__ == "__main__":
    print("[TEST] Damerau-Levenshtein fuzzy matching")
    test_distance()
    spark = SparkSession.builder.master("local[1]").appName("dl_test").getOrCreate()
    try:
        test_spark(spark)
    finally:
        spark.stop()
    print("All Damerau-Levenshtein tests passed.")
