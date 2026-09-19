from datetime import datetime

from pyspark.sql.types import StructType, StructField, LongType, BooleanType, TimestampType

from nyc311.quality import (
    count_duplicates,
    count_bounds_violations,
    freshness_summary,
    run_quality_checks,
)

SCHEMA = StructType(
    [
        StructField("complaint_id", LongType()),
        StructField("_duration_valid", BooleanType()),
        StructField("_ingested_at", TimestampType()),
    ]
)


def row(complaint_id, duration_valid=True, ingested_at=datetime(2026, 1, 1)):
    return (complaint_id, duration_valid, ingested_at)


def test_count_duplicates_is_zero_when_key_is_unique(spark):
    df = spark.createDataFrame([row(1), row(2), row(3)], SCHEMA)
    assert count_duplicates(df, "complaint_id") == 0


def test_count_duplicates_counts_the_excess_rows(spark):
    df = spark.createDataFrame([row(1), row(1), row(2)], SCHEMA)
    # 3 rows, 2 distinct ids -> 1 duplicate row
    assert count_duplicates(df, "complaint_id") == 1


def test_count_bounds_violations(spark):
    df = spark.createDataFrame(
        [row(1, duration_valid=True), row(2, duration_valid=False), row(3, duration_valid=False)],
        SCHEMA,
    )
    assert count_bounds_violations(df) == 2


def test_freshness_summary(spark):
    df = spark.createDataFrame(
        [row(1, ingested_at=datetime(2026, 1, 1)), row(2, ingested_at=datetime(2026, 1, 5))],
        SCHEMA,
    )
    result = freshness_summary(df)

    assert result["total_rows"] == 2
    assert result["latest_ingested_at"] == datetime(2026, 1, 5)


def test_run_quality_checks_combines_all_three(spark):
    df = spark.createDataFrame(
        [
            row(1, duration_valid=True, ingested_at=datetime(2026, 1, 1)),
            row(1, duration_valid=False, ingested_at=datetime(2026, 1, 2)),
        ],
        SCHEMA,
    )
    result = run_quality_checks(df, "complaint_id")

    assert result == {
        "duplicate_count": 1,
        "bounds_violation_count": 1,
        "latest_ingested_at": datetime(2026, 1, 2),
        "total_rows": 2,
    }
