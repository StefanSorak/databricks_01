from pyspark.sql import DataFrame, functions as F


def count_duplicates(df: DataFrame, key_col: str) -> int:
    return df.count() - df.select(key_col).distinct().count()


def count_bounds_violations(df: DataFrame) -> int:
    return df.filter(~F.col("_duration_valid")).count()


def freshness_summary(df: DataFrame) -> dict:
    row = df.agg(
        F.max("_ingested_at").alias("latest_ingested_at"),
        F.count("*").alias("total_rows"),
    ).first()
    return {"latest_ingested_at": row["latest_ingested_at"], "total_rows": row["total_rows"]}


def run_quality_checks(df: DataFrame, key_col: str = "complaint_id") -> dict:
    fresh = freshness_summary(df)
    return {
        "duplicate_count": count_duplicates(df, key_col),
        "bounds_violation_count": count_bounds_violations(df),
        "latest_ingested_at": fresh["latest_ingested_at"],
        "total_rows": fresh["total_rows"],
    }
