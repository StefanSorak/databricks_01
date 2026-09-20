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
    # One combined aggregation instead of calling the three functions above separately -
    # serverless compute doesn't support caching, so this avoids 4 full recomputes of
    # build_silver() for what is otherwise one pass over the data.
    row = df.agg(
        F.count("*").alias("total_rows"),
        F.countDistinct(F.col(key_col)).alias("distinct_keys"),
        F.count(F.when(~F.col("_duration_valid"), 1)).alias("bounds_violation_count"),
        F.max("_ingested_at").alias("latest_ingested_at"),
    ).first()
    return {
        "duplicate_count": row["total_rows"] - row["distinct_keys"],
        "bounds_violation_count": row["bounds_violation_count"],
        "latest_ingested_at": row["latest_ingested_at"],
        "total_rows": row["total_rows"],
    }
