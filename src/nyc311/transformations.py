from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import DecimalType

MAX_RESOLUTION_YEARS = 5

KEEP_COLUMNS = {
    # source_name -> silver_name
    "unique_key": "complaint_id",
    "created_date": "created_at",
    "closed_date": "closed_at",
    "resolution_action_updated_date": "last_updated_at",
    "agency": "agency",
    "agency_name": "agency_name",
    "complaint_type": "complaint_type",
    "descriptor": "description",
    "borough": "borough",
    "status": "status",
    "open_data_channel_type": "intake_channel",
    "incident_address": "incident_address",
    "city": "city",
    "_ingested_at": "_ingested_at",
    "_source_file": "_source_file",
}


def select_and_rename(df: DataFrame) -> DataFrame:
    return df.select([F.col(src).alias(dst) for src, dst in KEEP_COLUMNS.items()])


def normalize_strings(df: DataFrame) -> DataFrame:
    sentinel_values = ["", "n/a", "unspecified", "unknown"]
    timestamp_cols = ["created_at", "closed_at", "last_updated_at", "_ingested_at"]

    for c in df.columns:
        if c in timestamp_cols:
            continue
        cleaned = F.regexp_replace(F.trim(F.col(c)), r"\s+", " ")
        is_sentinel = F.lower(cleaned).isin(sentinel_values)

        df = df.withColumn(c, F.when(is_sentinel, None).otherwise(cleaned))

    df = df.withColumn("borough", F.initcap("borough"))
    df = df.withColumn("city", F.initcap("city"))
    df = df.withColumn("intake_channel", F.initcap("intake_channel"))
    df = df.withColumn("agency", F.upper("agency"))
    return df


def cast_types(df: DataFrame) -> DataFrame:
    df = df.withColumn("complaint_id", F.col("complaint_id").cast("long"))
    for column in ["created_at", "closed_at", "last_updated_at"]:
        df = df.withColumn(column, F.try_to_timestamp(F.col(column)))
    return df


def normalize_status(df: DataFrame) -> DataFrame:
    # closed_at wins over the raw status - sampling found "Assigned" rows that were actually closed.
    return df.withColumn(
        "status",
        F.when(F.col("closed_at").isNotNull(), F.lit("Closed"))
        .when(F.col("status").isNotNull(), F.col("status"))
        .otherwise(F.lit("Open")),
    )


def deduplicate(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("complaint_id").orderBy(
        F.col("last_updated_at").desc_nulls_last(), F.col("_ingested_at").desc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def validate_bounds(df: DataFrame) -> DataFrame:
    max_seconds = MAX_RESOLUTION_YEARS * 365.25 * 24 * 3600
    duration_seconds = F.unix_timestamp("closed_at") - F.unix_timestamp("created_at")

    return df.withColumn(
        "_duration_valid",
        F.when(
            F.col("closed_at").isNull(), F.lit(True)
        )  # still open, nothing to validate yet
        .when((duration_seconds < 0) | (duration_seconds > max_seconds), F.lit(False))
        .otherwise(F.lit(True)),
    )


def derive_metrics(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "response_time_hours",
        F.when(
            F.col("_duration_valid"),
            F.round(
                (F.unix_timestamp("closed_at") - F.unix_timestamp("created_at")) / 3600,
                2,
            ),
        ).cast(DecimalType(10, 2)),
    )


def prepare(df: DataFrame) -> DataFrame:
    return (
        df.transform(select_and_rename)
        .transform(normalize_strings)
        .transform(cast_types)
        .transform(normalize_status)
    )


def finalize(df: DataFrame) -> DataFrame:
    # validate_bounds/derive_metrics only add columns, so a count after this is a dedup count too.
    return df.transform(deduplicate).transform(validate_bounds).transform(derive_metrics)


def build_silver(df: DataFrame) -> DataFrame:
    return finalize(prepare(df))
