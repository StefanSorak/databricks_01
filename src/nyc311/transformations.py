from pyspark.sql import DataFrame, functions as F

KEEP_COLUMNS = {
    # source_name -> silver_name
    "unique_key": "complaint_id",
    "created_date": "created_at",
    "closed_date": "closed_at",
    "resolution_action_updated_date": "last_updated_at",
    "agency": "agency",
    "agency_name": "agency_name",
    "complaint_type": "complaint_type",
    "descriptor": "descriptor",
    "borough": "borough",
    "status": "status",
    "open_data_channel_type": "intake_channel",
    "incident_address": "incident_address",
    "city": "city",
    "_ingested_at": "_ingested_at",
    "_source_file": "_source_file",
}


def select_and_rename(df: DataFrame) -> DataFrame:
    return df.select(
        [F.col(src).alias(dst) for src, dst in KEEP_COLUMNS.items()]
    )


def normalize_strings(df: DataFrame) -> DataFrame:
    null_values = [" ", "  ", "N/A", "Unspecified", "Unknown"]

    df = df.select(
        [F.lower(F.trim(F.col(c))).alias(c) for c in df.columns])
    
    for c in df.columns:
    
    df = df.withColumn("borough", F.initcap("borough"))
    df = df.withColumn("city", F.initcap("city"))
    df = df.withColumn("intake_channel", F.when(F.col("intake_channel") == "OTHER", "Unknown")
                                   .otherwise(F.initcap("intake_channel")))
    return df


def cast_types(df: DataFrame) -> DataFrame:
    df = df.withColumn("complaint_id", F.col("complaint_id").cast("int"))
    for column in ["created_at", "closed_at", "last_updated_at"]:
        df = df.withColumn(column, F.col(column).cast("timestamp"))
    return df


def build_silver(df: DataFrame) -> DataFrame:
    return (df
            .transform(select_and_rename)
            .transform(normalize_strings)
            .transform(cast_types)
    )