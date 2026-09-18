from pyspark.sql import DataFrame, functions as F, Window


def gold_agency_performance(df: DataFrame) -> DataFrame:
    df = df.withColumn("month", F.date_trunc("month", "closed_at"))

    df_closed = df.filter(F.col("closed_at").isNotNull())
    df_open = df.filter(F.col("closed_at").isNull())

    df_open = df_open.withColumn("open_age_hours", F.timestamp_diff("hour", F.col("created_at"), F.current_timestamp()))
    
    df_closed_agg = df_closed.groupBy("agency", "complaint_type", "month").agg(
        F.count("*").alias("closed_count"),
        F.round(F.median(F.col("response_time_hours")), 2).alias("median_response_time_hours"),
        F.round(F.percentile_approx(F.col("response_time_hours"), 0.9), 2).alias("p90_response_hours"),
    )
    
    df_open_agg = df_open.groupBy("agency", "complaint_type").agg(
        F.count("*").alias("open_count"),
        F.round(F.median(F.col("open_age_hours")), 2).alias("median_open_age_hours"),
    )

    df = df_closed_agg.join(df_open_agg, on=["agency", "complaint_type"], how="full")

    df = df.na.fill(0, subset=["closed_count", "open_count"])
    
    return df


def gold_borough_metrics(df: DataFrame) -> DataFrame:
    df = df.withColumn("open_age_hours", F.when(F.col("closed_at").isNull(), F.timestamp_diff("hour", F.col("created_at"), F.current_timestamp())).otherwise(None))

    df = df.groupBy("borough", "complaint_type").agg(
        F.count_if(F.col("closed_at").isNotNull()).alias("closed_count"),
        F.round(F.median(F.col("response_time_hours")), 2).alias("median_response_time_hours"),
        F.round(F.percentile_approx(F.col("response_time_hours"), 0.9), 2).alias("p90_response_hours"),
        F.count_if(F.col("closed_at").isNull()).alias("open_count"),
        F.round(F.median(F.col("open_age_hours")), 2).alias("median_open_age_hours"),
    )

    return df


def gold_channel_trends(df: DataFrame) -> DataFrame:
    df = df.withColumn("month", F.date_trunc("month", "created_at"))

    return df.groupBy("intake_channel", "month").agg(F.count("*").alias("complaint_count"))
    

def gold_weather_date(df_silver: DataFrame, df_weather: DataFrame) -> DataFrame:
    df_silver = df_silver.filter(F.col("complaint_type") == "HEAT/HOT WATER")
    df_silver = df_silver.withColumn("date", F.to_date( "created_at"))
    df_silver = df_silver.groupBy("date").agg(F.count("*").alias("complaint_count"))

    df = (df_weather.join(df_silver, on="date", how="left")
        .withColumn("temperature_2m_mean", F.round(F.col("temperature_2m_mean"), 2))
        .withColumn("temperature_2m_max", F.round(F.col("temperature_2m_max"), 2))
        .withColumn("temperature_2m_min", F.round(F.col("temperature_2m_min"), 2))
        .na.fill(0, subset=["complaint_count"]))

    return df