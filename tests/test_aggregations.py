from datetime import datetime, date

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType, DoubleType, BooleanType, LongType,
)

from nyc311.aggregations import (
    gold_agency_performance,
    gold_borough_metrics,
    gold_channel_trends,
    gold_weather_date,
)

SILVER_SCHEMA = StructType(
    [
        StructField("complaint_id", LongType()),
        StructField("created_at", TimestampType()),
        StructField("closed_at", TimestampType()),
        StructField("agency", StringType()),
        StructField("complaint_type", StringType()),
        StructField("borough", StringType()),
        StructField("intake_channel", StringType()),
        StructField("_duration_valid", BooleanType()),
        StructField("response_time_hours", DoubleType()),
    ]
)


def silver_row(**overrides):
    row = {
        "complaint_id": 1,
        "created_at": datetime(2025, 10, 1, 0, 0, 0),
        "closed_at": datetime(2025, 10, 1, 2, 0, 0),
        "agency": "HPD",
        "complaint_type": "HEAT/HOT WATER",
        "borough": "Brooklyn",
        "intake_channel": "Phone",
        "_duration_valid": True,
        "response_time_hours": 2.0,
    }
    row.update(overrides)
    return tuple(row[f.name] for f in SILVER_SCHEMA.fields)


def test_gold_agency_performance_broadcasts_open_stats_across_closed_months(spark):
    df = spark.createDataFrame(
        [
            silver_row(complaint_id=1, agency="HPD", complaint_type="HEAT/HOT WATER",
                       created_at=datetime(2025, 10, 1), closed_at=datetime(2025, 10, 1, 2, 0, 0),
                       response_time_hours=2.0),
            silver_row(complaint_id=2, agency="HPD", complaint_type="HEAT/HOT WATER",
                       created_at=datetime(2025, 10, 1), closed_at=None, response_time_hours=None,
                       _duration_valid=True),
        ],
        SILVER_SCHEMA,
    )
    result = gold_agency_performance(df).collect()

    assert len(result) == 1  # open snapshot merges into the one closed-month row, not a second row
    row = result[0]
    assert row["closed_count"] == 1
    assert row["open_count"] == 1
    assert float(row["median_response_time_hours"]) == 2.0


def test_gold_agency_performance_zero_fills_counts_not_medians(spark):
    # only closed data exists for this agency/type - open_count must be 0, not null
    df = spark.createDataFrame([silver_row()], SILVER_SCHEMA)
    row = gold_agency_performance(df).first()

    assert row["open_count"] == 0
    assert row["median_open_age_hours"] is None  # null, not zero - "no data" != "zero hours"


def test_gold_agency_performance_open_only_agency_has_null_month(spark):
    # an agency/type with no closed complaints ever should have month = null, not join-dropped
    df = spark.createDataFrame(
        [silver_row(closed_at=None, response_time_hours=None)], SILVER_SCHEMA,
    )
    row = gold_agency_performance(df).first()

    assert row["month"] is None
    assert row["closed_count"] == 0
    assert row["median_response_time_hours"] is None
    assert row["open_count"] == 1


def test_gold_borough_metrics_keeps_null_borough(spark):
    df = spark.createDataFrame(
        [silver_row(complaint_id=1, borough=None), silver_row(complaint_id=2, borough="Brooklyn")],
        SILVER_SCHEMA,
    )
    result = gold_borough_metrics(df)
    boroughs = {r["borough"] for r in result.collect()}

    assert None in boroughs  # BR-2: missing borough counted, never dropped


def test_gold_borough_metrics_counts_closed_and_open(spark):
    df = spark.createDataFrame(
        [
            silver_row(complaint_id=1, closed_at=datetime(2025, 10, 1, 2, 0, 0)),
            silver_row(complaint_id=2, closed_at=None, response_time_hours=None),
        ],
        SILVER_SCHEMA,
    )
    row = gold_borough_metrics(df).first()

    assert row["closed_count"] == 1
    assert row["open_count"] == 1


def test_gold_channel_trends_groups_by_channel_and_creation_month(spark):
    df = spark.createDataFrame(
        [
            silver_row(complaint_id=1, intake_channel="Phone", created_at=datetime(2025, 10, 15)),
            silver_row(complaint_id=2, intake_channel="Phone", created_at=datetime(2025, 10, 20)),
            silver_row(complaint_id=3, intake_channel=None, created_at=datetime(2025, 10, 20)),
        ],
        SILVER_SCHEMA,
    )
    result = gold_channel_trends(df).collect()
    by_channel = {r["intake_channel"]: r["complaint_count"] for r in result}

    assert by_channel["Phone"] == 2
    assert by_channel[None] == 1  # null channel kept as its own group


WEATHER_SCHEMA = StructType(
    [
        StructField("date", DateType()),
        StructField("temperature_2m_max", DoubleType()),
        StructField("temperature_2m_min", DoubleType()),
        StructField("temperature_2m_mean", DoubleType()),
    ]
)


def test_gold_weather_date_keeps_zero_complaint_days(spark):
    weather_df = spark.createDataFrame(
        [
            (date(2025, 10, 1), 20.0, 10.0, 15.0),
            (date(2025, 10, 2), 5.0, -2.0, 1.5),
        ],
        WEATHER_SCHEMA,
    )
    silver_df = spark.createDataFrame(
        [silver_row(complaint_id=1, complaint_type="HEAT/HOT WATER", created_at=datetime(2025, 10, 2))],
        SILVER_SCHEMA,
    )
    result = gold_weather_date(silver_df, weather_df).orderBy("date").collect()

    assert result[0]["complaint_count"] == 0  # mild day, no complaints - kept, not dropped
    assert result[1]["complaint_count"] == 1


def test_gold_weather_date_filters_to_heat_hot_water_only(spark):
    weather_df = spark.createDataFrame([(date(2025, 10, 1), 20.0, 10.0, 15.0)], WEATHER_SCHEMA)
    silver_df = spark.createDataFrame(
        [silver_row(complaint_id=1, complaint_type="NOISE", created_at=datetime(2025, 10, 1))],
        SILVER_SCHEMA,
    )
    result = gold_weather_date(silver_df, weather_df).first()

    assert result["complaint_count"] == 0  # non-HEAT/HOT WATER complaint must not be counted
