from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from nyc311.transformations import (
    select_and_rename,
    normalize_strings,
    cast_types,
    normalize_status,
    deduplicate,
    validate_bounds,
    derive_metrics,
    build_silver,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("unique_key", StringType()),
        StructField("created_date", StringType()),
        StructField("closed_date", StringType()),
        StructField("resolution_action_updated_date", StringType()),
        StructField("agency", StringType()),
        StructField("agency_name", StringType()),
        StructField("complaint_type", StringType()),
        StructField("descriptor", StringType()),
        StructField("borough", StringType()),
        StructField("status", StringType()),
        StructField("open_data_channel_type", StringType()),
        StructField("incident_address", StringType()),
        StructField("city", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("street_name", StringType()),  # not in KEEP_COLUMNS - should be dropped
    ]
)


def bronze_row(**overrides):
    row = {
        "unique_key": "1",
        "created_date": "2025-10-01T00:00:00.000",
        "closed_date": None,
        "resolution_action_updated_date": None,
        "agency": "hpd",
        "agency_name": "Housing Preservation",
        "complaint_type": "HEAT/HOT WATER",
        "descriptor": "  ENTIRE  BUILDING  ",
        "borough": "BROOKLYN",
        "status": None,
        "open_data_channel_type": "phone",
        "incident_address": "1 Main St",
        "city": "Unspecified",
        "_ingested_at": datetime(2026, 1, 1, 0, 0, 0),
        "_source_file": "run_1_page_0000.json.gz",
        "street_name": "Main St",
    }
    row.update(overrides)
    return tuple(row[f.name] for f in BRONZE_SCHEMA.fields)


def test_select_and_rename_maps_and_drops_columns(spark):
    df = spark.createDataFrame([bronze_row()], BRONZE_SCHEMA)
    result = select_and_rename(df)

    assert set(result.columns) == {
        "complaint_id", "created_at", "closed_at", "last_updated_at", "agency",
        "agency_name", "complaint_type", "description", "borough", "status",
        "intake_channel", "incident_address", "city", "_ingested_at", "_source_file",
    }
    row = result.first()
    assert row["complaint_id"] == "1"
    assert row["intake_channel"] == "phone"


def test_normalize_strings_nullifies_sentinels_case_insensitively(spark):
    df = spark.createDataFrame([bronze_row(city="unspecified", status="N/A")], BRONZE_SCHEMA)
    result = normalize_strings(select_and_rename(df))
    row = result.first()

    assert row["city"] is None
    assert row["status"] is None


def test_normalize_strings_preserves_case_while_collapsing_whitespace(spark):
    df = spark.createDataFrame([bronze_row(descriptor="  ENTIRE  BUILDING  ")], BRONZE_SCHEMA)
    result = normalize_strings(select_and_rename(df))
    row = result.first()

    # regression guard: reusing one lowercased expression for both the stored value and the
    # sentinel check would silently lowercase every value
    assert row["description"] == "ENTIRE BUILDING"


def test_normalize_strings_nullifies_all_whitespace_string(spark):
    df = spark.createDataFrame([bronze_row(city="   ")], BRONZE_SCHEMA)
    result = normalize_strings(select_and_rename(df))
    assert result.first()["city"] is None


def test_normalize_strings_applies_casing_rules(spark):
    df = spark.createDataFrame(
        [bronze_row(agency="hpd", borough="BROOKLYN", city="new york", open_data_channel_type="online")],
        BRONZE_SCHEMA,
    )
    result = normalize_strings(select_and_rename(df))
    row = result.first()

    assert row["agency"] == "HPD"
    assert row["borough"] == "Brooklyn"
    assert row["city"] == "New York"
    assert row["intake_channel"] == "Online"


def test_normalize_strings_skips_timestamp_columns(spark):
    ingested = datetime(2026, 1, 1, 12, 0, 0)
    df = spark.createDataFrame([bronze_row(created_date="not-a-date-yet", _ingested_at=ingested)], BRONZE_SCHEMA)
    result = normalize_strings(select_and_rename(df))
    row = result.first()

    # created_at is still a raw string at this stage (cast_types runs after); must be untouched
    assert row["created_at"] == "not-a-date-yet"
    assert row["_ingested_at"] == ingested


def test_cast_types_parses_valid_and_nulls_invalid_timestamps(spark):
    df = spark.createDataFrame(
        [
            bronze_row(unique_key="1", created_date="2025-10-01T00:00:00.000"),
            bronze_row(unique_key="2", created_date="garbage"),
        ],
        BRONZE_SCHEMA,
    )
    result = cast_types(select_and_rename(df)).orderBy("complaint_id")
    rows = result.collect()

    assert rows[0]["complaint_id"] == 1  # cast to long
    assert rows[0]["created_at"] == datetime(2025, 10, 1, 0, 0, 0)
    assert rows[1]["created_at"] is None  # try_to_timestamp fails safe, not an error


SILVER_COLUMNS = [
    "complaint_id", "created_at", "closed_at", "last_updated_at", "agency", "agency_name",
    "complaint_type", "description", "borough", "status", "intake_channel",
    "incident_address", "city", "_ingested_at", "_source_file",
]
SILVER_SCHEMA = StructType(
    [StructField("complaint_id", StringType())]
    + [StructField(c, TimestampType()) for c in ["created_at", "closed_at", "last_updated_at"]]
    + [StructField(c, StringType()) for c in SILVER_COLUMNS if c not in
       {"complaint_id", "created_at", "closed_at", "last_updated_at", "_ingested_at"}]
    + [StructField("_ingested_at", TimestampType())]
)


def silver_row(**overrides):
    row = {
        "complaint_id": "1",
        "created_at": datetime(2025, 10, 1, 0, 0, 0),
        "closed_at": None,
        "last_updated_at": None,
        "agency": "HPD",
        "agency_name": "Housing Preservation",
        "complaint_type": "HEAT/HOT WATER",
        "description": "ENTIRE BUILDING",
        "borough": "Brooklyn",
        "status": None,
        "intake_channel": "Phone",
        "incident_address": "1 Main St",
        "city": "New York",
        "_ingested_at": datetime(2026, 1, 1, 0, 0, 0),
        "_source_file": "run_1_page_0000.json.gz",
    }
    row.update(overrides)
    return tuple(row[f.name] for f in SILVER_SCHEMA.fields)


def test_normalize_status_fills_only_when_null(spark):
    df = spark.createDataFrame(
        [
            silver_row(complaint_id="1", status=None, closed_at=datetime(2025, 10, 2)),
            silver_row(complaint_id="2", status=None, closed_at=None),
            silver_row(complaint_id="3", status="Assigned", closed_at=datetime(2025, 10, 2)),
        ],
        SILVER_SCHEMA,
    )
    result = normalize_status(df).orderBy("complaint_id")
    statuses = [r["status"] for r in result.collect()]

    assert statuses == ["Closed", "Open", "Assigned"]  # existing non-null status is left alone


def test_deduplicate_keeps_latest_last_updated_at(spark):
    df = spark.createDataFrame(
        [
            silver_row(complaint_id="1", last_updated_at=datetime(2025, 10, 1), description="old"),
            silver_row(complaint_id="1", last_updated_at=datetime(2025, 10, 5), description="new"),
        ],
        SILVER_SCHEMA,
    )
    result = deduplicate(df)

    assert result.count() == 1
    assert result.first()["description"] == "new"


def test_deduplicate_breaks_tie_with_ingested_at(spark):
    df = spark.createDataFrame(
        [
            silver_row(complaint_id="1", last_updated_at=None, _ingested_at=datetime(2026, 1, 1), description="first"),
            silver_row(complaint_id="1", last_updated_at=None, _ingested_at=datetime(2026, 1, 2), description="second"),
        ],
        SILVER_SCHEMA,
    )
    result = deduplicate(df)

    assert result.count() == 1
    assert result.first()["description"] == "second"


def test_deduplicate_does_not_collapse_different_ids(spark):
    df = spark.createDataFrame(
        [silver_row(complaint_id="1"), silver_row(complaint_id="2")],
        SILVER_SCHEMA,
    )
    assert deduplicate(df).count() == 2


def test_validate_bounds_still_open_is_valid(spark):
    df = spark.createDataFrame([silver_row(closed_at=None)], SILVER_SCHEMA)
    assert validate_bounds(df).first()["_duration_valid"] is True


def test_validate_bounds_flags_negative_duration(spark):
    df = spark.createDataFrame(
        [silver_row(created_at=datetime(2025, 10, 5), closed_at=datetime(2025, 10, 1))],
        SILVER_SCHEMA,
    )
    assert validate_bounds(df).first()["_duration_valid"] is False


def test_validate_bounds_flags_implausibly_long_duration(spark):
    df = spark.createDataFrame(
        [silver_row(created_at=datetime(2015, 1, 1), closed_at=datetime(2025, 1, 1))],
        SILVER_SCHEMA,
    )
    assert validate_bounds(df).first()["_duration_valid"] is False


def test_validate_bounds_accepts_plausible_duration(spark):
    df = spark.createDataFrame(
        [silver_row(created_at=datetime(2025, 10, 1, 0, 0, 0), closed_at=datetime(2025, 10, 1, 2, 0, 0))],
        SILVER_SCHEMA,
    )
    assert validate_bounds(df).first()["_duration_valid"] is True


def test_derive_metrics_computes_hours_when_valid(spark):
    df = spark.createDataFrame(
        [silver_row(created_at=datetime(2025, 10, 1, 0, 0, 0), closed_at=datetime(2025, 10, 1, 2, 0, 0))],
        SILVER_SCHEMA,
    )
    df = validate_bounds(df)
    result = derive_metrics(df).first()

    assert float(result["response_time_hours"]) == 2.0


def test_derive_metrics_nulls_when_invalid(spark):
    df = spark.createDataFrame(
        [silver_row(created_at=datetime(2025, 10, 5), closed_at=datetime(2025, 10, 1))],
        SILVER_SCHEMA,
    )
    df = validate_bounds(df)
    assert derive_metrics(df).first()["response_time_hours"] is None


def test_build_silver_end_to_end_dedupes_and_derives(spark):
    df = spark.createDataFrame(
        [
            bronze_row(unique_key="1", created_date="2025-10-01T00:00:00.000",
                       closed_date="2025-10-01T02:00:00.000", resolution_action_updated_date=None),
            bronze_row(unique_key="1", created_date="2025-10-01T00:00:00.000",
                       closed_date=None, resolution_action_updated_date=None,
                       _ingested_at=datetime(2025, 12, 1)),  # earlier duplicate, should lose
        ],
        BRONZE_SCHEMA,
    )
    result = build_silver(df)

    assert result.count() == 1
    row = result.first()
    assert row["complaint_id"] == 1
    assert row["closed_at"] is not None
    assert float(row["response_time_hours"]) == 2.0
