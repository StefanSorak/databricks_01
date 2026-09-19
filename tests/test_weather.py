from nyc311.weather import rows_from_daily


def test_rows_from_daily_transposes_columns_into_rows():
    daily = {
        "time": ["2025-10-01", "2025-10-02"],
        "temperature_2m_max": [20.3, 19.1],
        "temperature_2m_min": [12.1, 9.6],
    }
    result = rows_from_daily(daily)

    assert result == [
        {"date": "2025-10-01", "temperature_2m_max": 20.3, "temperature_2m_min": 12.1},
        {"date": "2025-10-02", "temperature_2m_max": 19.1, "temperature_2m_min": 9.6},
    ]


def test_rows_from_daily_handles_empty_range():
    assert rows_from_daily({"time": [], "temperature_2m_max": []}) == []
