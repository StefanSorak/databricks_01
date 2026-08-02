from pyspark.sql.types import StructType, StringType

BRONZE_COLUMNS = [
    "address_type",
    "agency",
    "agency_name",
    "bbl",
    "borough",
    "bridge_highway_direction",
    "bridge_highway_name",
    "bridge_highway_segment",
    "city",
    "closed_date",
    "community_board",
    "complaint_type",
    "council_district",
    "created_date",
    "cross_street_1",
    "cross_street_2",
    "descriptor",
    "descriptor_2",
    "due_date",
    "facility_type",
    "incident_address",
    "incident_zip",
    "intersection_street_1",
    "intersection_street_2",
    "landmark",
    "latitude",
    "location",
    "location_type",
    "longitude",
    "open_data_channel_type",
    "park_borough",
    "park_facility_name",
    "police_precinct",
    "resolution_action_updated_date",
    "resolution_description",
    "road_ramp",
    "status",
    "street_name",
    "taxi_company_borough",
    "taxi_pick_up_location",
    "unique_key",
    "vehicle_type",
    "x_coordinate_state_plane",
    "y_coordinate_state_plane",
]


def bronze_schema() -> StructType:
    schema = StructType()
    for col in BRONZE_COLUMNS:
        schema = schema.add(col, StringType())
    return schema