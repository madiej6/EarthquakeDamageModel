import os

REPO_PATH = os.getcwd()

ALL_SCHEMAS_PATH = os.path.join(REPO_PATH, "src/schemas/all_schemas.yaml")


USA_STRUCTURES_URL = "https://disasters.geoplatform.gov/USA_Structures/"
CENSUS_URL = "https://www2.census.gov/geo/tiger/TIGER2024/TRACT/"

FEEDURL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson"  # Significant Events - 1 week
# FEEDURL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson' #1 hour M4.5+
# FEEDURL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson' #1 day M4.5+
# FEEDURL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.geojson"

# File path to building centroids GDB
BuildingCentroids = "data/ORNL_USAStructures_Centroids_LightboxSpatialJoin.gdb"

# For testing mode, update the file paths for the Napa and Idaho directories
NapaEventDir = "data/testing/napa2014shakemap"
IdahoEventDir = "data/testing/idaho2017shakemap"

# duckdb table names
DAMAGE_FUNCTION_VARS_TABLE = "damage_function_vars"
BLDG_PCT_BY_TRACT_TABLE = "bldg_type_mappings"
EVENT_INFO_TABLE = "event_log"
