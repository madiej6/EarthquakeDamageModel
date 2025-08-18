import duckdb
import os
import logging
import geopandas as gpd
from constants import (
    EVENT_INFO_TABLE,
    DAMAGE_FUNCTION_VARS_TABLE,
    BLDG_PCT_BY_TRACT_TABLE,
    ALL_SCHEMAS_PATH,
)
from configs.schemas import SchemaConfig, TableSchema


logging.basicConfig(level=logging.INFO)

DB_PATH = os.path.join(os.getcwd(), "data", "eq_damage_model.db")
DAMAGE_FUNCTION_VARS_PATH = os.path.join(
    os.getcwd(), "tables", "DamageFunctionVariables.csv"
)
BLDG_PCTS_PER_TRACT_PATH = os.path.join(
    os.getcwd(), "tables", "Building_Percentages_Per_Tract_ALLSTATES.csv"
)


def table_cleanup(conn: duckdb.DuckDBPyConnection):
    execute(conn, "DROP TABLE IF EXISTS shakemaps;")
    execute(conn, "DROP TABLE IF EXISTS epicenters;")


def insert_gdf_into_table(
    conn: duckdb.DuckDBPyConnection,
    gdf: gpd.GeoDataFrame,
    table_name: str,
    schema: TableSchema,
):
    """Inserts a geodataframe into existing DuckDB table.

    Assumes the schema of the geodataframe is the same as the DuckDB table's schema."""
    # convert geometry to WKT
    gdf["geometry"] = gdf["geometry"].apply(lambda geom: geom.wkt)

    # register the gdf as a DuckDB table
    conn.register("new_gdf", gdf)

    list_cols = ", ".join(schema.schema.keys())
    query = f"""INSERT INTO {table_name} ({list_cols})
                SELECT {list_cols} FROM new_gdf;"""

    # persist it into DuckDB
    try:
        execute(
            conn,
            query,
        )
    except duckdb.ConstraintException as e:
        logging.info(query)
        logging.info(f"Skipping insert: {e.args[0]}")
    conn.unregister("new_gdf")


def execute(conn: duckdb.DuckDBPyConnection, query: str):
    logging.info("-----------query-start-----------")
    logging.info(query)
    logging.info("------------query-end------------")
    result = conn.execute(query)
    return result


def spatial_extension(conn: duckdb.DuckDBPyConnection):
    """Install spatial extension if needed.

    Args:
        conn (duckdb.DuckDBPyConnection): Connection to the database.

    Returns:
        conn (duckdb.DuckDBPyConnection): Connection to the database.
    """

    # Check if the spatial extension is installed
    try:
        execute(conn, "LOAD SPATIAL;")
        print("Spatial extension is installed.")
    except duckdb.IOException:
        print("Spatial extension is NOT installed.")
        execute(conn, "INSTSLL SPATIAL;")
        execute(conn, "LOAD SPATIAL;")

    return conn


def initialize(rebuild_tables: bool = False):
    conn = duckdb.connect(DB_PATH)
    spatial_extension(conn)
    table_cleanup(conn)
    create_tables(conn, rebuild_tables)
    return conn


def create_tables(conn: duckdb.DuckDBPyConnection, replace: bool = False):
    if replace:
        create_statement = "CREATE OR REPLACE TABLE"
    else:
        create_statement = "CREATE TABLE IF NOT EXISTS"

    # Create tables from input csv files
    execute(
        conn,
        f"{create_statement} {DAMAGE_FUNCTION_VARS_TABLE} AS SELECT * FROM read_csv('{DAMAGE_FUNCTION_VARS_PATH}')",
    )
    execute(
        conn,
        f"{create_statement} {BLDG_PCT_BY_TRACT_TABLE} AS SELECT * FROM read_csv('{BLDG_PCTS_PER_TRACT_PATH}')",
    )

    # Read all schemas from the all_schemas.yaml file
    schemas = SchemaConfig.from_yaml(ALL_SCHEMAS_PATH).schemas

    # Create all tables in all_schemas.yaml
    for table_name in [
        "epicenters",
        "shakemaps",
        "census_geo_exposure",
        EVENT_INFO_TABLE,
    ]:
        execute(
            conn,
            f"{create_statement} {table_name} ({schemas[table_name].duckdb_schema}, PRIMARY KEY ({schemas[table_name].duckdb_pk}));",
        )
