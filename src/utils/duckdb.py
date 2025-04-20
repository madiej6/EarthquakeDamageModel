import duckdb
import os
import logging
from typing import List, Dict
import geopandas as gpd
from constants import (
    EVENT_INFO_TABLE,
    DAMAGE_FUNCTION_VARS_TABLE,
    BLDG_PCT_BY_TRACT_TABLE,
)
from schemas.epicenters import (
    schema as epicenters_schema,
    primary_key as epicenters_primary_key,
)
from schemas.shakemaps import (
    schema as shakemaps_schema,
    primary_key as shakemaps_primary_key,
)


logging.basicConfig(level=logging.INFO)

DB_PATH = os.path.join(os.getcwd(), "data", "eq_damage_model.db")
DAMAGE_FUNCTION_VARS_PATH = os.path.join(
    os.getcwd(), "tables", "DamageFunctionVariables.csv"
)
BLDG_PCTS_PER_TRACT_PATH = os.path.join(
    os.getcwd(), "tables", "Building_Percentages_Per_Tract_ALLSTATES.csv"
)


def table_cleanup(conn: duckdb.DuckDBPyConnection):
    execute(conn, "DROP TABLE shakemaps;")
    execute(conn, "DROP TABLE epicenters;")


def insert_gdf_into_table(
    conn: duckdb.DuckDBPyConnection,
    gdf: gpd.GeoDataFrame,
    table_name: str,
    schema: Dict,
    primary_keys=List[str],
):
    """Insertd a geodataframe into existing DuckDB table.

    Assumes the schema of the geodataframe is the same as the DuckDB table's schema."""
    # convert geometry to WKT
    gdf["geometry"] = gdf["geometry"].apply(lambda geom: geom.wkt)

    # register the gdf as a DuckDB table
    conn.register("new_gdf", gdf)

    columns = schema.keys()

    # # create the primary key conditions. this ensure that an error
    # # is not thrown if this data already exists in the duckdb table
    # terms_list = []
    # for key in primary_keys:
    #     terms = f"{table_name}.{key} = new_gdf.{key}"
    #     terms_list.append(terms)
    # condition = f"WHERE {' AND '.join(terms_list)}"

    # persist it into DuckDB
    execute(
        conn,
        f"""INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM new_gdf;""",
    )
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
        conn.execute("LOAD SPATIAL;")
        print("Spatial extension is installed.")
    except duckdb.IOException:
        print("Spatial extension is NOT installed.")
        conn.install_extension("spatial")

    conn.load_extension("spatial")

    return conn


def initialize(rebuild_tables: bool = False):
    conn = duckdb.connect(DB_PATH)
    table_cleanup(conn)
    create_tables(conn, rebuild_tables)
    return conn


def create_tables(conn: duckdb.DuckDBPyConnection, replace: bool = False):
    if replace:
        create_statement = "CREATE OR REPLACE TABLE"
    else:
        create_statement = "CREATE TABLE IF NOT EXISTS"

    execute(
        conn,
        f"{create_statement} {DAMAGE_FUNCTION_VARS_TABLE} AS SELECT * FROM read_csv('{DAMAGE_FUNCTION_VARS_PATH}')",
    )
    execute(
        conn,
        f"{create_statement} {BLDG_PCT_BY_TRACT_TABLE} AS SELECT * FROM read_csv('{BLDG_PCTS_PER_TRACT_PATH}')",
    )
    execute(
        conn,
        f"{create_statement} {EVENT_INFO_TABLE} (event_id VARCHAR PRIMARY KEY, status STRING, timestamp TIMESTAMP);",
    )
    epicenters_schema_duckdb = [
        f"{col} {type}" for col, type in epicenters_schema.items()
    ]
    execute(
        conn,
        f"{create_statement} epicenters ({', '.join(epicenters_schema_duckdb)}, PRIMARY KEY ({', '.join(epicenters_primary_key)}));",
    )
    shakemap_schema_duckdb = [f"{col} {type}" for col, type in shakemaps_schema.items()]
    execute(
        conn,
        f"{create_statement} shakemaps ({', '.join(shakemap_schema_duckdb)}, PRIMARY KEY ({', '.join(shakemaps_primary_key)}));",
    )
