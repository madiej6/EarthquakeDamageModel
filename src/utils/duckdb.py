import duckdb
import os
import logging
from constants import (
    EVENT_INFO_TABLE,
    DAMAGE_FUNCTION_VARS_TABLE,
    BLDG_PCT_BY_TRACT_TABLE,
)
from schemas.shakemaps import schema, primary_key


logging.basicConfig(level=logging.INFO)

DB_PATH = os.path.join(os.getcwd(), "data", "eq_damage_model.db")
DAMAGE_FUNCTION_VARS_PATH = os.path.join(
    os.getcwd(), "tables", "DamageFunctionVariables.csv"
)
BLDG_PCTS_PER_TRACT_PATH = os.path.join(
    os.getcwd(), "tables", "Building_Percentages_Per_Tract_ALLSTATES.csv"
)


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
    shakemap_schema = [f"{col} {type}" for col, type in schema.ites()]
    execute(
        conn,
        f"{create_statement} shakemaps ({', '.join(shakemap_schema)}, PRIMARY KEY ({primary_key}));",
    )
