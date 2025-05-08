import os
import requests
import argparse
from bs4 import BeautifulSoup
import zipfile
import tempfile
import time
import duckdb
from typing import List
from utils.duckdb import initialize, execute, insert_gdf_into_table
from constants import ALL_SCHEMAS_PATH, USA_STRUCTURES_URL
from configs.schemas import SchemaConfig
import geopandas as gpd
import logging

logging.basicConfig(level=logging.INFO)


# Scrape the FTP-style directory page for links
def get_zip_links(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """Gets all zip download links at a provided URL.

    Args:
        conn (duckdb.DuckDBPyConnection): duckdb connection

    Returns:
        List[str]: list of zip download links
    """

    done_urls = execute(conn, "select distinct URL from usa_structures;").fetchall()
    if len(done_urls) > 0:
        done_urls = [i[0] for i in done_urls]

    response = requests.get(USA_STRUCTURES_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    all_links = [a for a in soup.find_all("a")]
    zip_links = [
        a
        for a in all_links
        if "href" in a.attrs and a["href"].endswith(".zip")
        if a["href"] not in done_urls
    ]
    return [a["href"] for a in zip_links]


def create_bldgs_table(conn: duckdb.DuckDBPyConnection, overwrite: bool = False):
    """Create the usa_structures duckdb table.

    Args:
        conn (duckdb.DuckDBPyConnection): duckdb connection
        overwrite (bool): flag to overwrite existing table
    """

    bldgs_schema = SchemaConfig.from_yaml(ALL_SCHEMAS_PATH).schemas["usa_structures"]

    # Set up
    if overwrite:
        create_statement = "CREATE OR REPLACE TABLE"
    else:
        create_statement = "CREATE TABLE IF NOT EXISTS"
    execute(
        conn,
        f"""
        {create_statement} usa_structures
        ({bldgs_schema.duckdb_schema}, PRIMARY KEY ({bldgs_schema.duckdb_pk}));
    """,
    )


def ensure_columns(gdf: gpd.GeoDataFrame, expected_columns: list) -> gpd.GeoDataFrame:
    """
    Ensure that all expected columns exist in the DataFrame.
    If any are missing, they are added with None (null) values.

    Args:
        gdf (gpd.GeoDataFrame): The input GeoDataFrame.
        expected_columns (list): List of expected column names.

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame with all expected columns.
    """
    for col in expected_columns:
        if col not in gdf.columns:
            gdf[col] = None
    return gdf


def get_bldgs(conn: duckdb.DuckDBPyConnection, zip_url: str):
    """Downloads, unzips, and loads USA Structures building outlines from a given zip URL into DuckDB.

    Args:
        conn (duckdb.DuckDBPyConnection): DuckDB connection to insert data into.
        zip_url (str): URL of the zip file containing the geodatabase (GDB).

    Raises:
        ValueError: If no layers are found in the geodatabase.
    """

    logging.info(f"Downloading zip file: {zip_url}")

    # Download the zip file
    response = requests.get(zip_url)

    # Create a temporary directory for extraction
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = os.path.join(tmp_dir, "download.zip")
        with open(zip_path, "wb") as f:
            f.write(response.content)

        # Extract ZIP contents
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmp_dir)

        # Locate the GDB directory inside the extracted contents
        subdir = zip_url.split("/")[-1].split(".zip")[0]
        list_unzipped_files = os.listdir(os.path.join(tmp_dir, subdir))
        gdb = [f for f in list_unzipped_files if f.endswith(".gdb")][0]
        gdb_path = os.path.join(tmp_dir, subdir, gdb)

        # List layers in the GDB
        layers = gpd.list_layers(gdb_path)
        if len(layers) == 0:
            raise ValueError(f"No layers found in {gdb_path}")

        layer_name = layers["name"].loc[0]

        logging.info(f"Reading layer: {layer_name} from {gdb_path}")

        # Read the GeoDataFrame
        gdf = gpd.read_file(gdb_path, layer=layer_name)

        bldgs_schema = SchemaConfig.from_yaml(ALL_SCHEMAS_PATH).schemas[
            "usa_structures"
        ]
        gdf = ensure_columns(gdf, bldgs_schema.schema.keys())
        gdf["NAME"] = layer_name
        gdf["URL"] = zip_url

        # Assert that BUILD_ID is a unique id
        assert (
            gdf[bldgs_schema.primary_key].duplicated().sum() == 0
        ), f"{bldgs_schema.primary_key} is non-unique."

        insert_gdf_into_table(conn, gdf, "usa_structures", bldgs_schema)

        logging.info(f"Inserted {len(gdf)} rows into usa_structures")


if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--overwrite",
        dest="overwrite",
        action="store_true",
        help="When used, existing table is overwritten.",
    )

    args = parser.parse_args()

    overwrite = args.overwrite

    conn = initialize()
    links = get_zip_links(conn)
    # links = ['https://fema-femadata.s3.amazonaws.com/Partners/ORNL/USA_Structures/Rhode+Island/Deliverable20230502RI.zip']
    if len(links) > 0:
        create_bldgs_table(conn, overwrite)
        # create_bldgs_table(conn, True)
        for link in links:
            get_bldgs(conn, link)
