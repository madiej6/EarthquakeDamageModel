import requests
from typing import List
import io
import duckdb
import geopandas as gpd
from bs4 import BeautifulSoup
from utils.duckdb import execute, initialize
import logging
import tempfile
import os
from schemas.tracts import schema, primary_key


logging.basicConfig(level=logging.INFO)

# Constants
CENSUS_URL = "https://www2.census.gov/geo/tiger/TIGER2024/TRACT/"


# Scrape the FTP-style directory page for links
def get_zip_links(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """Gets all zip download links at a provided URL.

    Args:
        conn (duckdb.DuckDBPyConnection): duckdb connection

    Returns:
        List[str]: list of zip download links
    """

    done_states = execute(conn, "select distinct statefp from tracts_2024;").fetchall()
    if len(done_states) > 0:
        done_states = [i[0] for i in done_states]

    response = requests.get(CENSUS_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    all_links = [a for a in soup.find_all("a")]
    zip_links = [
        a
        for a in all_links
        if "href" in a.attrs and a["href"].endswith(".zip")
        if a["href"].split("_")[2] not in done_states
    ]
    return [CENSUS_URL + a["href"] for a in zip_links]


def create_tracts_table(conn: duckdb.DuckDBPyConnection, overwrite: bool = False):

    tracts_schema = [f"{col} {type}" for col, type in schema.items()]
    # Set up
    if overwrite:
        create_statement = "CREATE OR REPLACE TABLE"
    else:
        create_statement = "CREATE TABLE IF NOT EXISTS"
    execute(
        conn,
        f"""
        {create_statement} tracts_2024
        ({', '.join(tracts_schema)}, PRIMARY KEY ({primary_key}));
    """,
    )


# Download and extract shapefile to memory, then insert into DuckDB
def get_tracts(conn: duckdb.DuckDBPyConnection, zip_url: str):
    """Extracts 2024 Census Tract data into DuckDB.

    Args:
        conn (duckdb.DuckDBPyConnection): duckdb connection
        zip_url (str): download url
    """

    logging.info(f"Processing {zip_url}")

    # Download the zip file
    r = requests.get(zip_url)
    zip_bytes = io.BytesIO(r.content)

    # Write the zip to a temporary local file
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
        tmp_zip_file.write(zip_bytes.read())
        tmp_zip_path = tmp_zip_file.name

    # Read from the zip using /vsizip/
    vsizip_path = f"/vsizip/{tmp_zip_path}"
    gdf = gpd.read_file(vsizip_path, engine="pyogrio")

    # convert geometry to WKT
    gdf["geometry"] = gdf["geometry"].apply(lambda geom: geom.wkt)
    # insert into DuckDB
    conn.register("gdf_view", gdf)
    conn.execute(
        """
        INSERT INTO tracts_2024
        SELECT * FROM gdf_view
    """
    )
    conn.unregister("gdf_view")

    # Clean up temporary zip file
    os.remove(tmp_zip_path)


if __name__ == "__main__":
    conn = initialize()
    links = get_zip_links(conn)
    if len(links) > 0:
        create_tracts_table(conn, False)
        for link in links:
            get_tracts(conn, link)
