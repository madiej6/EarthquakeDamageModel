import os
import numpy as np
import geopandas as gpd
import duckdb
from configs.event import Event
from utils.duckdb import execute


def shakemap_into_census_geo(
    conn: duckdb.DuckDBPyConnection, event: Event, census_geo: str
) -> None:
    """Joins the ShakeMap values for MMI, PGA and PGV onto census geographies.

    For all values (MMI, PGA, and PGV), the min/max/avg values are joined to the census geo.
    For example, if a county intersects with 3 MMI values, the columns 'MMI_min', 'MMI_max',
    and 'MMI_avg' will all be populated accordingly.

    Args:
        conn (duckdb.DuckDBPyConnection): duckdb connection
        event (Event): pydantic base model
        census_geo (str): the census geography i.e. 'tracts', 'counties', etc.

    Returns:
        None
    """

    gdfs_to_consolidate = []
    for dataset in ["mi", "pga", "pgv"]:

        if dataset == "mi":
            new_col_name = "MMI"
        else:
            new_col_name = dataset.upper()

        gdf = execute(
            conn,
            f"""
        WITH shakemap_extent AS
            (SELECT
                event_id,
                ST_Union_Agg(ST_GEOMFROMTEXT(geometry)) AS geometry
            FROM shakemaps
            WHERE event_id = '{event.id}' and dataset='{dataset}'
            GROUP BY event_id),
        subset_geo AS (
            SELECT
                {census_geo}_2024.GEOID,
                ST_GEOMFROMTEXT({census_geo}_2024.geometry) AS geometry
            FROM {census_geo}_2024 JOIN shakemap_extent
            ON ST_INTERSECTS(ST_GEOMFROMTEXT({census_geo}_2024.geometry),shakemap_extent.geometry)),
        shakemap AS (
            SELECT
                event_id,
                PARAMVALUE AS {new_col_name},
                ST_GEOMFROMTEXT(geometry) AS geometry
            FROM shakemaps
            WHERE event_id = '{event.id}' and dataset='{dataset}'),
        final AS (
            SELECT
                subset_geo.GEOID,
                subset_geo.geometry,
                shakemap.event_id,
                shakemap.{new_col_name}
            FROM subset_geo JOIN shakemap
            ON ST_INTERSECTS(subset_geo.geometry, shakemap.geometry))
        SELECT
            GEOID,
            ANY_VALUE(event_id) AS event_id,
            ST_ASTEXT(ANY_VALUE(geometry)) AS geometry,
            MAX({new_col_name}) AS {new_col_name}_max,
            MIN({new_col_name}) AS {new_col_name}_min,
            AVG({new_col_name}) AS {new_col_name}_avg,
        FROM final
        GROUP BY GEOID;
        """,
        ).fetchdf()

        gdf["geometry"] = gpd.GeoSeries.from_wkt(gdf["geometry"])
        gdf = gpd.GeoDataFrame(gdf, crs="EPSG:4326")

        gdfs_to_consolidate.append(gdf)

    # join all shakemap min/max/avg cols for MMI, PGA & PGV together into single geodataframe
    gdf = (
        gdfs_to_consolidate[0]
        .set_index("GEOID")
        .join(
            gdfs_to_consolidate[1][
                ["GEOID", "PGA_max", "PGA_min", "PGA_avg"]
            ].set_index("GEOID"),
            how="left",
        )
        .join(
            gdfs_to_consolidate[2][
                ["GEOID", "PGV_max", "PGV_min", "PGV_avg"]
            ].set_index("GEOID"),
            how="left",
        )
    )

    # add an "integer" MMI with this math: "math.floor( !max_MMI! )",
    gdf["MMI_int"] = np.floor(gdf["MMI_max"])

    # save as shapefile
    gdf.to_file(os.path.join(event.shakemap_dir, f"{census_geo}_2024_{event.id}.shp"))

    # TO DO: save to duckdb table
