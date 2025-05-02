from shapely import Point
from urllib.request import urlopen
import geopandas as gpd
import json
import os
import zipfile
import io
from datetime import datetime
from utils.within_conus import check_coords
from utils.get_file_paths import get_shakemap_dir
from utils.status_logger import log_status, get_last_status
from utils.get_date import convert_to_timestamp
import logging
from utils.duckdb import execute, insert_gdf_into_table
import duckdb
from schemas.epicenters import primary_key as epicenters_pk, schema as epicenters_schema
from schemas.shakemaps import primary_key as shakemaps_pk, schema as shakemaps_schema
from configs.event import Event
from configs.shakemap import ShakeMap
from constants import FEEDURL

logging.basicConfig(level=logging.INFO)


def get_data_from_url(url: str):
    """Read data from a provided url.

    Args:
        url (str): url to read data from
    Returns:
        data (bytes): data from url
    """
    fh = urlopen(url)
    data = fh.read()
    fh.close()

    return data


def add_shakemap_layer_to_duckdb(
    conn: duckdb.DuckDBPyConnection, event: Event, shapefile: str
):

    shp_path = os.path.join(event.shakemap_dir, f"{shapefile}.shp")
    gdf = gpd.read_file(shp_path)
    gdf["event_id"] = event.id
    gdf["updated"] = event.properties.updated
    gdf["dataset"] = shapefile
    insert_gdf_into_table(conn, gdf, "shakemaps", shakemaps_schema, shakemaps_pk)


def create_shakemap_gis_files(
    conn: duckdb.DuckDBPyConnection,
    event: Event,
    shapezip_url: str = None,
    test: bool = False,
):
    """Extracts & unzips ShakeMap GIS Files. Converts the earthquake epicenter
    into a point shapefile.

    Args:
        conn (duckdb.DuckDBPyConnection): duckdb connection
        event (Event): event base model
        shapezip_url (str): URL of the ShakeMap zip file
        test (bool): whether or not to run in test mode, False by default
    """
    if not test:
        assert shapezip_url, "missing parameter: shapezip_url"

        data = get_data_from_url(shapezip_url)

        # Create a StringIO object, which behaves like a file
        zip_buffer = io.BytesIO(data)

        # Create a ZipFile object, instantiated with our file-like StringIO object.
        # Extract all of the Data from that StringIO object into files in the provided output directory.
        shakemap_zip = zipfile.ZipFile(zip_buffer, "r", zipfile.ZIP_DEFLATED)
        shakemap_zip.extractall(event.shakemap_dir)
        shakemap_zip.close()
        zip_buffer.close()

        logging.info(
            f"New event successfully downloaded: {event.id} | {event.properties.title}"
        )

    log_status(event.id, event.properties.status, event.properties.updated, conn)

    data_dict = [
        {
            "event_id": event.id,
            "title": event.properties.title,
            "magnitude": event.properties.mag,
            "date_time": event.properties.time_pretty,
            "place": event.properties.mag,
            "depth_km": event.geometry.depth,
            "url": event.properties.url,
            "status": event.properties.status,
            "updated": event.properties.updated,
        }
    ]

    # update empty point with epicenter lat/long
    epicenter = Point(event.geometry.lon, event.geometry.lat)
    # convert to geodataframe
    event_gdf = gpd.GeoDataFrame(data_dict, geometry=[epicenter])
    event_gdf.to_file(os.path.join(event.shakemap_dir, "epicenter.shp"))

    # add epicenter to epicenters table
    insert_gdf_into_table(
        conn, event_gdf, "epicenters", epicenters_schema, epicenters_pk
    )

    # add shakemap files to shakemaps table
    for shp in ["mi", "pga", "pgv"]:
        add_shakemap_layer_to_duckdb(conn, event, shp)


def earthquake_shakemap_download(
    conn: duckdb.DuckDBPyConnection, mmi_threshold: int = 4.0, overwrite: bool = False
) -> list:
    """Check for shakemaps using the uncommented FEEDURL.

    Args:
        conn (duckdb.DuckDBPyConnection): duckdb connection
        mmi_threshold (int): MMI threshold for earthquakes to download.
        overwrite (bool): When true, overwrites existing files.

    Returns:
        new_events (list): list of new events that were found & downloaded
    """

    shakemap_dir = get_shakemap_dir()
    new_events = []

    # Get the data from the FEEDURL as a json dictionary
    data = get_data_from_url(FEEDURL)
    feed_dict = json.loads(data)

    for earthquake_dict in feed_dict["features"]:
        event = Event(**earthquake_dict)

        if event.properties.mag < mmi_threshold:
            logging.info(f"Skipping {event.id}: mag < {mmi_threshold}")
            continue

        data = get_data_from_url(event.properties.detail)
        shakemap_dict = json.loads(data)

        if "shakemap" not in shakemap_dict["properties"]["products"].keys():
            logging.info(f"Skipping {event.id}: no shakemap available")
            continue

        shakemap = ShakeMap(**shakemap_dict["properties"]["products"]["shakemap"][0])

        in_conus = check_coords(event.geometry.lat, event.geometry.lon)
        if not in_conus:
            logging.info("Skipping {}: epicenter not in conus".format(event.id))
            continue

        # get the download url for the shape zipfile
        shapezip_url = shakemap.contents["download/shape.zip"].url
        event_dir = os.path.join(shakemap_dir, event.id)

        # Creates a new folder (named the eventid) if it does not already exist
        result_df = execute(
            conn, f"SELECT * FROM shakemaps WHERE event_id = '{event.id}';"
        ).fetchdf()
        if (len(result_df) < 1) or overwrite:
            if not os.path.exists(event.shakemap_dir):
                os.mkdir(event.shakemap_dir)
            logging.info(f"Downloading Event ID: {event.id} to: {event.shakemap_dir}")

            create_shakemap_gis_files(conn, event, shapezip_url)

            file_list = os.listdir(event.shakemap_dir)
            logging.info(
                f"Extracted {len(file_list)} ShakeMap files to {event.shakemap_dir}"
            )
            new_events.append(event)

        else:
            logging.info(
                f"Event ID {event.id} exists in shakemaps table already. Checking for updates."
            )

            old_status, old_updated = get_last_status(event.id, conn)

            # check to see if new dataset has been updated or has a new status
            status_change = False
            recent_update = False

            if event.properties.status != old_status:
                status_change = True

            if (
                datetime.fromtimestamp(
                    int(convert_to_timestamp(event.properties.updated)) / 1000
                )
                > old_updated
            ):
                recent_update = True

            if recent_update or status_change:

                logging.info(f"Status update found for Event ID {event.id}.")

                # create archive subdirectory
                list_subfolders = [f.name for f in os.scandir(event_dir) if f.is_dir()]
                old_date = datetime.fromtimestamp(int(old_updated[:-3])).strftime(
                    "%Y%m%d"
                )
                archive_folder_name = "archive_{}".format(old_date)
                archive_zip_name = archive_folder_name + ".zip"

                if archive_zip_name not in list_subfolders:
                    # copy all old files to new archive folder
                    archive_zip_path = os.path.join(event_dir, archive_zip_name)
                    files_to_move = [
                        f
                        for f in os.listdir(event_dir)
                        if os.path.isfile(os.path.join(event_dir, f))
                    ]

                    with zipfile.ZipFile(archive_zip_path, "w") as zip:
                        for file in files_to_move:
                            zip.write(os.path.join(event_dir, file))
                    for file in files_to_move:
                        os.remove(os.path.join(event_dir, file))

                else:
                    # delete all old files if they have already been moved to archive folder
                    files_to_delete = [
                        f
                        for f in os.listdir(event_dir)
                        if os.path.isfile(os.path.join(event_dir, f))
                    ]
                    for file in files_to_delete:
                        os.remove(os.path.join(event_dir, file))

                logging.info(
                    f"Previously downloaded ShakeMap files for {event.id} have been archived."
                )

                logging.info(f"Downloading new shakemaps for Event ID: {event.id}")
                create_shakemap_gis_files(conn, event, shapezip_url)

                filecount = [
                    f
                    for f in os.listdir(event_dir)
                    if os.path.isfile(os.path.join(event_dir, f))
                ]
                logging.info(
                    f"Successfully downloaded {len(filecount)} ShakeMap files to {event_dir}"
                )
                new_events(event)

            else:

                logging.info(
                    f"ShakeMap files for {event.id} already exist and have not been updated."
                )

    logging.info("Completed.")

    return new_events
