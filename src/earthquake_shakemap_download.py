from shapely import Point
from urllib.request import urlopen
import geopandas as gpd
import json
import os
import zipfile
import io
import datetime
from utils.within_conus import check_coords
from utils.get_file_paths import get_shakemap_dir
from utils.status_logger import log_status, get_last_status
import logging

logging.basicConfig(level=logging.INFO)

FEEDURL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson"  # Significant Events - 1 week
# FEEDURL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson' #1 hour M4.5+
# FEEDURL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson' #1 day M4.5+


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


def create_shakemap_gis_files(
    event_id: str, shapezip_url: str, event_dir: str, earthquake_dict: dict
):
    """Extracts & unzips ShakeMap GIS Files. Converts the earthquake epicenter
    into a point shapefile.

    Args:
        event_id(str): event id
        shapezip_url (str): URL of the ShakeMap zip file
        event_dir (str): filepath of the event dir where files will be extracted to
        earthquake_dict (dict): earthquake json from the FEED URL
    """

    data = get_data_from_url(shapezip_url)

    # Create a StringIO object, which behaves like a file
    stringbuf = io.BytesIO(data)

    # Create a ZipFile object, instantiated with our file-like StringIO object.
    # Extract all of the Data from that StringIO object into files in the provided output directory.
    shakemap_zip = zipfile.ZipFile(stringbuf, "r", zipfile.ZIP_DEFLATED)
    shakemap_zip.extractall(event_dir)
    shakemap_zip.close()
    stringbuf.close()

    # Create feature class of earthquake info
    epi_x = earthquake_dict["geometry"]["coordinates"][0]
    epi_y = earthquake_dict["geometry"]["coordinates"][1]
    depth = earthquake_dict["geometry"]["coordinates"][2]
    title = str(earthquake_dict["properties"]["title"])
    mag = earthquake_dict["properties"]["mag"]
    time = str(earthquake_dict["properties"]["time"])
    time_pretty = datetime.datetime.fromtimestamp(int(time[:-3])).strftime("%c")
    place = str(earthquake_dict["properties"]["place"])
    url = str(earthquake_dict["properties"]["url"])
    event_id = str(event_id)
    status = str(earthquake_dict["properties"]["status"])
    updated = str(earthquake_dict["properties"]["updated"])
    updated_pretty = datetime.datetime.fromtimestamp(int(updated[:-3])).strftime("%c")

    log_status(event_dir, status, updated)

    event_details = (title, mag, time_pretty, place, depth, url, event_id)
    logging.info(f"New event successfully downloaded: {event_details}")

    # Update empty point with epicenter lat/long
    epi = Point(epi_x, epi_y)

    data = [
        {
            "Title": title,
            "Magnitude": mag,
            "Date_Time": time_pretty,
            "Place": place,
            "Depth_km": depth,
            "URL": url,
            "Event_ID": event_id,
            "Status": status,
            "Updated": updated_pretty,
        }
    ]
    event_gdf = gpd.GeoDataFrame(data, geometry=[epi])
    event_gdf.to_file(os.path.join(event_dir, "epicenter.shp"))


def check_for_shakemaps(mmi_threshold: int = 4) -> list:
    """Check for shakemaps using the uncommented FEEDURL.

    Args:
        mmi_threshold (int): MMI threshold for earthquakes to download.

    Returns:
        new_shakemap_folders (list): list of file paths for the data that was extracted
    """

    shakemap_dir = get_shakemap_dir()
    new_shakemap_folders = []

    data = get_data_from_url(FEEDURL)
    feed_dict = json.loads(
        data
    )  # Parse that Data using the stdlib json module.  This turns into a Python dictionary.

    # noinspection PyUnboundLocalVariable
    for earthquake_dict in feed_dict[
        "features"
    ]:  # jdict['features'] is the list of events
        event_id = earthquake_dict["id"]

        event_url = earthquake_dict["properties"]["detail"]
        data = get_data_from_url(event_url)
        shakemap_dict = json.loads(data)
        if shakemap_dict["properties"]["mag"] < mmi_threshold:
            logging.info("Skipping {}: mag < {}".format(event_id, mmi_threshold))
            continue
        if "shakemap" not in shakemap_dict["properties"]["products"].keys():
            logging.info("Skipping {}: no shakemap available".format(event_id))
            continue

        [lon, lat] = shakemap_dict["geometry"]["coordinates"][0:2]
        in_conus = check_coords(lat, lon)
        if not in_conus:
            logging.info("Skipping {}: epicenter not in conus".format(event_id))
            continue

        # get the first shakemap associated with the event
        shakemap = shakemap_dict["properties"]["products"]["shakemap"][0]
        # get the download url for the shape zipfile
        shapezip_url = shakemap["contents"]["download/shape.zip"]["url"]

        event_dir = os.path.join(shakemap_dir, str(event_id))

        # Creates a new folder (named the eventid) if it does not already exist
        if not os.path.isdir(event_dir):
            os.mkdir(event_dir)
            logging.info("New Event ID: {}".format(event_dir))

            create_shakemap_gis_files(
                event_id, shapezip_url, event_dir, earthquake_dict
            )

            file_list = os.listdir(event_dir)
            logging.info(
                "Extracted {} ShakeMap files to {}".format(len(file_list), event_dir)
            )
            new_shakemap_folders.append(event_dir)

        else:

            old_status, old_updated = get_last_status(event_dir)

            status = str(earthquake_dict["properties"]["status"])
            updated = str(earthquake_dict["properties"]["updated"])

            # check to see if new dataset has been updated or has a new status
            status_change = False
            recent_update = False

            if status != old_status:
                status_change = True

            if int(updated) > int(old_updated):
                recent_update = True

            if recent_update or status_change:

                # create archive subdirectory
                list_subfolders = [f.name for f in os.scandir(event_dir) if f.is_dir()]
                old_date = datetime.datetime.fromtimestamp(
                    int(old_updated[:-3])
                ).strftime("%Y%m%d")
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
                    files_to_move.remove("event_info.txt")

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
                    files_to_delete.remove("event_info.txt")
                    for file in files_to_delete:
                        os.remove(os.path.join(event_dir, file))

                logging.info(
                    "Previously downloaded ShakeMap files for {} have been archived.".format(
                        event_id
                    )
                )

                create_shakemap_gis_files(
                    event_id, shapezip_url, event_dir, earthquake_dict
                )

                filecount = [
                    f
                    for f in os.listdir(event_dir)
                    if os.path.isfile(os.path.join(event_dir, f))
                ]
                logging.info(
                    "Successfully downloaded {} ShakeMap files to {}".format(
                        len(filecount), event_dir
                    )
                )
                new_shakemap_folders.append(event_dir)

            else:

                logging.info(
                    "ShakeMap files for {} already exist and have not been updated.".format(
                        event_id
                    )
                )

    logging.info("Completed.")

    return new_shakemap_folders


if __name__ == "__main__":
    check_for_shakemaps()
