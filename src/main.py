import time
import argparse
import os
from utils.yaml import load_config_from_yaml

from earthquake_shakemap_download import (
    earthquake_shakemap_download,
    create_shakemap_gis_files,
)

from shakemap_census_exposure import shakemap_into_census_geo

# from get_bldg_centroids import shakemap_get_bldgs
# from tract_damage_model import main as tract_damages
import logging
from utils.duckdb import initialize
from configs.event import Event

logging.basicConfig(level=logging.INFO)


def main(mmi_threshold: int = 4.0, test_mode: bool = False, overwrite: bool = False):

    conn = initialize()

    if not test_mode:
        # if not in testing mode, look for real new shakemaps
        new_events = earthquake_shakemap_download(conn, mmi_threshold, overwrite)
        # new events should be a list of newly downloaded earthquake event folders

    else:
        # if testing mode, use the napa 2014 shakemap
        logging.info("testing mode")
        data = load_config_from_yaml(
            os.path.join("data", "testing", "napa2014", "event_config.yaml")
        )
        event = Event(**data)
        create_shakemap_gis_files(conn, event, test=test_mode)
        # new_events = [constants.NapaEventDir]
        new_events = [event]

    for event in new_events:
        logging.info("Census Data Processing for: ", event.id)
        shakemap_into_census_geo(conn, event, "tracts")

        logging.info("Gathering Building Outlines for: ", event.id)
        # shakemap_get_bldgs(event_id=event_id)

        logging.info("Running Tract-Level Damage Assessment Model for: ", event.id)
        # tract_damages(event_id=event_id)

    return


if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--test",
        dest="test",
        action="store_true",
        help="When used, only run in testing mode.",
    )

    parser.add_argument(
        "--overwrite",
        dest="overwrite",
        action="store_true",
        help="When used, overwrite existing files.",
    )

    parser.add_argument(
        "--mmi",
        dest="mmi_threshold",
        default=4.0,
        type=float,
        help="MMI Threshold. Only checks for events greater than or equal to this MMI.",
    )

    args = parser.parse_args()

    test = args.test
    mmi_threshold = args.mmi_threshold
    overwrite = args.overwrite

    main(mmi_threshold, test, overwrite)

    logging.info("--- {} seconds ---".format(time.time() - start_time))
