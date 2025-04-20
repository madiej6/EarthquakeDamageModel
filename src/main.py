import time
import argparse

from earthquake_shakemap_download import earthquake_shakemap_download

from shakemap_census_exposure import shakemap_into_census_geo

# from get_bldg_centroids import shakemap_get_bldgs
# from tract_damage_model import main as tract_damages
import constants
import logging
from utils.duckdb import initialize

logging.basicConfig(level=logging.INFO)


def main(mmi_threshold: int = 4, test_mode: bool = False):

    conn = initialize()

    if not test_mode:
        # if not in testing mode, look for real new shakemaps
        new_events = earthquake_shakemap_download(conn, mmi_threshold)
        # new events should be a list of newly downloaded earthquake event folders

    else:
        # if testing mode, use the napa 2014 shakemap
        logging.info("testing mode")
        # new_events = [constants.NapaEventDir]
        new_events = [constants.IdahoEventDir]

    for event in new_events:
        logging.info("Census Data Processing for: ", event)
        shakemap_into_census_geo(eventdir=event)

        logging.info("Gathering Building Outlines for: ", event)
        # shakemap_get_bldgs(eventdir=event)

        logging.info("Running Tract-Level Damage Assessment Model for: ", event)
        # tract_damages(eventdir=event)

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
        "--mmi",
        dest="mmi_threshold",
        type=float,
        help="MMI Threshold. Only checks for events greater than or equal to this MMI.",
    )

    args = parser.parse_args()

    test = args.test
    mmi_threshold = args.mmi_threshold

    main(mmi_threshold, test)

    logging.info("--- {} seconds ---".format(time.time() - start_time))
