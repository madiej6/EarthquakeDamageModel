import time

from earthquake_shakemap_download import check_for_shakemaps
import o2_Earthquake_ShakeMap_Into_CensusGeographies
import o3_Earthquake_GetBldgCentroids
import o4_TractLevel_DamageAssessmentModel
import config
import logging

logging.basicConfig(level=logging.INFO)


def main(testingmode=True):
    if not testingmode:
        # if not in testing mode, look for real new shakemaps
        new_events = check_for_shakemaps()
        # new events should be a list of newly downloaded earthquake event folders

    else:
        # if testing mode, use the napa 2014 shakemap
        logging.info("testing mode")
        # new_events = [config.NapaEventDir]
        new_events = [config.IdahoEventDir]

    for event in new_events:
        logging.info("Census Data Processing for: ", event)
        o2_Earthquake_ShakeMap_Into_CensusGeographies.shakemap_into_census_geo(
            eventdir=event
        )

        logging.info("Gathering Building Outlines for: ", event)
        _ = o3_Earthquake_GetBldgCentroids.shakemap_get_bldgs(eventdir=event)

        logging.info("Running Tract-Level Damage Assessment Model for: ", event)
        o4_TractLevel_DamageAssessmentModel.main(eventdir=event)

    return


if __name__ == "__main__":
    start_time = time.time()
    main(testingmode=True)
    logging.info("--- {} seconds ---".format(time.time() - start_time))
