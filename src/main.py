import os
import time
        
from earthquake_shakemap_download import check_for_shakemaps
import o2_Earthquake_ShakeMap_Into_CensusGeographies
import o3_Earthquake_GetBldgCentroids
import o4_TractLevel_DamageAssessmentModel
import config


def main(testingmode = True):
    if not testingmode:
        # if not in testing mode, look for real new shakemaps
        new_events = check_for_shakemaps()
        # new events should be a list of newly downloaded earthquake event folders


    else:
        # if testing mode, use the napa 2014 shakemap
        print('testing mode')
        # new_events = [config.NapaEventDir]
        new_events = [config.IdahoEventDir]

    for event in new_events:
        print('\nCensus Data Processing for: ', event)
        o2_Earthquake_ShakeMap_Into_CensusGeographies.shakemap_into_census_geo(eventdir = event)

        print('\nGathering Building Outlines for: ', event)
        ORNL_LB_bldgs = o3_Earthquake_GetBldgCentroids.shakemap_get_bldgs(eventdir = event)

        print('\nRunning Tract-Level Damage Assessment Model for: ', event)
        o4_TractLevel_DamageAssessmentModel.main(eventdir = event)

    return


if __name__ == "__main__":
    start_time = time.time()
    main(testingmode=True)
    print("--- {} seconds ---".format(time.time() - start_time))

