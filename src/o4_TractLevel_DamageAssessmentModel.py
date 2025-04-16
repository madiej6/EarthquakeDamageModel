import os
import geopandas as gp
import pandas as pd
from scipy.stats import norm
import numpy as np
import time
import config

# Import Spreadsheet with Hazus Building Type Breakdown per Tract
bldg_percentages_by_tract_csv = r"..\Tables\Building_Percentages_Per_Tract_ALLSTATES.csv"
bldg_percentages_by_tract_df = pd.read_csv(bldg_percentages_by_tract_csv)

#add leading zeroes to FIPS codes that do not have leading zeroes
bldg_percentages_by_tract_df["Tract_str"] = bldg_percentages_by_tract_df["Tract"].apply(str)
for fips in bldg_percentages_by_tract_df["Tract_str"].unique():
    if len(fips) == 11:
        None
    elif len(fips) == 10:
        # add leading zero to fips string
        newfips = "0" + fips
        idx = bldg_percentages_by_tract_df[bldg_percentages_by_tract_df["Tract_str"]==fips]["Tract_str"].index
        bldg_percentages_by_tract_df.loc[idx, "Tract_str"] = newfips

# Import Damage Function Variables Spreadsheet
dmgfvars = r"..\Tables\DamageFunctionVariables.csv"
dmgfvarsDF = pd.read_csv(dmgfvars)
dmgfvarsDF = dmgfvarsDF.drop('Unnamed: 0', axis=1)
list_bldgtypes = dmgfvarsDF["BLDG_TYPE"].unique()


def main(tracts_layer = "census_tract_max_mmi_pga_pgv_bldgcount", eventdir = config.IdahoEventDir):

    gdb = os.path.join(eventdir, "eqmodel_outputs.gdb")

    tracts = gp.read_file(gdb, layer = tracts_layer)
    tract_FIPS_list = tracts["FIPS"].unique()

    newcols = ['W1', 'W2', 'S1L', 'S1M', 'S1H', 'S2L', 'S2M', 'S2H', 'S3',
               'S4L', 'S4M', 'S4H', 'S5L', 'S5M', 'S5H', 'C1L', 'C1M', 'C1H', 'C2L',
               'C2M', 'C2H', 'C3L', 'C3M', 'C3H', 'PC1', 'PC2L', 'PC2M', 'PC2H',
               'RM1L', 'RM1M', 'RM2L', 'RM2M', 'RM2H', 'URML', 'URMM', 'MH', 'Slight',
               'Moderate', 'Extensive', 'Complete']
    for col in newcols:
        tracts[col] = 0

    for FIPS in tract_FIPS_list:
        subset = tracts[tracts["FIPS"] == FIPS]

        df = subset[["FIPS", "max_MMI", "max_PGA", "max_PGV", "min_PGA", "mean_PGA", "Point_Count", "geometry",
                     'W1', 'W2', 'S1L', 'S1M', 'S1H', 'S2L', 'S2M', 'S2H', 'S3',
                     'S4L', 'S4M', 'S4H', 'S5L', 'S5M', 'S5H', 'C1L', 'C1M', 'C1H', 'C2L',
                     'C2M', 'C2H', 'C3L', 'C3M', 'C3H', 'PC1', 'PC2L', 'PC2M', 'PC2H',
                     'RM1L', 'RM1M', 'RM2L', 'RM2M', 'RM2H', 'URML', 'URMM', 'MH',
                     'Slight', 'Moderate', 'Extensive', 'Complete']]

        subset_bldgpcts = bldg_percentages_by_tract_df[bldg_percentages_by_tract_df["Tract_str"] == FIPS]
        if len(subset_bldgpcts) == 0:
            continue

        bldgtype_cols = ['W1', 'W2', 'S1L', 'S1M', 'S1H', 'S2L', 'S2M', 'S2H', 'S3',
                    'S4L', 'S4M', 'S4H', 'S5L', 'S5M', 'S5H', 'C1L', 'C1M', 'C1H', 'C2L',
                    'C2M', 'C2H', 'C3L', 'C3M', 'C3H', 'PC1', 'PC2L', 'PC2M', 'PC2H',
                    'RM1L', 'RM1M', 'RM2L', 'RM2M', 'RM2H', 'URML', 'URMM', 'MH']

        # multiply total building count by percentage for each building type
        bldgcount = df["Point_Count"].item()
        for col in bldgtype_cols:
            df[col] = bldgcount * subset_bldgpcts[col].iloc[0]


        maxPGA = df["max_PGA"].item()
        minPGA = df["min_PGA"].item()
        meanPGA = df["mean_PGA"].item()

        # This runs through each building type, assuming High Code but dropping to Medium, Low or Pre-Code depending
        # on what variables are available, then grabs the variables associated with that Building Type + Code

        # Then, plots the damage function curve and based on the Min or Max PGA, estimates the probability of damage
        # for that structure type. The probabilities are then multiplied by the number of structures in the tract.


        for BLDG_TYPE in list_bldgtypes:

            df_vars = dmgfvarsDF[(dmgfvarsDF["BLDG_TYPE"]==BLDG_TYPE) & (dmgfvarsDF["BUILDINGCO"]=="HC")]
            seiscode = "HC"
            if len(df_vars) == 0:
                df_vars = dmgfvarsDF[(dmgfvarsDF["BLDG_TYPE"]==BLDG_TYPE) & (dmgfvarsDF["BUILDINGCO"]=="MC")]
                seiscode = "MC"
                if len(df_vars) == 0:
                    df_vars = dmgfvarsDF[(dmgfvarsDF["BLDG_TYPE"]==BLDG_TYPE) & (dmgfvarsDF["BUILDINGCO"]=="LC")]
                    seiscode = "LC"
                    if len(df_vars) == 0:
                        df_vars = dmgfvarsDF[(dmgfvarsDF["BLDG_TYPE"]==BLDG_TYPE) & (dmgfvarsDF["BUILDINGCO"]=="PC")]
                        seiscode = "PC"

            #Beta
            Bslight = df_vars["BETASLIGHT"].item()
            Bmoderate = df_vars["BETAMODERA"].item()
            Bextensive = df_vars["BETAEXTENS"].item()
            Bcomplete = df_vars["BETACOMPLE"].item()
            #Median
            PGAslight = df_vars["MEDIANSLIG"].item()
            PGAmoderate = df_vars["MEDIANMODE"].item()
            PGAextensive = df_vars["MEDIANEXTE"].item()
            PGAcomplete = df_vars["MEDIANCOMP"].item()

            Pslight = norm.cdf((1/Bslight)*np.log(minPGA/PGAslight))
            Pmoderate = norm.cdf((1/Bmoderate)*np.log(minPGA/PGAmoderate))
            Pextensive = norm.cdf((1/Bextensive)*np.log(minPGA/PGAextensive))
            Pcomplete = norm.cdf((1/Bcomplete)*np.log(minPGA/PGAcomplete))

            numSlight = df[BLDG_TYPE].item() * Pslight
            numModerate = numSlight * Pmoderate
            numExtensive = numModerate * Pextensive
            numComplete = numExtensive * Pcomplete

            numSlight = numSlight - numModerate
            numModerate = numModerate - numExtensive
            numExtensive = numExtensive - numComplete

            df["Slight"] += numSlight
            df["Moderate"] += numModerate
            df["Extensive"] += numExtensive
            df["Complete"] += numComplete

        tracts.update(df)

    tracts["Green"] = tracts["Slight"]+tracts["Moderate"]
    tracts["Yellow"] = tracts["Extensive"]
    tracts["Red"] = tracts["Complete"]

    tracts.to_file(os.path.join(eventdir, "TractLevel_DamageAssessmentModel_Output.shp"))

    return



if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- {} seconds ---".format(time.time() - start_time))