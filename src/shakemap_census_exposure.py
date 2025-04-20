import os
from utils.get_file_paths import get_shakemap_dir
from utils.get_shakemap_files import get_shakemap_files
import constants


def shakemap_into_census_geo(event_dir=constants.NapaEventDir):

    # ShakeMap GIS File Folder
    shakemap_dir = get_shakemap_dir()

    mi, pgv, pga = get_shakemap_files(event_dir)
    unique = event_dir.split("\\")[-1]

    # Variables for Census Geographies
    ###Blocks = #filepath
    Tracts = os.path.join(
        os.path.dirname(os.getcwd()),
        "Data",
        "tl_2019_us_tracts",
        "2019censustracts.shp",
    )
    DetailCounties = os.path.join(
        os.path.dirname(os.getcwd()),
        "Data",
        "esri_2019_detailed_counties",
        "2019detailedcounties.shp",
    )

    # Other layers/shapefiles
    arcpy.management.CreateFileGDB(eventdir, "eqmodel_outputs")
    arcpy.env.workspace = os.path.join(eventdir, "eqmodel_outputs.gdb")
    GDB = os.path.join(eventdir, "eqmodel_outputs.gdb")

    # Clip all USGS ShakeMap GIS shapefiles to the county layer
    arcpy.Clip_analysis(
        mi, DetailCounties, os.path.join(GDB, "shakemap_countyclip_mmi")
    )
    arcpy.Clip_analysis(
        pgv, DetailCounties, os.path.join(GDB, "shakemap_countyclip_pgv")
    )
    arcpy.Clip_analysis(
        pga, DetailCounties, os.path.join(GDB, "shakemap_countyclip_pga")
    )

    arcpy.AddField_management(
        os.path.join(GDB, "shakemap_countyclip_mmi"),
        "MMI_int",
        "SHORT",
        "",
        "",
        "",
        "MMI_int",
    )
    arcpy.CalculateField_management(
        os.path.join(GDB, "shakemap_countyclip_mmi"),
        "MMI_int",
        "math.floor( !PARAMVALUE! )",
        "PYTHON_9.3",
        "",
    )
    arcpy.Dissolve_management(
        os.path.join(GDB, "shakemap_countyclip_mmi"),
        os.path.join(GDB, "shakemap_countyclip_mmi_int"),
        "MMI_int",
        "",
        "MULTI_PART",
        "DISSOLVE_LINES",
    )

    # need to make layers for all of these before doing spatial join
    arcpy.MakeFeatureLayer_management(DetailCounties, "Counties_lyr_{}".format(unique))
    arcpy.MakeFeatureLayer_management(Tracts, "Tracts_lyr_{}".format(unique))
    # arcpy.MakeFeatureLayer_management(Blocks,"Blocks_lyr_{}".format(unique))

    # Select Counties That Intersect with USGS ShakeMap GIS shapefiles
    SelectedCounties = arcpy.SelectLayerByLocation_management(
        "Counties_lyr_{}".format(unique), "INTERSECT", mi, "", "NEW_SELECTION"
    )

    def set_field_mappings_withmax(layer1, layer2, field_to_max, new_field_name):
        fieldmappings = arcpy.FieldMappings()
        fieldmappings.addTable(layer1)  # SelectedCounties
        fieldmappings.addTable(layer2)  # mmi
        FieldIndex_toMax = fieldmappings.findFieldMapIndex(field_to_max)  # "PARAMVALUE"
        fieldmap = fieldmappings.getFieldMap(FieldIndex_toMax)
        field = fieldmap.outputField
        field.name = new_field_name  # "max_MMI"
        field.aliasName = new_field_name  # "max_MMI"
        fieldmap.outputField = field
        fieldmap.mergeRule = "max"
        fieldmappings.replaceFieldMap(FieldIndex_toMax, fieldmap)
        return fieldmappings

    def set_field_mappings_withmin(layer1, layer2, field_to_min, new_field_name):
        fieldmappings = arcpy.FieldMappings()
        fieldmappings.addTable(layer1)  # SelectedCounties
        fieldmappings.addTable(layer2)  # mmi
        FieldIndex_toMin = fieldmappings.findFieldMapIndex(field_to_min)  # "PARAMVALUE"
        fieldmap = fieldmappings.getFieldMap(FieldIndex_toMin)
        field = fieldmap.outputField
        field.name = new_field_name  # "min_MMI"
        field.aliasName = new_field_name  # "min_MMI"
        fieldmap.outputField = field
        fieldmap.mergeRule = "min"
        fieldmappings.replaceFieldMap(FieldIndex_toMin, fieldmap)
        return fieldmappings

    def set_field_mappings_withmean(layer1, layer2, field_to_mean, new_field_name):
        fieldmappings = arcpy.FieldMappings()
        fieldmappings.addTable(layer1)  # SelectedCounties
        fieldmappings.addTable(layer2)  # mmi
        FieldIndex_toMean = fieldmappings.findFieldMapIndex(
            field_to_mean
        )  # "PARAMVALUE"
        fieldmap = fieldmappings.getFieldMap(FieldIndex_toMean)
        field = fieldmap.outputField
        field.name = new_field_name  # "mean_MMI"
        field.aliasName = new_field_name  # "mean_MMI"
        fieldmap.outputField = field
        fieldmap.mergeRule = "mean"
        fieldmappings.replaceFieldMap(FieldIndex_toMean, fieldmap)
        return fieldmappings

    def remove_field_map(fieldmappings, listoffields):
        for F in listoffields:
            x = fieldmappings.findFieldMapIndex(F)
            fieldmappings.removeFieldMap(x)
        return

    # maxMMI
    fieldmappings = set_field_mappings_withmax(
        SelectedCounties, mi, "PARAMVALUE", "max_MMI"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )

    # Spatial Join MAX MI to each County
    arcpy.SpatialJoin_analysis(
        target_features=SelectedCounties,
        join_features=mi,
        out_feature_class=os.path.join(GDB, "census_county_max_mmi"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # maxPGA
    fieldmappings = set_field_mappings_withmax(
        os.path.join(GDB, "census_county_max_mmi"), pga, "PARAMVALUE", "max_PGA"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )

    # Spatial Join MAX PGA to each County
    arcpy.SpatialJoin_analysis(
        target_features=os.path.join(GDB, "census_county_max_mmi"),
        join_features=pga,
        out_feature_class=os.path.join(GDB, "census_county_max_mmi_pga"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # minPGA
    fieldmappings = set_field_mappings_withmin(
        os.path.join(GDB, "census_county_max_mmi_pga"), pga, "PARAMVALUE", "min_PGA"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )

    # Spatial Join MAX PGV to each County
    arcpy.SpatialJoin_analysis(
        target_features=os.path.join(GDB, "census_county_max_mmi_pga"),
        join_features=pga,
        out_feature_class=os.path.join(GDB, "census_county_max_mmi_pga_"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # maxPGV
    fieldmappings = set_field_mappings_withmax(
        os.path.join(GDB, "census_county_max_mmi_pga_"), pgv, "PARAMVALUE", "max_PGV"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )

    # Spatial Join MAX PGV to each County
    arcpy.SpatialJoin_analysis(
        target_features=os.path.join(GDB, "census_county_max_mmi_pga_"),
        join_features=pgv,
        out_feature_class=os.path.join(GDB, "census_county_max_mmi_pga_pgv"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # Get MI as Integer Field
    arcpy.AddField_management(
        os.path.join(GDB, "census_county_max_mmi_pga_pgv"),
        "max_MMI_int",
        "SHORT",
        "",
        "",
        "",
        "max_MMI_int",
    )
    arcpy.CalculateField_management(
        os.path.join(GDB, "census_county_max_mmi_pga_pgv"),
        "max_MMI_int",
        "math.floor( !max_MMI! )",
        "PYTHON_9.3",
        "",
    )

    # Delete PGA and PGV county feature classes
    arcpy.Delete_management(os.path.join(GDB, "census_county_max_mmi_pga_"))
    arcpy.Delete_management(os.path.join(GDB, "census_county_max_mmi_pga"))
    arcpy.Delete_management(os.path.join(GDB, "census_county_max_mmi"))

    ############################### TRACTS ####################################

    # Select Tracts That Intersect with USGS ShakeMap GIS shapefiles
    SelectedTracts = arcpy.SelectLayerByLocation_management(
        "Tracts_lyr_{}".format(unique), "INTERSECT", mi, "", "NEW_SELECTION"
    )

    # maxMMI
    fieldmappings = set_field_mappings_withmax(
        SelectedTracts, mi, "PARAMVALUE", "max_MMI"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )
    # Spatial Join MAX MI to each Tract
    arcpy.SpatialJoin_analysis(
        target_features=SelectedTracts,
        join_features=mi,
        out_feature_class=os.path.join(GDB, "census_tract_max_mmi"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # maxPGA
    fieldmappings = set_field_mappings_withmax(
        os.path.join(GDB, "census_tract_max_mmi"), pga, "PARAMVALUE", "max_PGA"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )
    # Spatial Join MAX PGA to each Tract
    arcpy.SpatialJoin_analysis(
        target_features=os.path.join(GDB, "census_tract_max_mmi"),
        join_features=pga,
        out_feature_class=os.path.join(GDB, "census_tract_max_mmi_pga"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # minPGA
    fieldmappings = set_field_mappings_withmin(
        os.path.join(GDB, "census_tract_max_mmi_pga"), pga, "PARAMVALUE", "min_PGA"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )
    # Spatial Join MAX PGA to each Tract
    arcpy.SpatialJoin_analysis(
        target_features=os.path.join(GDB, "census_tract_max_mmi_pga"),
        join_features=pga,
        out_feature_class=os.path.join(GDB, "census_tract_max_mmi_pga_"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # meanPGA
    fieldmappings = set_field_mappings_withmean(
        os.path.join(GDB, "census_tract_max_mmi_pga_"), pga, "PARAMVALUE", "mean_PGA"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )
    # Spatial Join MAX PGV to each Tract
    arcpy.SpatialJoin_analysis(
        target_features=os.path.join(GDB, "census_tract_max_mmi_pga_"),
        join_features=pga,
        out_feature_class=os.path.join(GDB, "census_tract_max_mmi_pga__"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # maxPGV
    fieldmappings = set_field_mappings_withmax(
        os.path.join(GDB, "census_tract_max_mmi_pga__"), pgv, "PARAMVALUE", "max_PGV"
    )
    remove_field_map(
        fieldmappings, ["AREA", "PERIMETER", "PGAPOL_", "PGAPOL_ID", "GRID_CODE"]
    )
    # Spatial Join MAX PGV to each Tract
    arcpy.SpatialJoin_analysis(
        target_features=os.path.join(GDB, "census_tract_max_mmi_pga__"),
        join_features=pgv,
        out_feature_class=os.path.join(GDB, "census_tract_max_mmi_pga_pgv"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        field_mapping=fieldmappings,
    )

    # Get MI as Integer Field
    arcpy.AddField_management(
        os.path.join(GDB, "census_tract_max_mmi_pga_pgv"),
        "max_MMI_int",
        "SHORT",
        "",
        "",
        "",
        "max_MMI_int",
    )
    arcpy.CalculateField_management(
        os.path.join(GDB, "census_tract_max_mmi_pga_pgv"),
        "max_MMI_int",
        "math.floor( !max_MMI! )",
        "PYTHON_9.3",
        "",
    )

    # Delete PGA and PGV tract feature classes
    arcpy.Delete_management(os.path.join(GDB, "census_tract_max_mmi_pga__"))
    arcpy.Delete_management(os.path.join(GDB, "census_tract_max_mmi_pga_"))
    arcpy.Delete_management(os.path.join(GDB, "census_tract_max_mmi_pga"))
    arcpy.Delete_management(os.path.join(GDB, "census_tract_max_mmi"))

    # Copy over epicenter file if it exists
    if os.path.exists(os.path.join(eventdir, "Epicenter.shp")):
        # arcpy.MakeFeatureLayer_management(ShakeMapDir+"\Epicenter.shp","Epicenter_lyr")
        arcpy.CopyFeatures_management(
            os.path.join(eventdir, "Epicenter.shp"), os.path.join(GDB, "epicenter")
        )

    return


if __name__ == "__main__":
    shakemap_into_census_geo()
