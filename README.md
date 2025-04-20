# EarthquakeDamageModel

# TO-DO:
- Convert entire model to open source (geopandas instead of arcpy)
- Convert usage of text files to duckdb

#### Data Downloads:
The following supplemental data sets will need to be downloaded and stored in the "EarthquakeModel\Data" folder with the following names:

| Data Source | Link to Download | Geoprocessing Instructions | Name & Location in Data Folder
|-------|--------|---------|---------|
| Esri/Census| [link](https://www.arcgis.com/home/item.html?id=a00d6b6149b34ed3b833e10fb72ef47b)| Export layer "USA Counties (below 1:3m)" to shapefile | esri_2019_detailed_counties\2019detailedcounties.shp|
| Census | [link](https://www2.census.gov/geo/tiger/TIGER2019/TRACT/) | Download and merge all into a single nationwide tracts shapefile | tl_2019_us_tracts\2019censustracts.shp |

<img align="right" src = "images/bldg_centroids_gdb_screenshot.PNG" width="250">

#### Building Centroids:
In order to estimate the number of structures impacted, the user will need to have a local geodatabase
containing building centroids for each state. Some open and public data sets that could be used are
[Microsoft Building Footprints](https://github.com/microsoft/USBuildingFootprints),
[OpenStreetMap](https://osmbuildings.org/) or
[ORNL USA Structures](http://disasters.geoplatform.gov/publicdata/Partners/ORNL/USA_Structures/).
The building centroids are used to calculate the count of structures within each Census Tract.
The file path of this geodatabase will need to be updated in `config.py` for the variable "BuildingCentroids".
(see image on right)


#### Testing Mode:
The model can be set up to run on a Task Scheduler and it will check for new earthquake events
using the [USGS ShakeMap API](https://earthquake.usgs.gov/fdsnws/event/1/) in order to estimate impacts in near-real time.
The model can be run in <i>testing mode</i> to demonstrate what the model outputs should look like.
To run the model in testing mode:
1. Unzip the shape.zip files inside the ShakeMaps_Testing subdirectories.
2. Change the function parameters in main.py "testing_mode" to be <b>True</b>.
3. Update file paths in `config.py` and uncomment lines 20/21 of `main.py` (depending on which test to run)
4. Follow the instructions below to set up the environment and run the program.

#### Instructions to set up the environment and run the program:

- Set up a conda environment using the requirements.txt file
- Then, in terminal run the following lines to kickoff the Earthquake Model:
`conda activate <env-name>`
`python main.py`

#### Earthquake Model Methodology
For more information about model methodology, review [this blog post on medium](https://medium.com/new-light-technologies/a-predictive-earthquake-damage-model-written-in-python-e1862518fd92).

#### References:
- Mike Hearne, USGS ["get-event.py"](https://gist.github.com/mhearne-usgs/6b040c0b423b7d03f4b9)
- [OpenQuake Platform](https://platform.openquake.org/) (for Hazus Damage Functions)
- [Hazus Earthquake Technical Manual](https://www.fema.gov/flood-maps/tools-resources/flood-map-products/hazus/user-technical-manuals#:~:text=Hazus%20Earthquake%20Manuals&text=The%20Hazus%20Earthquake%20User%20and,%2C%20scenario%2C%20or%20probabilistic%20earthquakes.)

##### Contact
Madeline Jones - madeline.jones.data.engineer@gmail.com
