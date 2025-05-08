# EarthquakeDamageModel

For more information about model methodology, review [this blog post on Medium](https://medium.com/new-light-technologies/a-predictive-earthquake-damage-model-written-in-python-e1862518fd92).

## v2.0
Originally, this model was built using Esri's arcpy library, which is non-open source. Version 2.0 of the model includes the following major updates:
- Only utilizes open source Python libraries
- All datasets are stored and accessed using DuckDB
- Web scraper to download Census Tracts & Building Outlines for you
- Linters forcing better code documentation and standards

## Developer Setup
Set up a conda environment using the `requirements.txt` file.
Activate the conda environment before running any of the python commands below.
All commands in this README should be run inside of the following path:
`EarthquakeDamageModel/src`

## Data Downloads

This Python package contains modules to download Census Tracts and Building Outlines (USA Structures) for the USA. These datasets are required for the model to run. You should run these commands once, to download the source data and extract them to the local DuckDB that will contain all data necessary for the model. You do not need to run these commands again unless you change the data source, or delete the database and need to recreate it. In which case, you can add the `--overwrite` flag to the commands below.

All input datasets (tracts, building outlines, shakemaps, epicenters, exposure and damage outputs, etc) are stored in the following database: `EarthquakeDamageModel/data/eq_damage_model.db` and can be accessed via the command line (with your conda environment activated). From here, you can interact with the tables using SQL:
```
>> duckdb data/eq_damage_model.db
D show tables;
┌──────────────────────┐
│         name         │
│       varchar        │
├──────────────────────┤
│ bldg_type_mappings   │
│ census_geo_exposure  │
│ damage_function_vars │
│ damage_mappings      │
│ epicenters           │
│ event_log            │
│ shakemaps            │
│ tracts_2024          │
│ usa_structures       │
└──────────────────────┘
D select * from event_log limit 5;
┌──────────────┬──────────┬─────────────────────┐
│   event_id   │  status  │      timestamp      │
│   varchar    │ varchar  │      timestamp      │
├──────────────┼──────────┼─────────────────────┤
│ ci40925991   │ reviewed │ 2025-04-20 17:18:42 │
│ ak02550duu6u │ reviewed │ 2025-04-20 19:27:02 │
│ us6000q6cs   │ reviewed │ 2025-04-17 02:06:34 │
│ ci40926623   │ reviewed │ 2025-04-19 00:26:22 │
│ napa2014     │ None     │ 1970-01-01 00:00:00 │
└──────────────┴──────────┴─────────────────────┘
```

### Census Tracts
Source: https://www2.census.gov/geo/tiger/TIGER2024/TRACT/
To download, run:

```
python get_census_geos.py
```

You can add the flag `--overwrite` to overwrite the existing DuckDB table.

### USA Structures
Source: https://disasters.geoplatform.gov/USA_Structures/

To download, run:

```
python get_bldgs.py
```

You can add the flag `--overwrite` to overwrite the existing DuckDB table.


## Run the Earthquake Damage Model

Then, in terminal run the following initiate the Earthquake Model:
```
python main.py
```

The following args are available for use:

`--test`: Use this flag to run in testing mode.
`--overwrite`: Use this flag to overwrite existing tables.
`--mmi`: Use this arg to set a new threshold for ShakeMap downloads. Default value is 4.0, so any ShakeMaps with a Magnitude < 4.0 will not be downloaded. For example, `python main.py --mmi=3.0` would change the Magnitude threshold from 4.0 to 3.0.

You can also change the `FEED_URL` in `src/constants.py` to change the ShakeMap API that is scanned for new events.


#### Testing Mode:
The model can be set up to run on a Task Scheduler and it will check for new earthquake events
using the [USGS ShakeMap API](https://earthquake.usgs.gov/fdsnws/event/1/) in order to estimate impacts in near-real time.
The model can be run in <i>testing mode</i> to demonstrate what the model outputs should look like.
To run the model in testing mode:
1. Unzip the shape.zip files inside the ShakeMaps_Testing subdirectories.
2. Change the function parameters in main.py "testing_mode" to be <b>True</b>.
3. Update file paths in `config.py` and uncomment lines 20/21 of `main.py` (depending on which test to run)
4. Follow the instructions below to set up the environment and run the program.


**References**
- Mike Hearne, USGS ["get-event.py"](https://gist.github.com/mhearne-usgs/6b040c0b423b7d03f4b9)
- [OpenQuake Platform](https://platform.openquake.org/) (for Hazus Damage Functions)
- [Hazus Earthquake Technical Manual](https://www.fema.gov/flood-maps/tools-resources/flood-map-products/hazus/user-technical-manuals#:~:text=Hazus%20Earthquake%20Manuals&text=The%20Hazus%20Earthquake%20User%20and,%2C%20scenario%2C%20or%20probabilistic%20earthquakes.)

**Contact**
Madeline Jones - madeline.jones.data.engineer@gmail.com
