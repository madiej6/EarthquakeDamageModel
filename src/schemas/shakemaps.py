schema = {
    "event_id": "VARCHAR",
    "updated": "BIGINT",
    "dataset": "VARCHAR",
    "AREA": "FLOAT",
    "PERIMETER": "FLOAT",
    "PGAPOL_": "BIGINT",
    "PGAPOL_ID": "BIGINT",
    "GRID_CODE": "BIGINT",
    "PARAMVALUE": "FLOAT",
    "geometry": "VARCHAR",
}

primary_key = ["event_id", "updated", "dataset", "PARAMVALUE"]
