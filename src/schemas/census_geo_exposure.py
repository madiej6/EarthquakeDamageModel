schema = {
    "GEOID": "VARCHAR",
    "event_id": "VARCHAR",
    "MMI_max": "FLOAT",
    "MMI_min": "FLOAT",
    "MMI_avg": "FLOAT",
    "PGV_max": "FLOAT",
    "PGV_min": "FLOAT",
    "PGV_avg": "FLOAT",
    "PGA_max": "FLOAT",
    "PGA_min": "FLOAT",
    "PGA_avg": "FLOAT",
    "MMI_int": "INT",
    "geometry": "VARCHAR",
}

primary_key = ["GEOID", "event_id"]
