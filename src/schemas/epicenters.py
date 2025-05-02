schema = {
    "event_id": "VARCHAR",
    "title": "VARCHAR",
    "magnitude": "FLOAT",
    "date_time": "VARCHAR",
    "place": "VARCHAR",
    "depth_km": "FLOAT",
    "url": "VARCHAR",
    "status": "VARCHAR",
    "updated": "BIGINT",
    "geometry": "VARCHAR",
}

primary_key = ["event_id", "updated"]
