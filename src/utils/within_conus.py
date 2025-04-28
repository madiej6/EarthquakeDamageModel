top = 49.3457868  # north lat
left = -124.7844079  # west long
right = -66.9513812  # east long
bottom = 24.7433195  # south lat


def check_coords(lat: float, lng: float) -> bool:
    """Accepts a list of lat/lng tuples.

    Args:
        lat (float): latitude
        lng (float): longitude

    Returns:
        True if lat, lon is inside CONUS bounds. False if lat, lon is outside CONUS bounds.
    """
    if bottom <= lat <= top and left <= lng <= right:
        return True
    else:
        return False
