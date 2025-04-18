top = 49.3457868 # north lat
left = -124.7844079 # west long
right = -66.9513812 # east long
bottom = 24.7433195 # south lat

def check_coords(lat, lng):
    """
    Accepts a list of lat/lng tuples.
    
    Returns the list of tuples that are within the bounding box for the US.
    """
    if bottom <= lat <= top and left <= lng <= right:
        inside_box = 1
    else:
        inside_box = 0
    return inside_box
