from typing import Tuple
import os


def get_shakemap_files(shakemap_dir: str) -> Tuple:
    """Returns the ShakeMap file paths.

    Args:
        shakemap_dir (str): path to shakemap dir

    Returns:
        mi (str): path to mi shapefile
        pgv (str): path to pgv shapefile
        pga(str): path to pga shapefile
    """
    mi = os.path.join(shakemap_dir, "mi.shp")
    pgv = os.path.join(shakemap_dir, "pgv.shp")
    pga = os.path.join(shakemap_dir, "pga.shp")
    return mi, pgv, pga
