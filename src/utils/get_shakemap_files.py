
def get_shakemap_files(shakemap_dir: str):
    mi = "{}\mi.shp".format(shakemap_dir)
    pgv = "{}\pgv.shp".format(shakemap_dir)
    pga = "{}\pga.shp".format(shakemap_dir)
    return mi, pgv, pga
