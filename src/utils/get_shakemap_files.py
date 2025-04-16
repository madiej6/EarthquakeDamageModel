
def get_shakemap_files(ShakeMapDir):
    mi = "{}\mi.shp".format(ShakeMapDir)
    pgv = "{}\pgv.shp".format(ShakeMapDir)
    pga = "{}\pga.shp".format(ShakeMapDir)
    return mi, pgv, pga
