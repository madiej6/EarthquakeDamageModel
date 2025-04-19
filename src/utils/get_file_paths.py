import os


def get_shakemap_dir():
    shakemap_dir = os.path.join(os.getcwd(), "data", "shakemaps")
    # Set file path to save ShakeMap zip files to
    if not os.path.exists(shakemap_dir):
        os.mkdir(shakemap_dir)

    return shakemap_dir
