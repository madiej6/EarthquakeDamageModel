import os

def get_shakemap_dir():
    # Set file path to save ShakeMap zip files to
    if os.path.exists(os.path.join(os.path.dirname(os.getcwd()), 'ShakeMaps')):
        ShakeMapDir = os.path.join(os.path.dirname(os.getcwd()), 'ShakeMaps')
    else:
        os.mkdir(os.path.join(os.path.dirname(os.getcwd()), 'ShakeMaps'))
        ShakeMapDir = os.path.join(os.path.dirname(os.getcwd()), 'ShakeMaps')

    return ShakeMapDir
