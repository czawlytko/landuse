
os.walk(z:/whatver/V1_LC)

for cf in V1_dir:
    # check and make dirs
    if dir in ("input", "output", "temp"):
        mkdir()

    # check for input files and copy from various destinations
    in_file_list = ["whatever_landcover.tif", "parcels.shp", "whatever..."]
    for in_file in in_file_list:
        