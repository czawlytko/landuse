import os
from pathlib import Path
import time
import argparse
import multiprocessing as mp
import subprocess
import sys

import luconfig

def etime(cf, note, starttime):
    folder = luconfig.folder

    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    log_dir = Path(f"{folder}/{cf}") # define as pathlib path object, allows pathlib's exists()

    if log_dir.exists() == False:
        Path.mkdir(log_dir)
        print(f'making dir... {log_dir}')
    etime_file = Path(log_dir, "etlog.txt")

    f = open(etime_file, "a")
    if elapsed > 60:
        f.write(f'--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n')
        print(f'--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n')
    else:
        f.write(f'--{note} runtime - {round(elapsed, 2)} sec\n')
        print(f'--{note} runtime - {round(elapsed, 2)} sec\n')
    f.close()

def lu_etime(cf, psegs, note, starttime):
    # print text and write to log file 
    # includes percent of psegs classified and time in HMS or Seconds if time < 60 sec

    folder = luconfig.folder
    elapsed = time.time()-starttime
    log_dir = Path(f"{folder}/{cf}") # define as pathlib path object, allows pathlib's exists()

    if not log_dir.exists():
        Path.mkdir(log_dir)
        print(f'making dir... {log_dir}')

    etime_file = Path(log_dir, "etlog.txt")
    pct = round((len(psegs[(psegs.lu.notna())]))/len(psegs)*100, 2)
    if 'lu' in psegs.columns:  # etime() is used before lu exists
        print(f"----{pct}% lu classified")

    f = open(etime_file, "a")
    if elapsed > 60:
        logText = f'--{note} {pct} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n'
    else:
        logText = f'--{note} {pct} runtime - {round(elapsed, 2)} sec\n'

    f.write()
    print(f'--{note} {pct} runtime - {round(elapsed, 2)} sec\n')

    f.close()

def tformat(elapsed_t):
    if elapsed_t > 60:
        print(f'{time.strftime("%H:%M:%S", time.gmtime(elapsed_t))}')
    else:
        return f"{round(elapsed_t, 2)} seconds"

def checkFile(fPath): # used for all anci data
    if not os.path.isfile(fPath):
        print(f"--Input File Path failed checkFile()\n\t{fPath}")
        sys.exit()
    else:
        pass



def TESTING(args):
    name_dict = {
        "Low Vegetation" : "Herbaceous",
        "Developed Barren" : "Bare Developed",
        r"Scrub\\Shrub" : "Scrub/Shrub",
        "Other Impervious Surfaces" : "Other Impervious Surface",
        "Timber Harvest" : "Harvested Forest",
        "Shore Barren" : "Bare Shore",
        "Pasture" : "Pasture/Hay",
        "Orchard Vineyard" : "Orchard/Vineyard",
        'Solar Other Impervious' : 'Solar Field Impervious',
        'Solar Barren' : 'Solar Field Barren',
        'Solar Herbaceous' : 'Solar Field Herbaceous',
        'Solar Scrub/Shrub' : 'Solar Field Scrub/Shrub',
        }

    import argparse
    import geopandas as gpd
    import pandas as pd
    import shapely
    import rasterio as rio
    import rasterio.mask
    import numpy as np
    import fiona

    import time
    import os
    import sys
    import traceback # track assertion errors in sjoin_mp
    import multiprocessing as mp
    import gc
    from pathlib import Path
    import argparse

    import luconfig
    from helpers import etime


    psegs = gpd.read_file(r"B:/landuse/version1/glou_51073/input/data.gpkg", layer='psegs_joined', driver='GPKG')

    print(psegs.Class_name.unique())

    ADD_cols = ['lu', 'logic', 's_luz', 'p_luz']
    for col in ADD_cols:
        if col not in psegs.columns:
            print(f"adding {col} column as Str/None")
            psegs[col] = None

    if "Tree Canopy Over Roads" in psegs.Class_name.unique():
        print("TC over roads is there!")



    name_dict = luconfig.name_dict
    # clean up lu names to match final format
    for k, v in name_dict.items():
        print(k, " to ", v)
        psegs['lu'] = psegs['lu'].replace(k, v, regex=True)

    print("\nOutput LU list: ", psegs.lu.unique(), "\n")
    if len(psegs[(psegs.lu.isna())]) != 0:
        print("No LU count: ", len(psegs[(psegs.lu.isna())]))

    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'lu'] = "Tree Canopy Over Roads"
    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'logic'] = "TC over"
    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'Class_name'] = "Roads"

    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'lu'] = "Tree Canopy Over Other Impervious Surfaces"
    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'logic'] = "TC over"
    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'Class_name'] = "Other Impervious Surfaces"

    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'lu'] = "Tree Canopy Over Structures"
    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'logic'] = "TC over"
    psegs.loc[(psegs.Class_name == "Tree Canopy Over Roads"), 'Class_name'] = "Buildings"


    # POPULATE lucode 
    psegs['lucode'] = 0
    # populate lucode field if value in dictionary
    for lu in psegs.lu.unique():
        if lu in luconfig.lu_code_dict.keys():
            for k, v in luconfig.lu_code_dict.items():
                if k == lu:
                    psegs.loc[(psegs.lu == k), 'lucode'] = v
        else:
            print("\n", lu, " not in keys")

    print(psegs.lucode.unique())

    time.sleep(30)

    psegs.to_file(r"B:/landuse/version1/glou_51073/output/data2.gpkg", layer='psegs_lu', driver='GPKG')