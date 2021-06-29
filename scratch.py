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
import traceback
import multiprocessing as mp
import gc
from pathlib import Path
import argparse

import luconfig
from helpers import etime
import helpers
import landuse_rev2 as landuse
import tc.TC_LU_Submodule_noq_v1 as trees_over
from tc.createTiles_v1 import createTiles
import burn_in
import lu_change.lu_change_vector_v1_callable as lu_change_module

import subprocess



parser = argparse.ArgumentParser(description='Integrated Land Use production arg parser')
parser.add_argument('-cfs', nargs='+', help='list co_fips (cf) ', required=True)
parser.add_argument('--test', default=False, action='store_true')

args = parser.parse_args()
cflist = list(args.cfs)

batch_size = luconfig.batch_size
here_turf_batch_size = batch_size
test = args.test # confirm if this works

folder = luconfig.folder



# connect to blob



for cf in cflist:

    print('testing sp data copy')
    print(cf)

    copy_input = f"cp -R /home/azureuser/abData/version1/{cf} /home/azureuser/azData/version1"
    copy_output = f"cp -R /home/azureuser/azData/version1/{cf}/output /home/azureuser/abData/version1/{cf}"
    
    bash_command(input_copy_cmd)
    print('do python things')
    bash_command(output_copy_cmd)
    

print('finished copy')
