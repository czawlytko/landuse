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
import traceback # used for test sjoin assertion errors in sjoin_mp
import multiprocessing as mp
import gc
from pathlib import Path
import argparse

import luconfig
from helpers import etime
import helpers
import landuse_rev2 as landuse
import tc.TC_LU_Submodule_v1 as trees_over
from tc.createTiles_v1 import createTiles
import burn_in
import lu_change.lu_change_vector_v1_callable as lu_change_module

def intro(cflist):
    folder = luconfig.folder
    batch_log_Path = luconfig.batch_log_Path

    print(luconfig.forgivemeforhavingfun)
    print(f'\n\nS  T  A  R  T \n{time.asctime()}')
    print(f'Revision 2 - Last edited: 6/3/2021')

    b_log = open(batch_log_Path, "a")
    b_log.write(f'\nStart Batch {str(time.asctime)}\n--{os.path.basename(__file__)}')
    b_log.write(f'--Batch CF List: {cflist}')
    b_log.write(f'\n--Cores Available: {mp.cpu_count()}')
    b_log.write(f'\n--Fiona version: {fiona.__version__}')
    b_log.write(f'\n--Geopandas version: {gpd.__version__}')
    b_log.write(f'\n--Shapely version: {shapely.__version__}')
    b_log.close()

    print(f'\n--Batch CF List: {cflist}')
    print(f'--{os.path.basename(__file__)}')
    print(f'--Cores Available: {mp.cpu_count()}')
    print(f'--Fiona version: {fiona.__version__}')
    print(f'--Geopandas version: {gpd.__version__}')
    print(f'--Shapely version: {shapely.__version__}')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Integrated Land Use production arg parser')
    parser.add_argument('-cfs', nargs='+', help='list co_fips (cf) ', required=True)
    parser.add_argument('--test', default=False, action='store_true')

    args = parser.parse_args()
    cflist = list(args.cfs)

    batch_size = luconfig.batch_size
    here_turf_batch_size = batch_size
    test = args.test # confirm if this works

    folder = luconfig.folder

    intro(cflist)

    etime("batch", f"Start Batch: {cflist}", time.time())

    for cf in cflist: # replace with args/CLI after testing
        print('--Main.py Test:', test, type(test))
        print("--batch_size: ", batch_size)
        cf_st = time.time()

        # LAND USE
        if not os.path.isfile(f'{luconfig.folder}/{cf}/output/data.gpkg'):
            helpers.county_check(cf) # check ancillary data and fips and...
            lu_flag, psegs = landuse.RUN(cf, test) 
            if lu_flag != 0:
                print("Land Use failed. See logs.")
                sys.exit()
        else:
            print('LU already complete')
            lu_flag = 0
            restart = 1

        # TREE CANOPY
        ## TILES
        if not os.path.isfile(f'{luconfig.folder}/{cf}/output/trees_over.gpkg'):
            if not os.path.isfile(f'{luconfig.folder}/{cf}/temp/{cf}_tiles.shp'):
                print("Creating tiles...")
                tc_st = time.time()
                try:
                    createTiles(cf, psegs)
                except:
                    print('Failed to create tiles from psegs in memory. Reading.')
                    psegs = gpd.read_file(f'{luconfig.folder}/{cf}/output/data.gpkg', layer='psegs_lu')
                    createTiles(cf, psegs)
                etime("batch", "Created Tiles for TC", tc_st)
                del psegs # maybe remove?

            # TREE CANOPY MAIN
            tc_flag = trees_over.run_trees_over_submodule(luconfig.TC_CPUS, cf)
            if tc_flag == -1:
                print("Trees over Submodule incomplete; Check log for error")
                sys.exit()
        else:
            print('TC already complete')
            tc_flag = 0
        # BURN IN

        if lu_flag == 0 and tc_flag == 0 and not os.path.isfile(f"{folder}/{cf}/output/{cf}_lu_2017_2018.tif"):
            burnin_flag = burn_in.run_burnin_submodule(luconfig.folder, luconfig.anci_folder, cf)
            if burnin_flag == -1:
                print("Burn in Submodule incomplete; Check log for error")
                sys.exit()
        if os.path.isfile(f"{folder}/{cf}/output/{cf}_lu_2017_2018.tif"):
            burnin_flag = 0

        
        # LAND USE CHANGE
        if lu_flag == 0  and burnin_flag == 0 and tc_flag == 0:
            lu_type = 'v1'
            lu_chg_flag = lu_change_module.run_lu_change(cf, lu_type)

            if lu_chg_flag == -1:
                print("LU Change Module incomplete; Check log for error")
                sys.exit()

        if lu_flag == 0 and tc_flag == 0 and burnin_flag == 0 and lu_chg_flag == 0:
            etime("batch", f"{cf} Completed full run", cf_st)

