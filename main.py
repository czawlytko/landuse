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

def intro(cflist):
    folder = luconfig.folder
    batch_log_Path = luconfig.batch_log_Path

    print(luconfig.forgivemeforhavingfun)
    print(f'\n\nS  T  A  R  T \n{time.asctime()}')
    print(f'Revision 2 - Last edited: 6/3/2021')

    b_log = open(batch_log_Path, "a+")
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

def write_error(cf, module, exc_info, error_log_path):
    with open(error_log_path, "a") as e_log:
        print(f'ERROR: {cf} failed on {module}\n\n')
        e_log.write('----------------------------------\n')
        e_log.write(f'{cf} failed on {module}: \n')
        e_log.write(f'{exc_info}')
        e_log.write('----------------------------------\n')

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

    print('Attempting to mount blob')
    fuse_st = time.time()
    connect2fuse = "blobfuse /home/azureuser/abData --tmp-path=/mnt/resource/blobfusetmp  --config-file=/home/azureuser/fuse_connection.cfg -o attr_timeout=240 -o entry_timeout=240 -o negative_timeout=120"
    helpers.bash_command(connect2fuse)
    etime("batch", "mount blobfuse", fuse_st)

    for cf in cflist: # replace with args/CLI after testing
        print('--Main.py Test:', test, type(test))
        print("--batch_size: ", batch_size)
        cf_st = time.time()

        copy_st = time.time()
        copy_input = f"cp -R /home/azureuser/abData/version1/{cf} /home/azureuser/azData/version1"
        helpers.bash_command(copy_input)
        helpers.bash_command("ls -la")
        etime(cf, "Copied data from blobfuse", copy_st)

        # LAND USE
        if not os.path.isfile(f'{luconfig.folder}/{cf}/output/data.gpkg'):
            helpers.checkCounty(cf) # check ancillary data and fips and...
            try:
                psegs = landuse.RUN(cf, test)
            except:
                write_error(cf, 'Land Use', traceback.format_exc(), luconfig.batch_error_log_Path)
                continue
        else:
            print('\nLU already complete')

        # TREE CANOPY
        if not os.path.isfile(f'{luconfig.folder}/{cf}/output/trees_over.gpkg'):
            if not os.path.isfile(f'{luconfig.folder}/{cf}/temp/tc_tiles.shp'):
                print("Creating tiles...")
                tc_st = time.time()
                try:
                    createTiles(cf, psegs)
                except:
                    print('Failed to create tiles from psegs in memory. Reading.')
                    try:
                        psegs = gpd.read_file(f'{luconfig.folder}/{cf}/output/data.gpkg', layer='psegs_lu')
                        createTiles(cf, psegs)
                    except:
                        write_error(cf, 'Create TC Tiles', traceback.format_exc(), luconfig.batch_error_log_Path)
                        continue
                etime("batch", "Created Tiles for TC", tc_st)
                del psegs # maybe remove?

            # TREE CANOPY MAIN
            # try:
            trees_over.run_trees_over_submodule(luconfig.TC_CPUS, cf)
            # except:
            #     write_error(cf, 'Tree Canopy', traceback.format_exc(), luconfig.batch_error_log_Path)
            #     continue
        else:
            print('\nTC already complete')
        # BURN IN
        out_lu_burnin_path = f"{folder}/{cf}/output/{cf}_lu_2017_2018.tif"
        if not os.path.isfile(out_lu_burnin_path):
            try:
                burn_in.run_burnin_submodule(luconfig.folder, luconfig.anci_folder, cf)
            except:
                write_error(cf, 'Burn In', traceback.format_exc(), luconfig.batch_error_log_Path)
                continue
        else: #os.path.isfile(f"{folder}/{cf}/output/{cf}_lu_2017_2018.tif"):
            print('\nBurn in already complete')

        
        # LAND USE CHANGE
        change_csv_path = f'{folder}/{cf}/output/{cf}_P6LU_Change_v1_summary.csv'
        change_ras_path = f'{folder}/{cf}/output/{cf}_P6LU_Change_v1.tif'
        if not os.path.isfile(change_csv_path) or not os.path.isfile(change_ras_path):
            lu_type = 'v1'
            try:
                lu_change_module.run_lu_change(cf, lu_type)
            except:
                write_error(cf, 'LU Change', traceback.format_exc(), luconfig.batch_error_log_Path)
                continue

        copy_st2 = time.time()
        copy_output = f"cp -R /home/azureuser/azData/version1/{cf}/output /home/azureuser/abData/version1/{cf}"
        helpers.bash_command(copy_output)
        etime(cf, "Copied outputs to blobfuse", copy_st2)


        etime("batch", f"{cf} Completed full run", cf_st)