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

    intro(cflist)

    for cf in cflist: # replace with args/CLI after testing
        print('--Main.py Test:', test, type(test))
        print("--batch_size: ", batch_size)

        run_dp = False
        add_data = True # join TA tables to psegs
        run_lu = True
        run_tc = True
        run_bi = True
        run_change = False

        # if not dp_outs.exist() and run_dp:
        #     prep.RUN(folder, cf)

        if run_lu:
            helpers.county_check(cf) # check ancillary data and fips and...
            psegs = landuse.RUN(cf, add_data, test) # cf, adddata bool, test bool)
        else:
            print('Did not run lu')

        if run_tc:
            tc_flag = trees_over.run_trees_over_submodule(luconfig.TC_CPUS, cf)
            if tc_flag == 0:
                print("Trees over Submodule complete")
            elif tc_flag == -1:
                print("Trees over Submodule incomplete; Check log for error")
            else:
                print("Trees over Submodule flag invalid value: ", tc_flag)
                print("Check for trees_over.gpkg manually")
        else:
            print("Did not run_tc")

        if run_bi:
            burnin_flag = burn_in.run_burnin_submodule(luconfig.folder, luconfig.anci_folder, cf)
            if burnin_flag == 0:
                print("Burn in Submodule complete")
            elif burnin_flag == -1:
                print("Burn in Submodule incomplete; Check log for error")
            else:
                print("Burn in Submodule flag invalid value: ", burnin_flag)
                print("Check for lu intermediates manually")
        else:
            print('did not run rasterization and burn-in')

        if run_change:
            lu_type = 'v1' # extension added to file names to distinguish versions of data

            lu_chg_flag = lu_change_module.run_lu_change(cf, lu_type)

            if lu_chg_flag == 0:
                print("LU Change Module complete")
            elif lu_chg_flag == -1:
                print("LU Change Module incomplete; Check log for error")
            else:
                print("LU Change Module flag invalid value: ", lu_chg_flag)
                print("Check for roll up raster manually")
