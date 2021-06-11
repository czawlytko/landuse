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


def read_anci(anci_folder, ancipath, bounds):
    """
    Method: read_anci
    Purpose: Build path with base ancillary folder and sub folder paths, parse file path for file extensions and 
            read file as gdf if vector data. Pass 
    Params: ancipath - path to ancillary file from within anci_folder global variable
            bounds - only read in features overlapping with psegs.bbox or psegs.bounds (or gdfs other than psegs)
    returns: a geodataframe
    """

    # build path
    ancipath = Path(anci_folder, ancipath)
    print(ancipath)
    rt = time.time()
    if ".gdb" in str(ancipath).lower():
        anci = gpd.read_file(os.path.dirname(ancipath), driver='FileGDB', layer=os.path.basename(ancipath), bbox=bounds)
    if ".gpkg" in str(ancipath).lower():
        gpd.read_file(ancipath, bbox=bounds) # check for multiple layers?
        anci = gpd.read_file(ancipath, bbox=bounds)
    else:
        anci = gpd.read_file(ancipath,  bbox=bounds)  # read ancillary

    print(f'----Anci read time: {round(time.time()-rt)}')

    if anci.empty:
        print(f'----Anci ({os.path.basename(ancipath)}) is empty...') 

    return anci

def datacheck(cf, psegs, folder):

    """
    Revised: 4/20/21
    1. Check data strucutre
        1a. required columns
        1b. flex columns
    2. Check/fix column dtypes
    3. Check CRS
    4. TODO - check ancillary data via some list
        4a. path exists
        4b. check crs
    5. TODO - Check psegs coverage - compare psegs area to county boundary area +- threshold
    6. Calculate fresh PSID TODO - move/confirm existence in data prep
    7. Calculate ps_area TODO - move to data prep
    """
    # print("psegs dtypes: \n", psegs.dtypes)

    #required columns for landuse model to work
    reqcolumns = ['SID', 'PID', 'Class_name', 'PSID', 'p_area', 's_area', 'ps_area',
    'p_lc_1', 'p_lc_2', 'p_lc_3', 'p_lc_4', 'p_lc_5', 'p_lc_6', 'p_lc_7', 'p_lc_8', 'p_lc_9', 'p_lc_10', 'p_lc_11', 'p_lc_12',
    's_c18_0', 's_c18_1', 's_c18_2', 's_c18_3', 's_c18_4',
    'p_c18_0', 'p_c18_1', 'p_c18_2', 'p_c18_3', 'p_c18_4',
    's_c1719_0', 's_c1719_1', 's_c1719_2', 's_c1719_3', 's_c1719_4',
    's_n16_0', 's_n16_1', 
    'p_luz','s_luz',
    'logic', 'lu', 'geometry']

    # required values that could realistically be missing from an entire county resulting in no column
    flex_requiredcolumns = [
            'p_lc_2',
            's_c18_1', 's_c18_2', 's_c18_3', 's_c18_4',
            'p_c18_1', 'p_c18_2', 'p_c18_3', 'p_c18_4',
            's_c1719_1', 's_c1719_2', 's_c1719_3', 's_c1719_4',
            'p_luz','s_luz',
            ]

    non_int32_columns = [ 'Class_name','p_luz','s_luz', 'logic', 'lu', 'geometry']

    print(f'--Input psegs crs: {psegs.crs}')
    if psegs.crs != "epsg:5070":
        print("psegs crs is not 'epsg:5070', exiting...")
        sys.exit()

    pre = len(psegs)
    LC_classes = psegs.Class_name.unique()
    print(LC_classes)
    psegs = psegs[(psegs.Class_name.isin(['Tree Canopy Over Roads', 'Low Vegetation', 'Tree Canopy Over Other Impervious Surfaces', 'Other Impervious Surfaces', 'Tree Canopy', 'Scrub\\Shrub', 'Roads', 'Buildings', 'Water', 'Tree Canopy Over Structures','Barren']))]
    # psegs = psegs[(psegs.Class_name != "")] # this removed psegs we watned to keep
    post = len(psegs)
    LC_classes = psegs.Class_name.unique()
    print(LC_classes)

    print(f'--Removed {pre-post} segments missing Class_name\n----Pre-removal : {pre}\n----Post-removal: {post}')

    ADD_cols = ['lu', 'logic', 's_luz', 'p_luz']
    for col in ADD_cols:
        if col not in psegs.columns:
            print(f"adding {col} column as Str/None")
            psegs[col] = None



    if ('Tree Canopy Over Other Impervious Surfaces','Tree Canopy Over Roads', 'Tree Canopy Over Structures') in psegs.Class_name.unique():
        print('Reclassing TC over classes')
        tcreclass_st = time.time()
        psegs['logic'] = psegs.loc[(psegs.Class_name.isin(['Tree Canopy Over Other Impervious Surfaces','Tree Canopy Over Roads','Tree Canopy Over Structures'])), 'logic'] = 'TC Over Landcover'
        psegs['lu'] = psegs.loc[(psegs.Class_name.isin(['Tree Canopy Over Other Impervious Surfaces','Tree Canopy Over Roads','Tree Canopy Over Structures'])), 'lu'] = psegs.Class_name.replace("Tree Canopy Over ", "", regex=True)
        psegs['Class_name'] = psegs.loc[(psegs.Class_name.isin(['Tree Canopy Over Other Impervious Surfaces','Tree Canopy Over Roads','Tree Canopy Over Structures'])), 'Class_name'] = psegs.Class_name.replace("Tree Canopy Over ", "", regex=True)
        etime(cf, psegs,  "Reclassed TC Over classes", tcreclass_st)
        
    if (len(psegs.PSID.unique()) != len(psegs) or 'PSID' not in psegs.columns):
        print("Generating unique PSIDs")
        psegs['PSID'] = [int(x) for x in range(1, len(psegs) + 1)]


    print(f"PSEG rows   : {len(psegs)}")
    print(f"Unique PSIDs: {len(psegs.PSID.unique())}")
    print(f"Unique PIDs : {len(psegs.PID.unique())}")
    print(f"Unique SIDs : {len(psegs.SID.unique())}")

    if 'ps_area' not in psegs.columns:
        if len(psegs) >= 6000000:
            print("--Adding 'ps_area'(batching)")
            get_ps_area(cf, psegs, 1000000)
        else:
            print("--Adding 'ps_area' (no batching)")
            calc_ps_area_st = time.time()
            psegs["ps_area"] = psegs['geometry'].area
            etime(cf, psegs, "calculated 'ps_area'", calc_ps_area_st)

    for col in psegs.columns:
        if col not in reqcolumns:
            print(f'Pseg file has a column that is not required! : deleting {col}!')
            psegs = psegs.drop(columns=[col])

    for col in reqcolumns:
        if col not in psegs.columns:
            if col in flex_requiredcolumns:
                print(f'Flex column missing! - {col} \n--Adding {col} to table as zero...')
                psegs[col] = 0
                psegs[col] = psegs[col].astype('int32')
            else:
                print(f"Required column missing! - {col}")

    
        
        if col not in non_int32_columns:
            psegs[col] = psegs[col].fillna(0)
            psegs = psegs.astype({col: 'int32'})
    
    print(psegs.dtypes)

    return psegs


def sub_df_and_chunk(df1, df2, batch_size):
    """
       1. read in psegs
       2. filter all low veg segments & < 1000 sqm
       3. delete unnecessary columns
       4. create unique ids as DF1SID
       5. filter buildings and clean columns
       6. identify number of chunks for df2
       """
    # create 2 separate GeoDataFrames
    
    df1 = df1[['PSID', 'geometry', 's_area', 'lu']]  # add other fields you need
    df1 = df1.reset_index()  # this is so we can chunk by index
    df1['DF1SID'] = [int(x) for x in range(1, len(df1) + 1)]  # create UID  for df1 for batching

    df2 = df2[['PSID', 'geometry']]
    
    if len(df1) == 0:
        print('df1 is empty')
    if len(df2) == 0:
        print('df2 is empty')

    num_chunks = int((len(df1) / batch_size) + 1)

    return df1, df2, num_chunks

def sub_df_and_chunk2(df1, batch_size):
    """
       For LCMAP timber harvest - only need one query and number of chunks
       1. read in psegs
       2. filter remaining low veg and barren segments
       3. delete unnecessary columns
       4. create unique ids as DF1SID
       5. identify number of chunks for df1
       TODO - skip when chunks are empty, maybe
       """

    df1 = df1[['PSID', 'geometry']]  # 
    df1 = df1.reset_index()  # this is so we can chunk by index
    df1['DF1SID'] = [int(x) for x in range(1, len(df1) + 1)]  # create UID  for df1 for batching

    if df1.empty:
        print('df1 is empty') # TODO -step if this is true

    num_chunks = int((len(df1) / batch_size) + 1)

    return df1, num_chunks

def get_ps_area(cf, psegs, batch_size):

    ps_area_st = time.time()
    ###### PSID is already calculated on ln98 

    num_chunks = int((len(psegs) / batch_size) + 1) # why +1?
    
    chunk_iterator = []
    for i in range(0, num_chunks):
        mn, mx = i * batch_size, (i + 1) * batch_size
        mn = int(mn)
        mx = int(mx)
        print(f"--Chunk: {i}/{num_chunks} PSID {mn} thru {mx}")
        psegs.loc[mn:mx, 'ps_area'] = psegs[mn:mx].geometry.area

    etime(cf, psegs,  'get_ps_area', ps_area_st)


def sjoin_and_border(args):
    """
    args: df1, df2, psegs, DF1SID_min, DF1SID_max
    returns: turf_psid_list
    """
    dft = time.time()
    df1, df2, btype, minborder = args

    sjt = time.time()
    sjoinSeg = gpd.sjoin(df1, df2, how='inner', op='intersects')

    buildingsList = list(set(list(sjoinSeg['PSID_right'])))  # make list of set of lists of PSIDs for buildings

    count = 0
    ecount = 0
    passed_list = []
    for building in buildingsList:
        lowVeg = list(sjoinSeg[sjoinSeg['PSID_right'] == building][
                          'PSID_left'])  # Ids of low veg segments touching current building
        buildingGeo = list(df2[df2['PSID'] == building]['geometry'])[0]  # current building geometry

        for lv in lowVeg:
            count += 1
            lvGeo = list(df1[df1['PSID'] == lv]['geometry'])[0]
            try:

                border = buildingGeo.intersection(lvGeo)  # get multi-line
                blength = border.length
                if btype == 'minimum': # if border type is minimum, only pass if greater minimum shared border length
                    if blength > minborder: 
                        passed_list.append(int(lv))
                if btype == 'percent': # if border type is percent, only pass if greater than percent of total border of lvGeo
                    if blength > (minborder*lvGeo.length):
                        passed_list.append(int(lv))

            except:
                ecount += 1
                pass
            # except Exception as e:
            #     ecount += 1
            #     pass

    del df1
    del df2

    return passed_list


def sjoin_mp_pt2(args):
    
    """
    args: df1, df2
    returns: passed_list - list of PSIDs from df1
    """
    df1, df2 = args
    sjt = time.time()
    sjoinSeg = gpd.sjoin(df1, df2, how='inner', op='intersects')[['PSID']]
    # print('spatial join time: ', round(time.time() - sjt), 'seconds')
    passed_list = list(set(list(sjoinSeg['PSID'])))  # PSIDs from df1
    return passed_list

def apply_lu(psegs, results, newlu, newlogic):
    """
    1. flatten list of lists generated during parallel processing
    2. print n results todo-build in check of this value
    3. apply new lu and new logic to master pseg df
    4. profit
    """
    results = sum(results, [])
    print(f'----Results: {len(results)} {newlogic} segs')
    psegs.loc[psegs['PSID'].isin(results), 'lu'] = newlu + " " + psegs['Class_name']
    psegs.loc[psegs['PSID'].isin(results), 'logic'] = newlogic + " " + psegs['Class_name']


def sjoin_mp(psegs, newlu, newlogic, df1, anci_folder, ancipath, batch_size):
    """
    :param newlu: str of lu class to be assigned
    :param newlogic: str of explanation of logic
    :param dfq1: query for df1 (more polygons)
    :param ancipath: path to ancillary data including filename and extensions
    :param batch_size:  max rows per process
    :return:s
    """

    if type(newlogic) != str:
        print(f'Logic must be string! "logic" is currently {type(newlogic)}')
        sys.exit()

    print(f'--Start sjoin_mp for {newlu}, {newlogic}\n----Anci: {os.path.basename(ancipath)}')

    anci = read_anci(anci_folder,ancipath, psegs.envelope) 

    if anci.empty:
        print(f'----Anci ({os.path.basename(ancipath)}) is empty, skipping step') 

    else:
        try:
            df1, num_chunks = sub_df_and_chunk2(df1, batch_size)
            print(f"----df1 len: {len(df1)} anci len: {len(anci)}")

            cpus_minus_1 = mp.cpu_count() - 1
            # print(f'Utilizing {cpus_minus_1} out of {mp.cpu_count()} cores')
            pool = mp.Pool(processes=cpus_minus_1)

            chunk_iterator = []
            for i in range(0, num_chunks):
                mn, mx = i * batch_size, (i + 1) * batch_size
                gdf_args = df1[mn:mx], anci
                chunk_iterator.append(gdf_args)

            sj_results = pool.map(sjoin_mp_pt2, chunk_iterator)
            pool.close()
            apply_lu(psegs, sj_results, newlu, newlogic)

        except AssertionError:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred on line {} in statement {}'.format(line, text))
            exit(1)

def adjacency_mp(psegs, newlu, newlogic, df1, df2, btype, minborder, batch_size):
    """
    :param newlu: str of lu class to be assigned
    :param newlogic: str of explanation of logic
    :param dfq1: query for df1 (more polygons)
    :param dfq2: query for df2 (less polygons)
    :param minborder:  minimum shared border between df1 and df2
    :param batch_size:  max rows per process
    :return:
    """
    
    df1, df2, num_chunks = sub_df_and_chunk(df1, df2, batch_size)
    print(f"--Start adjacency_mp() for '{newlu}' {time.asctime()}")
    print(f'----Border type: {btype}, Minimum: {minborder}')
    print(f'----Batch_size: {batch_size} df1 len: {len(df1)} df2 len: {len(df2)}')

    cpus_minus_1 = mp.cpu_count() - 1
    pool = mp.Pool(processes=cpus_minus_1)

    chunk_iterator = []
    for i in range(0, num_chunks):
        mn, mx = i * batch_size, (i + 1) * batch_size
        gdf_args = df1[mn:mx], df2, btype, minborder
        chunk_iterator.append(gdf_args)

    bordering_results = pool.map(sjoin_and_border, chunk_iterator)
    pool.close()
    apply_lu(psegs, bordering_results, newlu, newlogic)

def ruleset1(cf, psegs):

    folder = luconfig.folder
    print(f'--Start ruleset1() {time.asctime()}')
    rs1_st = time.time()
    # direct LC to LU classes
    psegs.loc[(psegs.Class_name.isin(['Water','Buildings','Other Impervious Surfaces','Roads','Tree Canopy','Emergent Wetlands'])), 'logic'] = 'landcover'
    psegs.loc[(psegs.Class_name.isin(['Water','Buildings','Other Impervious Surfaces','Roads','Tree Canopy','Emergent Wetlands'])), 'lu'] = psegs['Class_name']

    # Turf: occupied parcel - parcel area <= 4046 - 93sqm # todo this may be interfering with the TCT ruleset
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area <= 4046) & ((psegs.p_lc_7 + psegs.p_lc_8 + psegs.p_lc_9 + psegs.p_lc_10 + psegs.p_lc_11 + psegs.p_lc_12) >= 93), 'logic'] = "Occupied parcel turf"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area <= 4046) & ((psegs.p_lc_7 + psegs.p_lc_8 + psegs.p_lc_9 + psegs.p_lc_10 + psegs.p_lc_11 + psegs.p_lc_12) >= 93), 'lu'] = "Turf Herbaceous"

    # Barren: occupied parcel - parcel area <= 4046 - 93sqm 
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Barren') & (psegs.p_area <= 4046) & ((psegs.p_lc_7 + psegs.p_lc_8 + psegs.p_lc_9 + psegs.p_lc_10 + psegs.p_lc_11 + psegs.p_lc_12) >= 93), 'logic'] = "Occupied parcel barren"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Barren') & (psegs.p_area <= 4046) & ((psegs.p_lc_7 + psegs.p_lc_8 + psegs.p_lc_9 + psegs.p_lc_10 + psegs.p_lc_11 + psegs.p_lc_12) >= 93), 'lu'] = "Developed Barren"

    etime(cf, psegs, 'Ruleset 1', rs1_st)

def ag_cdl(cf, psegs, folder): # classify psegs lu by CDL tabulations, no spatial operations
    print(f'--Start ag_cdl()  {time.asctime()}')
    cdl_st = time.time()
    
    #TODO where matching CDL AND LUZ, fill segs with that LU

    # CDL and NCLD both say pasture.
    # rev2 5/20 - added
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_n16_1 > psegs.s_n16_0) & (psegs.s_c18_4 > (psegs.s_c18_0 + psegs.s_c18_1 +psegs.s_c18_2 +psegs.s_c18_3)), 'logic'] = "nlcd and cdl have maj pas"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_n16_1 > psegs.s_n16_0) & (psegs.s_c18_4 > (psegs.s_c18_0 + psegs.s_c18_1 +psegs.s_c18_2 +psegs.s_c18_3)), 'lu'] = "Pasture" + " " + psegs['Class_name']
    # rev2 5/21 - added
    # rev2 6/2 changed to seg must be at least 50% of parcel area.
    # try p_n16_1 when v1 data prep is complete...
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_n16_1 > psegs.p_area*0.5), 'logic'] = "p_area > 50% NLCD Pasture"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_n16_1 > psegs.p_area*0.5), 'lu'] = "Pasture" + " " + psegs['Class_name']

    # Crop : CDL 2018 only
    #rev2 6/2- added max pasture limit 
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_c18_1 > 10000) & (psegs.s_c18_4 < 10000), 'logic'] = "s_c18_1 > 10000"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_c18_1 > 10000) & (psegs.s_c18_4 < 10000), 'lu'] = "Cropland" + " " + psegs['Class_name']

    # Pasture: CDL 2018 only
    #rev2 6/2- added max crp limit 
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_c18_4 > 10000), 'logic'] = "s_c18_4 > 10000"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_c18_4 > 10000), 'lu'] = "Pasture" + " " + psegs['Class_name']

    # Orchard/Vineyard : CDL 2018 only
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_c18_3 > (psegs.s_area*0.2)), 'logic'] = "s_c18_3 > 20%"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area > 4046*5) & (psegs.s_c18_3 > (psegs.s_area*0.2)), 'lu'] = "Orchard/Vineyard" + " " + psegs['Class_name']

    etime(cf, psegs,  'ag_cdl', cdl_st)

def p_maj_lu(cf, psegs, lc_vals, exclusions, threshold, batch_size, maj_replace):
    """
    args:
        psegs : main pseg gdf
        lc_vals : list of Class_names to be included in area count step
        exclusions : list of LU values to be excluded from PID list, relevant for maj_replace flag
        threshold : pct of parcel coverage required to classify (only includes area covered by lc_vals )
        batch_size : ps chunk size
        maj_replace : reclass existing LUs for lc_vals to majority in parcel minus lu exclusions
    """
    mlu_st = time.time()
    # get list of unique PID values that meet criteria
    # 
    if maj_replace:
        pids = list(set(list(psegs[ (psegs.Class_name.isin(lc_vals)) & (~psegs.lu.isin(exclusions))]['PID'])))
    else:
        pids = list(set(list(psegs[( (psegs.lu.isna()) | (psegs.lu.str.contains("ag_gen"))) & (psegs.Class_name.isin(lc_vals) & (~psegs.lu.isin(exclusions)))]['PID'])))

    # make temp df within parcels that have no lu 
    no_lu_df = psegs[(psegs['PID'].isin(pids)) & (psegs.Class_name.isin(lc_vals))][['PID', 'lu', 'ps_area','geometry']]
    # sum by parcel/lu
    lu_areas_list = [] #list of dfs with PID/lu area estimates
    lu_values = list(set(list(no_lu_df['lu']))) # unique land use values

    if len(lu_values) > 0:
        lu_values.remove(None)
        if 'ag_gen' in lu_values:
            lu_values.remove('ag_gen')
            print(f'removing ag_gen from useable lu_values')
            
        for lu in lu_values:
            tmp = no_lu_df[no_lu_df['lu'] == lu][['PID', 'ps_area']].groupby(['PID']).sum() # group by PID for specified land use and sum area
            tmp = tmp.reset_index() # bring PID back as column
            tmp.rename(columns={'ps_area':lu}, inplace=True) # rename total area column to be lu 
            lu_areas_list.append(tmp)

        # print("lu_areas_list: ", lu_areas_list)
        # merge all area estimates into one df on PID
        lu_area_df = lu_areas_list[0].copy()
        for l in range(1, len(lu_areas_list)): 
            lu_area_df = lu_area_df.merge(lu_areas_list[l], on=['PID'], how='outer')
        # get max lu name and area

        lu_area_df['max_lu'] = lu_area_df[lu_values].idxmax(axis=1) # max lu name
        lu_area_df['lu_area'] = lu_area_df[lu_values].max(axis=1) # max area value
        # print("lu_area_df: ",lu_area_df)

        #remove all uneeded results columns
        cols = ['PID', 'max_lu', 'lu_area']
        lu_area_df = lu_area_df[cols]
        # add parcel area and get % area?
        # get segments of specific classes and sum all segs of classes by PID
        tot_class_area = psegs[psegs['Class_name'].isin(lc_vals)][['PID', 'geometry']]

        if len(tot_class_area) > 6 * 1e6: # greater than 6 mil?
            num_chunks = int((len(tot_class_area) / 1e6) + 1)
            for i in range(0, num_chunks):
                print(f"{i}/{num_chunks}")
                mn, mx = i * batch_size, (i + 1) * batch_size
                mn = int(mn)
                mx = int(mx)
                print(f"calculating ps_area for chunk: {i}/{num_chunks} PSID {mn} thru {mx}")
                tot_class_area.loc[mn:mx, 'ps_area'] = tot_class_area[mn:mx].geometry.area
        else:
            tot_class_area['ps_area'] = tot_class_area.geometry.area

        tot_class_area = tot_class_area[['PID', 'ps_area']]
        tot_class_area = tot_class_area .groupby(['PID']).sum()
        tot_class_area = tot_class_area.reset_index()#bring PID back as column
        tot_class_area.rename(columns={'ps_area':'tot_area'}, inplace=True) #rename total area column
        lu_area_df = lu_area_df.merge(tot_class_area, on=['PID'], how='left') #merge total area with seg area
        lu_area_df.loc[:, 'PctArea'] = lu_area_df['lu_area'] / lu_area_df['tot_area']
        lu_area_df = lu_area_df[lu_area_df['PctArea'] > threshold]
        
        #loop through and apply land use
        max_lus = list(set(list(lu_area_df['max_lu']))) # unique lu values
        print("majority lu value: ", max_lus)
        for lu in max_lus:
            pids = list(set(list(lu_area_df[lu_area_df['max_lu'] == lu]['PID']))) # unique parcels with current lu
            # get unique psids with no lu and that have a maj parcel lu
            psids = list(set(list(psegs[((psegs.lu.isna()) | (psegs.lu == 'ag_gen')) & (psegs.Class_name.isin(lc_vals)) & (psegs.PID.isin(pids))]['PSID'])))
            psegs.loc[psegs['PSID'].isin(psids), 'lu'] = lu
            psegs.loc[psegs['PSID'].isin(psids), 'logic'] =  'majority lu > ' + str(threshold) + " and MajRep:" + str(maj_replace) 
        etime(cf, psegs, f'majority lu MajRep:{str(maj_replace)}', mlu_st)
    else:
        print(f"lu_values is empty...\n {lu_values} \n Full psegs lu:{psegs.lu.unique()}")


def natural_succession(cf, psegs, batch_size):

    # Segmentation perferation statistics/ratios
    # Segment density per parcel area
    # poly count : p_area
    # total border length : p_area

    # Large proportion of TC in parcel and small vegetation
    # rev2 5/21 - changed lc3/p_area from 70% to 45%
    
    psegs.loc[(psegs.lu.isna()) & (psegs.s_area < 1000) & (psegs.s_luz != "TG") & (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])) & ((psegs.p_lc_3 > (psegs.p_area * 0.45))), 'logic'] = r"70% TC parcel,  s_area < 1000"
    psegs.loc[(psegs.lu.isna()) & (psegs.s_area < 1000) & (psegs.s_luz != "TG") &  (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])) & ((psegs.p_lc_3 > (psegs.p_area * 0.45))), 'lu'] = "Natural Succession" + " " + psegs['Class_name']

    # Any parcel with scrub/shrub and no roads or structures, SS is natural succession
    psegs.loc[(psegs.lu.isna()) & (psegs.s_area < 1000) & (psegs.s_luz != "TG") &  (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])) & ((psegs.p_lc_7 + psegs.p_lc_8 + psegs.p_lc_9 + psegs.p_lc_10 + psegs.p_lc_11 + psegs.p_lc_12) < 93) & (psegs.p_c18_0 > (psegs.p_area*0.85)), 'logic'] = "Nat Sus, c18_0*0.85  93m2 of road or building"
    psegs.loc[(psegs.lu.isna()) & (psegs.s_area < 1000) & (psegs.s_luz != "TG") &  (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])) & ((psegs.p_lc_7 + psegs.p_lc_8 + psegs.p_lc_9 + psegs.p_lc_10 + psegs.p_lc_11 + psegs.p_lc_12) < 93) & (psegs.p_c18_0 > (psegs.p_area*0.85)), 'lu'] = "Natural Succession" + " " + psegs['Class_name']
                                                                                                                    
    # If parcel is over 1 acre, is 70%+ TC andand is has <50% p_c18 coverage, 
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])) & (psegs.s_luz != "TG") &  (psegs.p_area > 4046) & (psegs.s_area < 150) & (psegs.p_lc_3 > (psegs.p_area * 0.7)) & (psegs.p_c18_0 > (psegs.p_area*0.7)), 'logic'] = "nat 70% tc 150m2"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])) & (psegs.s_luz != "TG") &  (psegs.p_area > 4046) & (psegs.s_area < 150) & (psegs.p_lc_3 > (psegs.p_area * 0.7)) & (psegs.p_c18_0 > (psegs.p_area*0.7)), 'lu'] = "Natural Succession" + " " + psegs['Class_name']
    

    # If no LU, segment is LV, Barren, and parcel is > 1 acre and touches tree canopy with an s_area > 5000 
    # TODO Separate LV and SS logic. Set size thresh for LV adjacency_mp tool...
    nat_adj_st = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub'])) & (psegs.p_area > 4046) & (psegs.s_area <= 5000) & (psegs.s_luz != "TG") ]
    df2 = psegs[(psegs.Class_name == 'Tree Canopy') & (psegs.s_area >= 10000) & (psegs.p_area > 4046)]
    adjacency_mp(psegs, 'Natural Succession', 'Nat Big TC adj 1', df1, df2, 'percent', 0.7 , batch_size)
    etime(cf, psegs,  "Natural Sucession adjacent 1/3 (LV, B)", nat_adj_st)

    nat_adj2_st = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Scrub\\Shrub'])) & (psegs.p_area > 4046)&  (psegs.s_luz != "TG") ]
    df2 = psegs[(psegs.Class_name == 'Tree Canopy') & (psegs.s_area >= 10000) & (psegs.p_area > 4046)]
    adjacency_mp(psegs, 'Natural Succession', 'Nat Big TC adj 2', df1, df2, 'minimum', 0, batch_size)
    etime(cf, psegs,  "Natural Sucession adjacent 2/3 (SS)", nat_adj2_st)

def luz(cf, psegs):
    # revised 4/20/21 to not reclass buildings in CAFO, CATT, and POUL 
    # rev2 5/20 - added p_area thresh for tg changed s_luz to p_luz for TURF
    # TODO - revisit to help with ag classing
    print(f'--Start LUZ() {time.asctime()}')
    luz_st = time.time()
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation'])) & (psegs.p_luz == 'TG') & (psegs.p_area < 4046) , 'logic'] = "luz 'TG' maj <1ac" 
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation'])) & (psegs.p_luz == 'TG') & (psegs.p_area < 4046) , 'lu'] = "Turf Herbaceous"
    
    # LUZ_values = ('AG_GEN', 'BAR', 'CAFO', 'CATT', 'CENT', 'CONS', 'CROP', 'DEC', 'EVE', 'EXT', 'FALL', 'NAT', 'OV', 'PAS', 'POUL', 'SUS',
    # 'TG', 'TIM', 'WAT', 'WET', 'WET_NT', 'WET_T', 'no_luz')

    # LUZ where schrub shrub should be reclassed
    # rev2 5/20 - fixed pseg subsetting to only include SS, was including LV (whoops!)
    SS_LUZ_valuse = ('NAT', 'OV','SUS','CAFO', 'CATT','TIM')
    for luz in SS_LUZ_valuse:
        if luz == 'SUS':
            lu_name = 'Suspended Succession'
        elif luz == 'NAT':
            lu_name = 'Natural Succession'
        elif luz == 'TIM':
            lu_name = 'Harvested Forest'
        elif luz in ('OV'): # rev2 6/1- removed CROP
            lu_name = 'Orchard/Vineyard'
        elif luz in ('CAFO', 'CATT'):
            lu_name = 'Pasture'
        else:
            lu_name = luz
        
        psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Scrub\\Shrub'])) & (psegs.s_luz == luz) , 'logic'] = f"ss s_luz {luz} maj"
        psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Scrub\\Shrub'])) & (psegs.s_luz == luz) , 'lu'] = lu_name + " " + psegs['Class_name']

    del luz
    del lu_name

    # LUZ where low veg and barren should be reclassed excluding crop and pasture
    LVB_LUZ_values = ('CAFO', 'CATT', 'CENT', 'EXT', 'FALL', 'NAT', 'OV', 'SUS', 'TIM')
    for luz in LVB_LUZ_values:
        if luz in ( 'SUS', 'FALL'):
            lu_name = 'Suspended Succession'
        elif luz in ('NAT', 'EXT'): # added EXT for rev2 from suspended to natural
            lu_name = 'Natural Succession'
        elif luz == 'TIM':
            lu_name = 'Harvested Forest'
        elif luz in ('CAFO', 'CATT'):
            lu_name = 'Pasture'
        elif luz == 'OV':
            lu_name = 'Orchard/Vineyard'
        elif luz in ('AG_GEN', 'CENT'):
            lu_name = 'Cropland'
        else:
            lu_name = luz
        # rev2 6/2 - removed SS
        psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren'])) & (psegs.s_luz == luz) , 'logic'] = f"lvb s_luz {luz} maj"
        psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren'])) & (psegs.s_luz == luz) , 'lu'] = lu_name + " " + psegs['Class_name']
        del luz
        del lu_name

    # Pasture and Crop specific rules for LUZ
    # rev2 6/2 - removed Crop and Pasture from other loop and build more thresholds
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren'])) & (psegs.p_area > 4046*5 ) & (psegs.s_luz == 'PAS') & (psegs.s_luz == psegs.p_luz ) & (psegs.p_c18_4 > psegs.p_c18_1), 'logic'] = f"s_luz PAS, no cdl conflict, 5ac+"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren'])) & (psegs.p_area > 4046*5 ) & (psegs.s_luz == 'PAS') & (psegs.s_luz == psegs.p_luz ) & (psegs.p_c18_4 > psegs.p_c18_1) , 'lu'] = "Pasture " + psegs['Class_name']

    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren'])) & (psegs.p_area > 4046*5 ) & (psegs.s_luz == 'CROP') & (psegs.s_luz == psegs.p_luz ) & (psegs.p_c18_4 < psegs.p_c18_1), 'logic'] = f"s_luz CROP, no cdl conflict, 5ac+"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren'])) & (psegs.p_area > 4046*5 ) & (psegs.s_luz == 'CROP') & (psegs.s_luz == psegs.p_luz ) & (psegs.p_c18_4 < psegs.p_c18_1) , 'lu'] = "Cropland " + psegs['Class_name']

    # Extractive  
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin([['Other Impervious Surfaces', 'Barren']])) & (psegs.s_luz == 'EXT') , 'logic'] = 'Other imp and barren EXT luz'
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin([['Other Impervious Surfaces', 'Barren']])) & (psegs.s_luz == 'EXT') , 'logic'] = 'Suspended Succession' + psegs['Class_name']

    # LUZ where other impervious should be reclassed
    Imp_LUZ_values = ('EXT') # removed 'CAFO', 'CATT', 'POUL' 4/28/21
    for luz in Imp_LUZ_values:
        psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin([['Other Impervious Surfaces']])) & (psegs.s_luz == luz) , 'logic'] = f"s_luz {luz} maj"
        psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin([['Other Impervious Surfaces']])) & (psegs.s_luz == luz) , 'lu'] = luz + " " + psegs['Class_name']

    etime(cf, psegs,  'luz', luz_st)


def solar(psegs, ancipath, newlogic):
    # solar contains pseg with no lu

    """
    :param psegs: geodataframe of pseg file in memory, will be modified
    :param ancipath: string file path to ancillary dataset for intersect with low veg, barren, and ss psegs
    :param newlogic: string explaining logic (example: "transmission lines 15m buff intersect")
    :return:
    """

    print(f'--Start Solar {time.asctime()}')
    solar_st = time.time()

    anci = read_anci(anci_folder,ancipath, psegs.envelope)
    if len(anci) == 0:
        print(f'----No solar found in {cf}')
        pass
    else:
        print(f'there are {len(anci)} solar polys')
        lbsdf = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Buildings', 'Scrub\\Shrub', 'Barren', 'Other Impervious Surfaces']))]
        lbsdf = lbsdf[['SID', 'geometry', 's_area', 'lu']]  # add  needed fields

        sjt = time.time()
        sj = gpd.sjoin(anci, lbsdf, how='inner', op='intersects') 

        # TODO  parallelize
        for i in sj.SID.unique():
            psegs.loc[psegs["SID"] == i, 'logic'] = newlogic
            myClass_name = psegs.loc[psegs['SID'] == i].Class_name.values[0]  # get seg lc value as string
            psegs.loc[psegs["SID"] == i, 'lu'] = f"Solar {myClass_name}"

    etime(cf, psegs,  "solar", solar_st)


## TODO REMOVE, OLD
def here(psegs, ancipath, newlu, newlogic):

    anci = read_anci(anci_folder,ancipath, psegs.envelope)

    if anci.empty:
        print('passing on here()... Check file! ')
        pass

    else:
        print(f"{len(anci)} rows")
        lvdf = psegs[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation')]
        lvdf = lvdf[['SID', 'geometry', 's_area', 'lu']]  # add  needed fields
        print(f'{os.path.basename(ancipath)} \n-read time: {round(time.time() - here_st)} sec')

        sjt = time.time()
        sj = gpd.sjoin(anci, lvdf, how='inner', op='intersects')  # inner = len:640, 12-20sec
        # print(f'--sjoin time: {round(time.time() - sjt)} sec')

        for i in sj.SID.unique():
            psegs.loc[psegs["SID"] == i, 'logic'] = newlogic
            psegs.loc[psegs["SID"] == i, 'lu'] = newlu

    print(f'{len(sj.SID.unique())} segs found with HERE data')
    etime(cf, psegs,  f"HERE ", here_st)


def lcmap_timber_mp(psegs, thlu, thlogic, nslu, nslogic, df1, anci_folder, timHarRasPath, sucAgeRasPath, batch_size):
    """
    Method: lcmap_timber_mp()
    Purpose: Chunk up remaining low veg and barren psegs to be multiprocessed through lcmap timber harvest
             and natural succession workflow. Calls apply_lu to add lu and logic to segments found in workflow.
    Params: thlu - str of lu class to be assigned to timber harvest clearing segments
            thlogic - str of explanation of logic for timber harvest segments
            nslu - str of lu class to be assigned to natural succession due to timber harvest clearing segments
            nslogic - str of explanation of logic for natural succession due to timber harvest clearing segments
            dfq1 - query for df1 (find low veg and barren segs that do not have a lu)
            timHarRasPath - lcmap primary patterns raster with timber harvest class
            sucAgeRasPath - lcmap succession age raster
            batch_size - max rows per process
    returns: N/A
    """

    timHarRasPath = Path(anci_folder, timHarRasPath)
    sucAgeRasPath = Path(anci_folder, sucAgeRasPath)

    print('--Start timber harvest MP')

    df1, num_chunks = sub_df_and_chunk2(df1, batch_size) 
    cols = list(df1)
    df1 = df1.reset_index()
    df1 = df1[cols]
    cpus_minus_1 = mp.cpu_count() - 1
    pool = mp.Pool(processes=cpus_minus_1)
    chunk_iterator = []
    for i in range(num_chunks):
        mn, mx = i * batch_size, (i + 1) * batch_size
        gdf_args = df1[mn:mx], timHarRasPath, sucAgeRasPath
        chunk_iterator.append(gdf_args)
    th_results, ns_results = [], []
    for th_result, ns_result in pool.map(runTH, chunk_iterator): 
        th_results.append(th_result)
        ns_results.append(ns_result)
    pool.close()
    apply_lu(psegs, th_results, thlu, thlogic)
    apply_lu(psegs, ns_results, nslu, nslogic)
    
def runTH(args):
    """
    Method: runTH()
    Purpose: Call function to find timber harvest clearing and natural succession segments due to clearing.
    Params: segments - psegs gdf
            timHarRasPath - lcmap primary patterns raster; contains timber harvest and deforestation classes
            sucAgeRasPath - lcmap succession age raster; contains # of years a pixel has been low veg (grass + crop)
    Returns: th_list - list of PSIDs that contained timber harvest
             ns_list - list of PSIDs that contained nat succession
    Note: Do I need to calc succession age for all non-classes low veg segments? if not, nest within timber harvest
        method. Since we are not capping natural succession by age, we only need age to differentiate between 
        timber harvest clearing and natural succession. if the age data is not used anywhere else, only get age
        if timber harvest is detected."""
    segments, timHarRasPath, sucAgeRasPath = args
    th_list = [] # will store all PSID of timber harvest segments
    ns_list = [] # will store all PSID of timber harvest prior to 2015 (natural succession)
    with rio.open(timHarRasPath) as th:
        with rio.open(sucAgeRasPath) as ns:
            for index, feature in segments.iterrows(): #exclude newly added natural succession
                isHarv = timberHarvest(th, feature) #get timber harvest
                if isHarv != 0: # 0 if not tim harv, otherwise PSID
                    a = getSucAge(ns, feature) #get maj succession age
                    #play with a > 0 - may not be detected yet in LCMAP - does it make a difference to only test a <= 3?
                    #To include 0 below - most update getSucAge by commenting out: tmpAge = tmpAge[tmpAge != 0]
                    if a <= 5: # rev2 6/1 - changed a <= 3 to a<=5
                        th_list.append(isHarv)
                    else: #harvested before 2015 - natural succession
                        ns_list.append(isHarv)
    return th_list, ns_list

def timberHarvest(ras, feature):
    """
    Method: timberHarvest()
    Purpose: Mask lcmap timber harvest raster by current segment and return its PSID if there is timber harvset within the segment and
             at least 10% of the 10m cells in the segment are timber harvest and deforestation.
    Params: ras - rasterio object of LCMAP primary patterns (class 4 in timb harv with not tree cover, 2 is deforestation)
            feature - one row of a the psegs geodataframe
    Returns: PSID of segment if passes timber harvest threshold, 0 otherwise
    """

    p = feature['PSID']  # SID OR PSID?
    tmp, t = rio.mask.mask(ras, [feature['geometry']], crop=True, all_touched=False, nodata=100) #raster array where pixel centroids are within segment
    totalArea = (tmp < 100).sum()
    tmp = tmp[tmp != 0] # remove all 0 pixels
    tmp = tmp[tmp != 100] # remove all 100 pixels (pattern not classified)
    if 4 in tmp: # Check if timber harvest is detected
        pctFull = ((tmp == 2).sum() + (tmp == 4).sum()) / totalArea #total count of timb harv and deforestation divided by total count in segment
        if pctFull >= 0.1: # if timb harv exists and is at least 10% of pixels are timb harv/deforestation
            return p # return PSID
        else:
            return 0
    else:
        return 0

def getSucAge(ageRas, feature):
    """
    Method: getSucAge()
    Purpose: Mask succession age raster by current segment and return its majority age
    Params: ageRas - rasterio object of "succession age" data (# of years in a row LCMAP classes the pixel as grassy/shrub)
            feature - one row of psegs geodataframe
    Returns: majority succession age (0 if not detected - this is ok since UVM detected low veg/barren)
    """
    tmpAge, t_age = rio.mask.mask(ageRas, [feature['geometry']], crop=True, all_touched=False, nodata=100) #raster array where pixel centroids are within segment
    # tmpAge = tmpAge[tmpAge != 0] #remove all 0 pixels - possibly exclude this???
    tmpAge = tmpAge[tmpAge != 100] #remove all 100 pixels 
    tmpAge[tmpAge > 100] = tmpAge[tmpAge>100] - 100 #convert all "ages" to be between 0 and 33 (if >100 it means was developed prior)
    if len(tmpAge) > 0:
        vals, i = np.unique(tmpAge, return_inverse=True) #list of unique raster values whose centroid is within the segment and lsit of their count
        if len(vals) > 0: 
            return vals[np.argmax(np.bincount(i))]
    return 0


######################################################################
################################ MAIN ################################
######################################################################

def RUN(cf, test):

    folder = luconfig.folder
    destinationDir = luconfig.dest
    anci_folder = luconfig.anci_folder
    anci_dict = luconfig.anci_dict
    batch_size = luconfig.batch_size
    batch_log_Path = luconfig.batch_log_Path

    # try:
    cf_st = time.time()
    print(f'\n--Start {cf}: {time.asctime()}')
    b_log = open(batch_log_Path, "a")
    b_log.write(f'\n--Start {cf}: {time.asctime()}')
    b_log.close()

##################################### 
    if test:
        print('--landuse.RUN() Test: ', test, type(test))
        # psegsPath = r"B:/landuse/testing_subsets/psegs_sub4.gpkg" # for checking nat succession
        # psegsPath = r"B:/landuse/nat_testing/psegs_sub_edge.gpkg" # for checking edge NA data
        # inLayer = 'psegs'

        # make output name with iteration
        vnums = []
        for (root,dirs,files) in os.walk('Z:/landuse/testing_subsets/output', topdown=True):
            for file in files:
                vnum = int(file.split("_")[-1].split(".")[0])
                vnums.append(vnum)

        print("--Iteration number: ", max(vnums)+1)
        outPath = f"Z:/landuse/testing_subsets/output/psegs_sub4_rev2_{max(vnums)+1}.gpkg"
        outLayer = 'psegs_lu'

    else:
        print('--landuse.RUN() Test: ', test, type(test))
        psegsPath = f"{folder}/{cf}/input/data.gpkg" # output will swap 'input' to 'output' and layer 'psegs_lu'
        inLayer = 'psegs_joined'
        outPath = psegsPath.replace('input','output')
        outLayer = 'psegs_lu'

##################################### 

    # State specific file paths
    st_dict = luconfig.st_dict

    if st_dict[cf[5:7]] == 'PA':
        timberPath = Path(anci_folder, anci_dict['PAtimberPath'])
    if st_dict[cf[5:7]] == 'MD':
        timberPath = Path(anci_folder, anci_dict['MDtimberPath']) 

    ###  ACTION ********************************************************
    try:
        psread_st = time.time()
        print(f"Start reading psegs {time.asctime()}")
        psegs = gpd.read_file(psegsPath, layer=inLayer, driver='GPKG')
        etime(cf, psegs,  f"psegs read in", psread_st)
        print("psegs dtypes: \n", psegs.dtypes)
        # for layername in fiona.listlayers(psegsPath):
        #     with fiona.open(psegsPath, layer=layername) as src:
        #         print(f"--layer name : {layername} \n--rows: {len(src)}\n--schema: {src.schema}\n")

    except:
        print("ERROR! psegsPath failed to read...")
        print(f'--psegsPath: {psegsPath}')
        for layername in fiona.listlayers(psegsPath):
            with fiona.open(psegsPath, layer=layername) as src:
                print(f"--layer name : {layername} \n--rows: {len(src)}\n--schema: {src.schema}\n")


    psegs = datacheck(cf, psegs, folder)

    ruleset1(cf, psegs)  # run ruleset 1 - populates lu and logic fields

    st_here = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation')]
    sjoin_mp(psegs, "Turf", "HERE turf subset", df1, anci_folder, anci_dict['herePath'], batch_size)
    etime(cf, psegs,  "HERE turf sjoin", st_here)

    # rev2 5/20 - changed from p_area to ps_area 
    # rev2 6/1 - added OR statement to include s_LUZ TG without ps_area limit
    st_buildings = time.time()
    df1 = psegs[((psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.ps_area < 1000)) | ((psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.p_area < 4046*5) & (psegs.s_luz == "TG"))]
    df2 = psegs[(psegs.Class_name == 'Buildings')]
    adjacency_mp(psegs, 'Turf', 'Building turf', df1, df2, 'minimum', 0, batch_size)
    etime(cf, psegs,   "buildings", st_buildings)

    # Added rev2 5/6/2021
    st_buildings2 = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name == 'Low Vegetation') & (psegs.ps_area < 1000)]
    df2 = psegs[(psegs.Class_name == 'Low Vegetation') & (psegs.logic == 'building turf')]
    adjacency_mp(psegs, 'Turf', 'adj to building turf', df1, df2, 'minimum', 0, batch_size)
    etime(cf, psegs,  "buildings2", st_buildings2)

    # print("BUFFER TEST")
    # if not os.path.isfile(BuildingsBufferPath):
    #     buildings_buff = psegs[(psegs.Class_name == 'Buildings')].copy()
    #     buildings_buff['geometry'] = buildings_buff.geometry.buffer(1)
    #     buildings_buff.to_file(outPath, layer="buildings_buff", driver='GPKG')
    # del buildings_buff
    # # st_here = time.time()
    # # lv_dfq1 = """df1 = psegs[(psegs.s_area < 150 & (psegs.Class_name == 'Low Vegetation')]"""
    # # sjoin_mp(psegs, "Turf Herbaceous", "Building Buffer sj", lv_dfq1, BuildingsBufferPath, batch_size)
    # # etime(cf, psegs,  "HERE turf sjoin", st_here)
    ###############

    # rev2 5/10: reordered from step ~7 to step ~5
    # rev2 5/21 - added SS and barren to dfq1 (97.28% prior to adding)
    # Rules cant include any parcel stats due to dataprep issue
    st_roads = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub']) ) & (psegs.s_area < 2000) & ((psegs.s_c18_1 + psegs.s_c18_2 + psegs.s_c18_3 + psegs.s_c18_4) < (psegs.s_area*0.5))]
    df2 = psegs[(psegs.Class_name == 'Roads')]
    adjacency_mp(psegs, 'Suspended Succession', 'roadside sus', df1, df2, 'percent', 0.25, batch_size) # switched from adjacent at all to 25% of total perimeter must intersect with road
    etime(cf, psegs,  "Road-side Suspended Succession Adjacency", st_roads)

    # reference CDL and NCLD tabulations for Crp, OrVin, and Pas
    ag_cdl(cf, psegs, folder)

    # TODO maybe swap to shared border
    # rev2 5/10 - added 10k s_area threshold, 5/20 excluding s_luz CROP, 6/1 added overwriting natural succession LU
    st_trans = time.time()
    df1 = psegs[((psegs.lu.isna()) | (psegs.lu.str.contains("Natural Succession"))) & (~psegs.s_luz.isin(['CROP', 'PAS', 'OV'])) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub']) & (psegs.s_area < 10000))]
    sjoin_mp(psegs, "Suspended Succession", "transmission lines anci", df1, anci_folder, anci_dict['transPath'], batch_size)
    etime(cf, psegs,   "Suspended Succession - Transmission Lines lbs sjoin_mp", st_trans)

    bar_uac_sj = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name == 'Barren')]
    sjoin_mp(psegs, "Developed", "Census UAC sjoin", df1, anci_folder, anci_dict['UACPath'], batch_size)
    etime(cf, psegs,  "Developed Barren UAC sjoin", bar_uac_sj)

    # rev2 5/28 - moved up from just above lcmap timber harvest
    st_lfill = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub']))]
    sjoin_mp(psegs, "Suspended Succession", "landfill sjoin", df1, anci_folder, anci_dict['landfillPath'], batch_size)
    etime(cf, psegs,  "Suspended landfill sjoin_mp", st_lfill)

    st_mines = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub']))]
    sjoin_mp(psegs, "Natural Succession", "mines anci", df1, anci_folder, anci_dict['minePath'], batch_size)
    etime(cf, psegs,  "Mines anci sjoin", st_mines)

    # TODO - adjust size threshold, "shared border:total border", secondary adjacency to the first round results.
    st_bareshore = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name == 'Barren') & (psegs.s_area < 1000)]
    df2 = psegs[(psegs.Class_name == 'Water') & (psegs.s_area > 15)]
    adjacency_mp(psegs, 'Shore', 'bar adj to wat', df1, df2, 'percent', 0.3, batch_size)
    etime(cf, psegs,  "Shore Barren 1", st_bareshore) 

    st_bareshore2 = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name == 'Barren')]
    df2 = psegs[(psegs.lu == 'Shore Barren')]
    adjacency_mp(psegs, 'Shore', 'bar adj to shore', df1, df2, 'minimum', 0, batch_size)
    etime(cf, psegs,  "Shore Barren 2", st_bareshore2) 

    # TODO - possibly swap to sjoin_mp to filter psegs.

    st_solar = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Buildings', 'Scrub\\Shrub', 'Barren', 'Other Impervious Surfaces']))]
    sjoin_mp(psegs, "Solar", "solar sjoin", df1, anci_folder, anci_dict['solarPath'], batch_size)
    etime(cf, psegs,  "Solar anci sjoin", st_solar)

    # Map natural succession based on LC and seg size, contains TWO adjacency_mp() functions
    # rev1 - new submodel
    # rev2 6/1 - added "if not luz TG" clauses
    natural_succession(cf, psegs, batch_size)

    maj_lc_vals = ['Low Vegetation', 'Barren', 'Scrub\\Shrub']
    maj_lu_exclusions = ['Turf Herbaceous', 'Turf Low Vegetation']
    print(f'--Start majority lu (1 of 3) including {maj_lc_vals} - {time.asctime()}')
    p_maj_lu(cf, psegs, maj_lc_vals, maj_lu_exclusions, 0.50, int(1e6), maj_replace=False) # psegs, lc values to include, parcel area threshold (percentage as a decimal)

    if st_dict[cf[5:7]] in ('PA', 'MD'):
        st_timb = time.time()
        df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub']))]
        sjoin_mp(psegs, "Harvested Forest", "timber sjoin", df1, anci_folder, timberPath, batch_size)
        etime(cf, psegs,   "State Timber Harvest anci sjoin", st_timb)
    else:
        print('Skipping State Timber Harvest anci sjoin, no anci for state')

    # LCMAP
    th_st = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren']))]
    lcmap_timber_mp(psegs, 'Harvested Forest', 'lcmap clearing', 'Natural Succession', 'LCMAP clearing before 2015', df1, anci_folder, anci_dict['timHarRasPath'], anci_dict['sucAgeRasPath'],  10000)
    etime(cf, psegs,  'LCMAP timber harvest', th_st)

    # map LUZ values
    # rev2 5/10: reordered luz back to below natural_succession().

    luz(cf, psegs)  

    maj_lc_vals = ['Low Vegetation', 'Barren', 'Scrub\\Shrub']
    maj_lu_exclusions = ['Turf Herbaceous', 'Turf Low Vegetation']
    print(f'--Start majority lu (2 of 3) including {maj_lc_vals} - {time.asctime()}')
    p_maj_lu(cf, psegs, maj_lc_vals, maj_lu_exclusions, 0.25, int(1e6), maj_replace=False)

    # all remaining barren...
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Barren'])), 'logic'] = "All remaining Barren"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Barren'])), 'lu'] = "Developed Barren"

    # all remaining herbaceous with building
    psegs.loc[(psegs.lu.isna()) & (psegs.p_lc_7 > 0) & (psegs.p_area <= 40468) & (psegs.Class_name.isin(['Low Vegetation'])), 'logic'] = "Remaining LV w/ building in parcel <10ac"
    psegs.loc[(psegs.lu.isna()) & (psegs.p_lc_7 > 0) & (psegs.p_area <= 40468) & (psegs.Class_name.isin(['Low Vegetation'])), 'lu'] = "Turf Herbaceous"

    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation'])) & (psegs.p_lc_7 > 0) &(psegs.p_luz == 'TG'), 'logic'] = "remaining lv luz 'TG' w/building" 
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation'])) & (psegs.p_lc_7 > 0) &(psegs.p_luz == 'TG'), 'lu'] = "Turf Herbaceous"

    # trees, no build, 
    psegs.loc[(psegs.lu.isna()) & (psegs.p_lc_7 == 0) & (psegs.p_lc_3 > (psegs.p_area*0.3)) & (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])), 'logic'] = "Remaining LV w/ >30% p_lc_3 no build"
    psegs.loc[(psegs.lu.isna()) & (psegs.p_lc_7 == 0) & (psegs.p_lc_3 > (psegs.p_area*0.3)) & (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])), 'lu'] = "Natural Succession Herbaceous"


    psegs.loc[(psegs.lu.isna()) & (psegs.p_lc_7 == 0) & (psegs.s_luz.isin(['EVE', "DEC"])) & (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])), 'logic'] = "Remaining LV or SS no build EVE or DEC LUZ"
    psegs.loc[(psegs.lu.isna()) & (psegs.p_lc_7 == 0) & (psegs.s_luz.isin(['EVE', "DEC"])) & (psegs.Class_name.isin(['Low Vegetation', 'Scrub\\Shrub'])), 'lu'] = "Natural Succession" + " " + psegs['Class_name']

    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Scrub\\Shrub'])), 'logic'] = "Remaining SS no limits"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Scrub\\Shrub'])), 'lu'] = "Natural Succession" + " " + psegs['Class_name']

    # rev2 6/1 - added 6/4 removed df2 s_area threshold...
    st_remain_nat = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub']))]
    df2 = psegs[(psegs.lu.isin(["Natural Succession Herbaceous", "Natural Succession Scrub\\Shrub"]))]
    adjacency_mp(psegs, 'Natural Succession', 'lvb adj to nat sus', df1, df2, 'minimum', 0, batch_size)
    etime(cf, psegs,  "Remnant Adj to Natural Succession", st_remain_nat) 

    # All remaining lv segs with suspended succession if there is no known parcel area (probably roads or public lands)
    print("REVISE 'p_area = 0' logic when new data prep is ready") # replace with road tabulations
    psegs.loc[(psegs.lu.isna()) & (psegs.p_area == 0) & (psegs.Class_name.isin(['Low Vegetation'])), 'logic'] = "p_area = 0"
    psegs.loc[(psegs.lu.isna()) & (psegs.p_area == 0) & (psegs.Class_name.isin(['Low Vegetation'])), 'lu'] = "Suspended Succession Herbaceous"


    # if na or pasture in <5 parcel WITH NO build, make nat sus
    psegs.loc[(psegs.lu.str.contains("Pasture")) & (psegs.p_lc_7 == 0) & (psegs.p_area < (4046*5)) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub'])), 'logic'] = "Reclass Pas to Nat <5ac p"
    psegs.loc[(psegs.lu.str.contains("Pasture")) & (psegs.p_lc_7 == 0) & (psegs.p_area < (4046*5)) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub'])), 'lu'] = "Natural Succession" + " " + psegs['Class_name']

    # if na or pasture in <5 parcel WITH build, make turf
    psegs.loc[((psegs.lu.str.contains("Pasture")) | psegs.lu.isna()) & (psegs.p_lc_7 > 0) & (psegs.p_area < (4046*5)) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub'])), 'logic'] = "Reclass Pas to Turf <5ac p w/ building"
    psegs.loc[((psegs.lu.str.contains("Pasture")) | psegs.lu.isna()) & (psegs.p_lc_7 > 0) & (psegs.p_area < (4046*5)) & (psegs.Class_name.isin(['Low Vegetation'])), 'lu'] = "Turf Herbaceous"

    maj_lc_vals = ['Low Vegetation']
    maj_lu_exclusions = ['Turf Herbaceous', 'Turf Low Vegetation']
    print(f'--Start majority lu (3 of 3) including {maj_lc_vals} - {time.asctime()}')
    # p_maj_lu(cf, psegs, maj_lc_vals, maj_lu_exclusions, 0.05, int(1e6), maj_replace=True) # rev2 6/1 - changed thresh from 25% to 5%
    p_maj_lu(cf, psegs, maj_lc_vals, maj_lu_exclusions, 0.60, int(1e6), maj_replace=True) # rev2 6/1 - implemented maj_replace and bumped to 60%
    print('TESTING AG P MAJ REPLACE')

    st_all_remain = time.time()
    df1 = psegs[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation', 'Barren', 'Scrub\\Shrub'])) ]
    df2 = psegs[psegs.lu.str.contains("Natural Succession", na=False)]
    adjacency_mp(psegs, 'Natural Succession', 'Remnant adj to nat sus', df1, df2, 'minimum', 0, batch_size)
    etime(cf, psegs,  "Remnant Adj to Natural Succession", st_all_remain) 

    # ag_gen
    psegs.loc[(psegs.lu.isna()) & (psegs.s_luz == 'AG_GEN') & (psegs.s_area > 10000) & (psegs.p_lc_5 > psegs.p_area*0.5) & (psegs.Class_name.isin(['Low Vegetation'])), 'logic'] = "ag_gen last chance"
    psegs.loc[(psegs.lu.isna()) & (psegs.s_luz == 'AG_GEN') & (psegs.s_area > 10000) & (psegs.p_lc_5 > psegs.p_area*0.5) & (psegs.Class_name.isin(['Low Vegetation'])), 'lu'] = "Cropland Herbaceous"



    # rev2 6/4 -added
    maj_lc_vals = ['Low Vegetation']
    maj_lu_exclusions = ['Turf Herbaceous', 'Turf Low Vegetation']
    print(f'--Start majority lu (3 of 3) including {maj_lc_vals} - {time.asctime()}')
    # p_maj_lu(cf, psegs, maj_lc_vals, maj_lu_exclusions, 0.05, int(1e6), maj_replace=True) # rev2 6/1 - changed thresh from 25% to 5%
    print('TESTING AG P MAJ REPLACE')

    # All remaining LV

    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation'])), 'logic'] = "Whatevers left"
    psegs.loc[(psegs.lu.isna()) & (psegs.Class_name.isin(['Low Vegetation'])), 'lu'] = "Suspended Succession Herbaceous"

    ## Classification finished

    """
    TODO  if no lu apply adjacent lu with same Class_name instead of "Whatevers left"
    """

    # #Check for lu.isna() - how many? fill with majority LU from Matching Class_name segs in same parcel
    # if (len(psegs[(psegs.lu.isna())])-len(psegs) > 0):
    #     print(len(psegs[(psegs.lu.isna())])-len(psegs))
    #     # apply lu from closest seg with the similar Class_name

    # POPULATE lucode 
    psegs['lucode'] = 0
    name_dict = luconfig.name_dict
    # clean up lu names to match final format
    for k, v in name_dict.items():
        # print(k, " to ", v)
        psegs['lu'] = psegs['lu'].replace(k, v, regex=True)

    print("\nOutput LU list: ", psegs.lu.unique(), "\n")
    if len(psegs[(psegs.lu.isna())]) != 0:
        print("No LU count: ", len(psegs[(psegs.lu.isna())]))

    # populate lucode field if value in dictionary
    for lu in psegs.lu.unique():
        if lu in luconfig.lu_code_dict.keys():
            for k, v in luconfig.lu_code_dict.items():
                if k == lu:
                    psegs.loc[(psegs.lu == k), 'lucode'] = v
        else:
            print("\n", lu, " not in keys")

    print(psegs.lucode.unique())
    
    ########################

    sec_per_ps =  round(time.time() - cf_st) / len(psegs)
    etime(cf, psegs,  f"{cf} Main landuse pre-write complete! - {time.asctime()}\n({round(sec_per_ps, 4)} sec per pseg)", cf_st)
    b_log = open(batch_log_Path, "a")
    b_log.write(f"{cf} Main `landuse` complete {time.asctime()}")
    b_log.close()

    if not test:
        print('WARNING: Clearing fields from psegs')
        psegs = psegs[['PSID', 'PID', 'SID', 'Class_name', 'lu', 'logic', 'geometry']]

    print(f'Saving psegs to file...')
    print(f'--outPath: {outPath}\n--outLayer: {outLayer}')
    wt = time.time()
    psegs.to_file(outPath, layer=outLayer, driver='GPKG')
    etime(cf, psegs,  f"Output write", wt)
       
    return psegs

    # except Exception as e:
    #     print(f"{cf} FAILED")
    #     etime(cf, psegs,  f"main exception \n{e}", cf_st)
    #     sec_per_ps =  round(time.time() - cf_st) / len(psegs)
    #     print(f'{round(time.time() - cf_st) / len(psegs)} seconds per pseg')
    #     b_log = open(batch_log_Path, "a")
    #     b_log.write(f"{cf} Main landuse FAILED {time.asctime()}")    
    #     b_log.close()
    #     pass

