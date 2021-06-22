"""
Script: tabulateArea.py
Purpose: use open source Python functions to mimic ArcPy's TabulateArea function; This code calculates
         pixel count for each unqiue class of a raster within polygon geometries anc can convert the pixel count
         to area by multiplying by the cell size squared. The data is stored as a dictionary, and then converted
         to a Pandas dataframe.
Author: Sarah McDonald, geographer, USGS 
Contact: smcdonald@chesapeakebay.net
"""
import fiona
import numpy as np
import rasterio as rio
import rasterio.mask
import pandas as pd
import geopandas as gpd
import multiprocessing as mp
import os
import time
import sys

sys.path.append(r'B:/landuse')
import luconfig as config
import helpers
    
def readPolyData(gdbPath, layerName, polyID):
    """
    Method: readPolyData()
    Purpose: Read in the polygon data and store the fiona geometries in a dictionary.
    Params:  polyPath - path to polygon data
            polyID - name of field in polygon attribute table to use as unique ID
    Returns: shapes - dictionary of fiona geometries where the keys are the polyID
    """
    gdf = gpd.read_file(gdbPath, layer=layerName)
    gdf = gdf[[polyID, 'geometry']]
    shapes = {feature[polyID]:feature['geometry'] for idx, feature in gdf.iterrows()}
    
    if polyID == 'PID':
        gdf.loc[:, 'p_area'] = gdf.geometry.area
        gdf = gdf[[polyID]+['p_area']]
        gdf.loc[:, 'p_area'] = gdf.p_area.astype(int)
    elif polyID == 'SID':
        gdf.loc[:, 's_area'] = gdf.geometry.area
        gdf = gdf[[polyID]+['s_area']]
        gdf.loc[:, 's_area'] = gdf.s_area.astype(int)

    return shapes, gdf

def tabulateArea_mp(shapes, rasPath, rasVals, cols):
    """
    Method: tabulateAreas_mp()
    Purpose: Calculate the zonal stats / tabulate area for the specified raster and polygons.
    Params: shapes - dict of polygons
            rasPath - path to raster
            rasVals - unique raster values
            cols - list of column names
    Return: result - df of keys and tab area
    """
    cpus_minus_1 = mp.cpu_count() - 1
    if cpus_minus_1 == 0:
        cpus_minus_1 = 1

    batch_size = len(shapes) / cpus_minus_1
    if batch_size % 1 != 0:
        batch_size = int((batch_size) + 1)
    else:
        batch_size = int(batch_size)

    chunk_iterator = []
    for i in range(cpus_minus_1): # was num_chunks
        mn, mx = i * batch_size, (i + 1) * batch_size
        keys = list(shapes)[mn:mx]
        geoms = list(shapes.values())[mn:mx]
        gdf_args = keys, geoms, rasPath, rasVals
        chunk_iterator.append(gdf_args)

    pool = mp.Pool(processes=cpus_minus_1)
    results = pool.map(tabulateArea, chunk_iterator) 
    pool.close()

    result = {} #create one dict from list of dicts
    for d in results:
        result.update(d)
    del results

    df = pd.DataFrame.from_dict(result, orient='index', columns=cols[1:len(cols)])
    df[cols[0]] = df.index

    return df

def tabulateArea(args):
    """
    Method: tabulateArea()
    Purpose: tabulate area for each unique class in the raster within each polygon. Store data in
            a dictionary.
    Params: args:
                shapes - dictionary of polygon geometries
                rasPath - path to raster to summarize
                rasVals - list of unique classes in the raster
                fileType - flag for if raster is local land use or other
    Returns: tabArea_dict - dictionary of tabulate area data for each polygon
    """
    tabArea_dict = {} #dict to store data in
    keys, geoms, rasPath, rasVals = args
    with rio.open(rasPath) as src:
        noData = src.nodatavals[0]
        if noData in rasVals: #ignore NoData
            rasVals.remove(noData)
        for s in range(len(keys)):
            vals = tabArea(src, geoms[s], noData, rasVals) # list of unique vals in raster
            tabArea_dict[keys[s]] = vals
    return tabArea_dict

def tabArea(src, geom, noData, rasVals):
    """
    Method: tabArea()
    Purpose: Mask the classed raster to the current catchment and calculate the cell count
            for each class. Organize the counts in a list that will align with the total 
            number of unique classes found in the whole raster.
    Params: src - Rasterio open DatasetReader object for the raster
            geom - fiona geometry of the catchment
            noData - raster noData value
            rasVals - list of all unique classes in the raster
    Returns: finVals - list of class pixel counts within the catchment
    """
    try:
        ary, t = rio.mask.mask(src, [geom], crop=True, all_touched=False) #mask by current catchment
        vals, counts = np.unique(ary, return_counts=True) 
        del ary
        del t
        vals = list(vals)
        counts = list(counts)
        if noData in vals:
            i = vals.index(noData)
            vals.remove(noData) 
            del counts[i]
        finVals = [0 for x in range(len(rasVals))] #empty list with same length as unique raster values
        if len(vals) > 0:
            for idx, v in enumerate(vals): #for each class found within the catchment
                if int(v) in rasVals:
                    finVals[rasVals.index(int(v))] = counts[idx] #set count of unique class in same order as the unique raster values
        return  finVals #returns list of counts
    except:
        return [0] * len(rasVals)

def getRenameDF(rasPath):
    """
    Method: getRenameDF()
    Purpose: Read the local LU raster attribute table and relate the raster value
             with its string name
    Params: rasPath - path to raster
    Returns: rel - df with Value and CBP_mask for local land use raster
    """
    fullPath = rasPath + '.vat.dbf'
    if os.path.isfile(fullPath):
        rel = gpd.read_file(fullPath)
        cols = list(rel)
        mask = "CBP_mask"
        if mask not in rel.columns:
            for col in rel.columns:
                if 'mask' in col.lower():
                    rel = rel.rename(columns={col: mask})
        try:
            rel = rel[['Value', mask]]
            rel.loc[:, mask] = rel[mask].str.lower()
            lus = list(rel[mask])
            if None in lus and 'no_luz' not in lus: # if there is a NA and no no_luz name, name it no_luz
                if lus.count(None) == 1:
                    rel[[mask]] = rel[[mask]].fillna(value='no_luz')
        except:
            print(mask, " column does not exist in raster attribute table")
            print("Columns: ", list(rel))
            rel = pd.DataFrame()

        return rel
    else:
        print("Local land use file does not have raster attribute table dbf")
        print("Expecting dbf: ", fullPath)
    
def checkPath(path, msg):
    if not os.path.exists(path):
        print(msg, " path does not exist")
        print(path)

def checkFile(path, msg):
    if not os.path.isfile(path):
        print(msg, " path does not exist")
        print(path)

def runTabulateArea(cf):
    folder = config.folder
    temp_folder = f'{folder}/{cf}/temp'
    cf_st = time.time()
    TA_dict = helpers.generate_TA_dict(cf) 

    # SID
    segsGDB = f'{folder}/{cf}/input/dataprep.gdb'
    segsLayer = 'segs_vectorized'
    # PID
    parcelsGDB = f'{folder}/{cf}/temp/temp_dataprep.gdb'
    parcelsLayer = 'parcels_vectorized'

    # verify paths
    checkPath(segsGDB, 'VECTORIZED SEGS gdb')
    checkPath(parcelsGDB, 'VECTORIZED PARCELS gdb')
    
    for i in ['PID', 'SID']: #for each ID type
        st = time.time()
        if i == 'SID':
            shapes, areaDF = readPolyData(segsGDB, segsLayer, i)
        elif i == 'PID':
            shapes, areaDF = readPolyData(parcelsGDB, parcelsLayer, i)
        helpers.etime(cf, f'Read in {i} polys', st)

        for ta in TA_dict:
            if i.lower() in ta: 
                cur_dict = TA_dict[ta]
                st = time.time()

                if 'luz' in ta: # local land use - verify raster is there and that rat exists
                    if not os.path.isfile(cur_dict['path']):
                        helpers.etime(cf, f'{ta} raster does not exist', st)
                        st = time.time()
                        continue
                    luz_rat_df = getRenameDF(cur_dict['path'])
                    cur_dict['vals'] = [x.lower() for x in cur_dict['vals']]
                    if len(luz_rat_df) == 0:
                        helpers.etime(cf, f'{ta} RAT does not exist', st)
                        st = time.time()
                        continue
                    if list( set(list(luz_rat_df['CBP_mask'])) & set(cur_dict['vals'])) == 0: # no overlap in naming schemes
                        helpers.etime(cf, f'LUZ raster attribute table does not contain any of the LUZ values', st)
                        st = time.time()
                        print("LUZ Values: ", cur_dict['vals'])
                        print("RAT Values: ", list(luz_rat_df['CBP_mask']))
                        continue
                    finVals, vals = [], []
                    for lu in cur_dict['vals']:
                        if lu in list(luz_rat_df['CBP_mask']):
                            vals.append(int(list(luz_rat_df[luz_rat_df['CBP_mask']==lu]['Value'])[0]))
                            finVals.append(lu)
                    cur_dict['vals'] = finVals

                    if len(vals) < len(luz_rat_df):
                        print("LUZ RAT contains more classes than in the LUZ Values list - running anyway")
                else:
                    vals = cur_dict['vals']
                
                # tabulate area for 1 raster by PID or SID
                cols_names = [cur_dict['colname']+str(j) for j in cur_dict['vals']]
                tmp = tabulateArea_mp(shapes, cur_dict['path'], vals, [i]+cols_names)
                helpers.etime(cf, f'Tabulated area for {ta}', st)
                st = time.time()

                if 'luz' in ta: # if luz has empty cols, delete them
                    tmp.loc[:, (tmp != 0).any(axis=0)]

                # merge ta for raster with df containing PID/SID and their area
                areaDF = areaDF.merge(tmp, on=i, how='outer') # should be same as inner, but outer to be safe
                del tmp

        areaDF.to_csv(f'{temp_folder}/{i}_ta.csv', index=False)
        helpers.etime(cf, f'Write {i} CSV Time', st)
        st = time.time()
    
    helpers.etime(cf, f'Total Tabulate Area Time', cf_st)
    st = time.time()

if __name__ == "__main__":
    cf_list = [
                'glou_51073'
    ]
    for cf in cf_list:
        runTabulateArea(cf)