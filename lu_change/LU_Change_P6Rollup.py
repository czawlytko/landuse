"""
Script: LU_Change_RollUp.py
Purpose: To create and roll up land use for T1 and T2 to Phase 6 classes and 
         assign unique land use change values to them.
Author: Sarah McDonald, Geographer, U.S. Geological Survey
Contact: smcdonald@chesapeakebay.net
python lu_change\vector_version\LU_Change_P6Rollup.py
"""
import os
import rasterio as rio
import numpy as np
import pandas as pd
import sys
import time
import rasterio.mask
import geopandas as gpd
from rasterio.windows import from_bounds
from rasterio.transform import Affine
from osgeo import gdal, osr, gdalconst
sys.path.insert(0,'..')

import luconfig
from helpers import etime

def maskRasterWithRaster(mask_tif, ras_tif):
    """
    Method: maskRasterWithRaster()
    Purpose: Use the bounds of both rasters to determine what the shared raster space is
             and read both arrays in masking by those bounds to ensure the arrays are the
             same size.
    Params: mask_tif - path to raster
            ras_tif - path to raster
    Returns: mask_arr - numpy array of masked data
             ras_arr - numpy array of masked data
             out_meta - metadata for masked raster
    Based on code from: Labeeb Ahmed, Geographer, USGS, lahmed@chesapeakebay.net
    """
    # open mask and lc rasters
    with rasterio.open(mask_tif, compress="lzw") as mask: 
        with rasterio.open(ras_tif, compress="lzw") as ras: 
            # construct window bounds to mask out nlcd
            left, bottom, right, top = mask.bounds
            left1, bottom1, right1, top1 = ras.bounds
            if left1 > left:
                left = left1
            if bottom1 > bottom:
                bottom = bottom1
            if right1 < right:
                right = right1
            if top1 < top:
                top = top1
            clip_window_ras = from_bounds(left, bottom, right, top, ras.transform)
            clip_window_mask = from_bounds(left, bottom, right, top, mask.transform)
            # read in arrays
            mask_arr = mask.read(1, window=clip_window_mask)
            ras_arr = ras.read(1, window=clip_window_ras)
            # copy metadata
            out_meta = mask.meta.copy()
            out_meta.update({"driver": "GTiff",
                                "height": ras_arr.shape[0],
                                "width": ras_arr.shape[1],
                                "transform":Affine(1.0, 0.0, left, 0.0, -1.0, top)})
    return mask_arr, ras_arr, out_meta

def getRollUp(val):
    """
    Method: getRollUp()
    Purpose: Return Phase 6 integer value for the Phase 6 class.
    Params: val - string name of Phase 6 class or 'ALL' if you want the whole dict returned
    Returns: int phase 6 class
    """
    rollup_dict = {
        'Impervious Roads' : 1,
        'Impervious Non-Roads' : 2,
        'Tree Canopy Over Impervious' : 3,
        'Turf Grass' : 4,
        'Tree Canopy over Turf Grass' : 5,
        'Forest' : 6,
        'Wetlands, Floodplain' : 7,
        'Wetlands, Other' : 8,
        'Wetlands, Tidal' : 9,
        'Mixed Open' : 10,
        'Cropland' : 11,
        'Pasture' : 12,
        'Water' : 13
    }
    if val == 'ALL':
        return rollup_dict
    elif val in rollup_dict:
        return rollup_dict[val]
    else:
        print("ERROR: ", val, " not in roll_up dict; returning -1")
        return -1

def getRollUp3Letters(val):
    """
    Method: getRollUp3Letters()
    Purpose: Use dict to rename P6 classes to the abbreviations.
    Params: val - name of p6 class
    Returns: abbreviation of class
    """
    rollup_dict = {
        'Impervious Roads' : 'IR',
        'Impervious Non-Roads' : 'INR',
        'Tree Canopy Over Impervious' : 'TCI',
        'Turf Grass' : 'TG',
        'Tree Canopy over Turf Grass' : 'TCT',
        'Forest' : 'FORE',
        'Wetlands, Floodplain' : 'WLF',
        'Wetlands, Other' : 'WLO',
        'Wetlands, Tidal' : 'WLT',
        'Mixed Open' : 'MO',
        'Cropland' : 'CRP',
        'Pasture' : 'PAS',
        'Water' : 'WAT'
    }
    if val == 'ALL':
        return rollup_dict
    elif val in rollup_dict:
        return rollup_dict[val]
    else:
        print(val, ' not in rollup_dict; returning empty string')
        return ''

def get_lu_code(description, getKey):
    """
    Method: get_lu_code()
    Purpose: Reference dictionary of full lu classes to get their lu code value.
    Params: description - name of LU class
            getKey - boolean; False if passing description to get value, True if passing value to get description
    Returns: LU code value based on description
    """
    lu_code_dict = luconfig.lu_code_dict
    
    if description == 'ALL':
        return lu_code_dict
    if getKey: # need key from value
        for lu in lu_code_dict:
            if lu_code_dict[lu] == description:
                return lu
        return -1
        print("\nERROR: ", description, " not in lu_code_dict; returning -1\n")
    elif description in lu_code_dict:
        return lu_code_dict[description]
    else:
        print("\nERROR: ", description, " not in lu_code_dict; returning -1\n")
        return -1

def reclassRas(ary, rollUpdf):
    """
    Method: reclassRas()
    Purpose: Reclass array from full LU classes to rolled up classes
    Params: ary - array to reclass to rolled up classes
            rollUpdf - dataframe relating rolled up class names and full lu class names
    Returns: new_ary - array with phase 6 class values
    """
    ary = np.where(ary == 255, 0, ary)
    new_ary = np.zeros_like(ary) #new array of same shape as ary filled with 0s
    for rollup in list(set(list(rollUpdf['P6_LU']))):
        old_lus = list(rollUpdf[rollUpdf['P6_LU'] == rollup]['LU'])
        old_lus = [get_lu_code(l, False) for l in old_lus] #list of lu values
        new_lu_code = getRollUp(rollup)
        new_ary = np.where(np.isin(ary, old_lus), new_lu_code, new_ary)
    all_lus = set(list(np.unique(new_ary)))
    all_rollup = set(list(getRollUp('ALL').values()))
    missing = list(all_lus - all_rollup)
    if 0 in missing:
        missing.remove(0)
    if len(missing) > 0:
        print("LU Classes not rolled up: ", missing)
        print("Add to roll up csv")
        sys.exit(1)
    return new_ary

def createLUChange(t1_ary, t2_ary):
    """
    Method: createLUChange()
    Purpose: Create unique 4 digit codes for lu change, where first 2 digits are T1 rolled up LU
             and last 2 digits are T2 rolled up LU
    Params: t1_ary - array of rolled up T1 LU
            t2_ary - array of rolled up T2 LU
    Returns: array of LU change
    """
    t1_ary = np.where((t1_ary != t2_ary)&(t2_ary > 0), t1_ary, 0) #reduce to where there is change
    t2_ary = np.where(t1_ary > 0, t2_ary, 0) #reduce to where there is change
    t1_ary = t1_ary * 100
    t1_ary = t1_ary + t2_ary

    # any transition to wetland that is not a developed class gets nulled out as no change
    t1_ignore = [
                getRollUp('Tree Canopy over Turf Grass'),
                getRollUp('Forest'),
                getRollUp('Mixed Open'),
                getRollUp('Cropland'),
                getRollUp('Pasture')
    ]
    t2_ignore = [
                getRollUp('Wetlands, Floodplain'),
                getRollUp('Wetlands, Other'),
                getRollUp('Wetlands, Tidal')
    ]
    ignore_vals = []
    for t1 in t1_ignore: # something to wetland
        for t2 in t2_ignore:
            val = t1 * 100 + t2
            ignore_vals.append(val)
    for t2 in t1_ignore: # wetland to something
        for t1 in t2_ignore:
            val = t1 * 100 + t2
            ignore_vals.append(val)
    for t2 in t2_ignore: # wetland to wetland
        for t1 in t2_ignore:
            val = t1 * 100 + t2
            ignore_vals.append(val)
    t1_ary = np.where(np.isin(t1_ary, ignore_vals), 0, t1_ary)

    return t1_ary

def createPrimaryRaster(OUTPUT_DIR, df, vals, counts, ras, rio_meta):
    """
    Method: createPrimaryRaster()
    Purpose: Create raster attribute table and color map and assign it to raster band.
             Can I just write this out as .vat instead of re-writing the tiff?
    Params: OUTPUT_DIR - path to write results to
            df - dataframe of Change Values and Change descriptions
            vals - list of unique raster values
            counts - list of raster value counts
            ras - numpy array of raster data
            rio_meta - metadata from rasterio
    Returns: N/A
    """
    trans_rio = list(rio_meta['transform'])
    trans = (trans_rio[2], trans_rio[0], trans_rio[1], trans_rio[5], trans_rio[3], trans_rio[4])

    d = gdal.GetDriverByName('GTIFF')
    myRas = d.Create(OUTPUT_DIR, ras.shape[1], ras.shape[0], 1, gdal.GDT_UInt16, options=['COMPRESS=LZW'])
    myRas.SetGeoTransform(trans) 
    myRas.SetProjection(rio_meta['crs'].to_wkt())
    band = myRas.GetRasterBand(1)
    band = createRAT(band, df, vals, counts)
    band.SetNoDataValue(0)
    band.WriteArray(ras)
    myRas.FlushCache()

def createRAT(band, df, vals, counts):
    """
    Method: createRAT()
    Purpose: Create raster attribute table and color map and assign it to raster band.
             Can I just write this out as .vat instead of re-writing the tiff?
    Params: band - gdal band of raster to add RAT
            df - dataframe of Change Values and Change descriptions
            vals - list of unique raster values
            counts - list of raster value counts
    Returns: band - raster band with updated RAT
    """
    rat = gdal.RasterAttributeTable()
    rat.CreateColumn("Value", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    rat.CreateColumn("Count", gdalconst.GFT_Integer, gdalconst.GFU_PixelCount)
    rat.CreateColumn("Red", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    rat.CreateColumn("Green", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    rat.CreateColumn("Blue", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    rat.CreateColumn("Change", gdalconst.GFT_String, gdalconst.GFU_Name)

    # populate the columns
    ct = 0
    # Peter's colors
    rgb_dict = { 
        1: (0, 0, 0), # IR - black,
        2: (255, 0, 0), # INR - red,
        3: (64, 232, 35), # TCI - light green,
        4: (243, 247, 12), # TG - yellow,
        5: (64, 232, 35), # TCT - light green,
        6: (4, 117, 4), # FORE - dark green,
        7: (4, 194, 137), # WLF - blue green,
        8: (4, 194, 137), # WLO - blue green,
        9: (56, 245, 186), # WLT - aquamarine (lighter and more vibrant than blue green),
        10: (135, 105, 5), # MO - brown,
        11: (245, 118, 7), # CRP - blood orange,
        12: (207, 186, 126), # PAS - beige
        13: (2, 58, 189), # WAT - deep sky blue,
    }

    for idx, row in df.iterrows():
        if row['Value'] in vals:
            # print(ct, int(row['Value']), 1, row['Change'])
            rat.SetValueAsInt(ct, 0, int(row['Value']))
            rat.SetValueAsInt(ct, 1, int(counts[vals.index(row['Value'])]))
            t2_val = int(str(row['Value'])[-2:])
            rgb_tup = rgb_dict[t2_val]
            rat.SetValueAsInt(ct, 2, int(rgb_tup[0]))
            rat.SetValueAsInt(ct, 3, int(rgb_tup[1]))
            rat.SetValueAsInt(ct, 4, int(rgb_tup[2]))
            rat.SetValueAsString(ct, 5, row['Change'])
            ct += 1

    # set the default Raster Attribute Table for src_ds band 1 to the newly modified rat
    band.SetDefaultRAT(rat)
    return band

def createTable(vals, counts):
    """
    Method: createTable()
    Purpose: Create summary pivot table of acreages of change between Phase 6 classe.
    Params: vals - list of unique raster values
            counts - list of raster value counts
    Returns: df - summary pivot table
    """
    # Create Empty Table
    allP6 = list(getRollUp('ALL'))
    df = pd.DataFrame(columns=allP6)
    rows = list(getRollUp3Letters('ALL').values()) + ['Gain', 'Total', 'TotGain', 'TotLoss', 'Net']
    df['T1-T2 LU'] = rows #allP6
    df = df.set_index(df['T1-T2 LU']) 
    df = df[allP6] # Table with rows and columns named after P6 LUs
    # For each possibility - enter total acres in table
    for t1 in allP6:
        t1_val = getRollUp(t1)
        for t2 in allP6:
            t2_val = getRollUp(t2)
            chg = (t1_val * 100) + t2_val
            idx = getRollUp3Letters(t1)
            if chg in vals:
                df.loc[idx, t2] = counts[vals.index(chg)] / 4047
            else:
                df.loc[idx, t2] = 0
    df.loc['Gain'] = df.sum(axis=0)
    df['Loss'] = df.sum(axis=1)
    df['Gain'] = list(df.iloc[13])[0:13] + [np.nan,np.nan,np.nan,np.nan,np.nan]
    df['Net'] = df['Gain'] - df['Loss']
    df.loc['Net'] = list(df['Net'])[0:13] + [np.nan, np.nan, np.nan]
    df.loc['TotLoss'] = list(df['Loss'])[0:13] + [np.nan, np.nan, np.nan]
    df.loc['Total'] = [np.nan for i in range(16)]
    df.loc['TotGain'] = list(df.iloc[13])
    df.loc['TotGain', 'Loss'] = np.nan
    df = df[allP6+['Loss']] 
    for c in list(df): #round to 2 decimal places
        df[c] = df[c].astype(float).round(2)
        if c != 'Loss':
            newC = getRollUp3Letters(c)
            df = df.rename(columns={c:newC}) # rename columns
    df = df[list(getRollUp3Letters('ALL').values())+['Loss']]
    return df

def run_p6_rollup_change(cf, lu_type):
    """
    Method: run_p6_rollup_change()
    Purpose: Call functions to roll up T1 and T2 LU rasters to P6 classes,
             create LU change raster with RAT and color map, and create and
             write summary pivot table.
    Params: cf - first 4 letters of county + fips code
            lu_type - extension of naming scheme used in T1 LU raster
            anci_folder - path to ancillary folder
            mainPath - path to folder containing T1 LU and destination to write results
    Returns: N/A
    """
    anci_folder = luconfig.anci_folder
    folder = luconfig.folder
    startTime = time.time()
    rollupPath = luconfig.rollupPath
    rollUp2Path = luconfig.rollUp2Path
    
    print(rollUpPath, rollUp2Path)

    t1_path = f"{folder}/{cf}/temp/{cf}_T1_LU_{lu_type}.tif"
    t2_path = f'{folder}/{cf}/output/{cf}_lu_2017_2018.tif'
    # Create dataframe relating LU classes to their Roll up class
    rollUpDf = pd.read_csv(rollUpPath)
    etime(cf, 'Read roll up DF', startTime)
    st = time.time()

    rollUpDf2 = pd.read_csv(rollUp2Path)
    etime(cf, 'Read roll up 2 DF', startTime)
    st = time.time()

    # Read in T1 and T2 LU rasters and their metadata
    t1_ary, t2_ary, t2_meta = maskRasterWithRaster(t1_path, t2_path)
    if t1_ary.shape != t2_ary.shape:
        print("Shapes still not equal after masking ras with ras")
        print("t1: ", t1_ary.shape)
        print("t2: ", t2_ary.shape)
        sys.exit(1)
    etime(cf, 'Read T1 and T2 LU rasters', st)
    st = time.time()

    # Reclass T1 and T2 to rolled up classes
    t1_ary = reclassRas(t1_ary, rollUpDf)
    t2_ary = reclassRas(t2_ary, rollUpDf)
    etime(cf, 'Reclassed T1 and T2 LU to rollup clases', st)
    st = time.time()

    # Create LU change using rolled up T1 and T2 classes
    lu_change_ary = createLUChange(t1_ary, t2_ary)
    del t1_ary
    del t2_ary
    etime(cf, 'Created LU change array', st)
    st = time.time()

    # Get unique vals and counts
    vals, counts = np.unique(lu_change_ary, return_counts=True)
    vals = list(vals)
    counts = list(counts)

    # # Write out LU change
    outras_path = f"{folder}/{cf}/output/{cf}_P6LU_Change_{lu_type}.tif"
    createPrimaryRaster(outras_path, rollUpDf2, vals, counts, lu_change_ary, t2_meta)
    etime(cf, 'Wrote LU Change Raster', st)
    st = time.time()

    # Create table of summarized change
    summary_df = createTable(vals, counts)
    summary_df.to_csv(f"{folder}/{cf}/output/{cf}_P6LU_Change_{lu_type}_summary.csv", index=True)
    etime(cf, 'Create and wrote tabular summary', st)
    st = time.time()

    etime(cf, 'Total Run', startTime)
    st = time.time()
