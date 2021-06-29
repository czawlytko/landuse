#burn_in_callable
#python B:\landuse_dev\old\burn_in_master.py

"""
Emily Mills 5/4/2021
burn in master script callable from main land use script.
preps/rasterizes outputs of TC module and wetlands module
rasterizes output of LU ***with dictionary applied to change to 4 digit land use codes!!!
performs burn in of impervious surfaces, TC over, wetlands classes.
"""

import sys
from pathlib import Path
import geopandas as gpd
import rasterio
from rasterio import features
from rasterio.mask import mask
from rasterio.enums import Resampling
from rasterio import Affine
from rasterio.windows import Window
from rasterio.features import rasterize, shapes
import concurrent.futures
import fiona
import time
import numpy as np
import os
import multiprocessing as mp 
import threading
from shapely.geometry import box, mapping, Polygon
import shapely
from fiona.crs import from_epsg
import pandas as pd
from osgeo import gdal, osr, gdalconst
from scipy.ndimage import label

from helpers import etime
#####################################################################################
#------------------------------- MAIN ------------- --------------------------------#
#####################################################################################
def run_burnin_submodule(proj_folder, anci_folder, cf):
    """
    Method: run_burnin_submodule()
    Purpose: This is the method to be called from the LU model main script. This method
             calls all functions needs to do the final lu burn in. If this function fails,
             the exception is caught and it will not kill the lu model run.
    Parms: cf - the county fips string; first four letters of the county name, underscore, county fips
    Returns: Flag for if module ran properly;
                0 - module ran and created final burn in lu with symbology and RAT applied and pyramids built.
                -1 - exception was thrown and data may not have been created
    """
    # run a check to ensure trees_over.gpkg exists?

    tidal_lookup = f'{anci_folder}/wetlands/tidal_lookup.csv'
    slr_ras = f'{anci_folder}/wetlands/SLR_1.tif'
    symb_table= f'{anci_folder}/land_use_color_table_20210503.csv'
    county_shp = f'{anci_folder}/census/BayCounties20m_project.shp'

    ##########build file paths############
    rail_path= f'{anci_folder}/rail/rail_baywide.tif'

    lc_folder= Path(f'{proj_folder}/{cf}/input')
    og_lc_path= list(lc_folder.rglob(fr'*_landcover_*.tif'))[0]
    
    #masked lc path
    lc_path= os.path.splitext(og_lc_path)[0] + '_mask.tif'

    tc_path= f'{proj_folder}/{cf}/output/trees_over.gpkg'
    
    tc_composite_path= f'{proj_folder}/{cf}/output/tc_composite.tif'

    pond_path= f'{proj_folder}/{cf}/input/wetlands/ponds.gpkg' #file structure change 06-09-21
    pond_ras_path= f'{proj_folder}/{cf}/input/wetlands/ponds.tif' #file structure change 06-09-21

    nontidal_path = f'{proj_folder}/{cf}/input/wetlands/nontidal_wetlands.gpkg' #file structure change 06-09-21
    nontidal_ras_path = f'{proj_folder}/{cf}/input/wetlands/nontidal_wetlands.tif' #file structure change 06-09-21

    tidal_path= f'{proj_folder}/{cf}/input/wetlands/nwi_tidal_overlay.gpkg' #file structure change 06-09-21
    tidal_ras_path= f'{proj_folder}/{cf}/input/wetlands/nwi_tidal_overlay.tif' #file structure change 06-09-21

    slr_clip = f'{proj_folder}/{cf}/input/wetlands/slr_clip.tif' #file structure change 06-09-21
    tidal_composite_path =f'{proj_folder}/{cf}/input/wetlands/tidal_composite.tif' #file structure change 06-09-21

    lu_path = f'{proj_folder}/{cf}/output/data.gpkg'
    lu_ras_path = f'{proj_folder}/{cf}/output/lu_ras.tif'

    out_burnin_path = f'{proj_folder}/{cf}/output/{cf}_burnin.tif'
    out_pre_fixforest_path = f'{proj_folder}/{cf}/output/{cf}_burnin_pre_fixforest.tif'
    out_lu_burnin_path = f'{proj_folder}/{cf}/output/{cf}_lu_2017_2018.tif'
    

    print("\n*******************************")
    print(  "********* BURN IN *************")
    print(  "*******************************\n")

    ###############do stuff###############

    start = time.time()

    #mask land cover to correct extent using 20m buffered county polygon
    if not os.path.exists(lc_path):
        clipRasByGeom(county_shp, cf, og_lc_path, lc_path)
    etime(cf, "LC masked by county boundary", start)
    st = time.time()

    #prep tct
    if not os.path.exists(tc_composite_path):
        prepTCT(lc_path, tc_path, proj_folder, cf, 'lu_code')
    etime(cf, "TCT layers rasterized", st)
    st = time.time()

    if not os.path.exists(tc_composite_path):
        createTCComposite(proj_folder, cf, tc_composite_path)
    etime(cf, "TC composite created", st)
    st = time.time()

    #prep ponds
    if os.path.exists(pond_path):
        if not os.path.exists(pond_ras_path):
            prepPonds(lc_path, pond_path, pond_ras_path, 'pond')
    etime(cf, "Ponds rasterized", st)
    st = time.time()

    #prep wetlands, whether nontidal or tidal
    gdf=gpd.read_file(tidal_lookup)
    # if intersect tagged a county as tidal but the tidal gpkg does not exist - set as nontidal
    if int(gdf.loc[gdf['cf'] == cf]['tidal']) == 1 and not os.path.isfile(tidal_path):
        gdf.loc[gdf['cf'] == cf, 'tidal'] = 0
        etime(cf, "tagged as tidal but no gpkg exists - treating as nontidal", st)
        st = time.time()

    if int(gdf.loc[gdf['cf'] == cf]['tidal']) == 0:
        print(cf, " is a nontidal county. Running nontidal prep only")
        if not os.path.exists(nontidal_ras_path):
            prepNontidalWetlands(lc_path, nontidal_path, nontidal_ras_path, 'w_type_code')
        etime(cf, "Nontidal Wetlands rasterized", st)
        st = time.time()
        
    else:
        print(cf, " is a tidal county. Running nontidal and tidal prep")
        if not os.path.exists(nontidal_ras_path):
            prepNontidalWetlands(lc_path, nontidal_path, nontidal_ras_path, 'w_type_code')
        etime(cf, "Nontidal Wetlands rasterized", st)
        st = time.time()

        if not os.path.exists(tidal_ras_path):
            prepTidalWetlands(lc_path, tidal_path, tidal_ras_path, 'w_type_code')
        etime(cf, "Tidal Wetlands rasterized", st)
        st = time.time()

        if not os.path.exists(tidal_composite_path):
            createTidalComposite(tidal_ras_path, slr_ras, slr_clip, tidal_composite_path)
        etime(cf, "Tidal Wetlands composite raster created", st)
        st = time.time()

    # rasterize LU
    if not os.path.exists(lu_ras_path):
        rasterizeLU(lc_path, lu_path, lu_ras_path, 'lu_code')
    etime(cf, "Land use rasterized", st)
    st = time.time()

    # run burn ins
    clip_dict = {
        'rail': [rail_path, 'uint8'],
        'nontidal': [nontidal_ras_path, 'uint16'],
        'lc': [lc_path, 'uint8'],
        'tc': [tc_composite_path, 'uint16'],
        'ponds': [pond_ras_path, 'uint16'], 
        'tidal': [tidal_composite_path, 'uint8']
        }
    if int(gdf.loc[gdf['cf'] == cf]['tidal']) ==0:
        clip_dict.popitem()
    

    for key in clip_dict:
        if os.path.isfile(clip_dict[key][0]):
            in_clip=clip_dict[key][0]
            out_clip= fr'{proj_folder}/{cf}/output/{cf}_{key}_clip.tif'
            clip_dict[key].append(out_clip)
            dtype= (clip_dict[key][1])
            
            clip_to_lu(lu_ras_path, key, in_clip, out_clip, dtype)
        else:
            etime(cf, f'{key} tif does not exist - not clipping', st)

    etime(cf, "Clips done", st)
    st = time.time()

    #make burn raster

    if int(gdf.loc[gdf['cf'] == cf]['tidal']) ==0:
        rail_clip= clip_dict['rail'][2]
        nontidal_clip=clip_dict['nontidal'][2]
        lc_clip= clip_dict['lc'][2]
        tc_clip= clip_dict['tc'][2]
        pond_clip = clip_dict['ponds'][2]

        if not os.path.exists(out_burnin_path):
            reclassBurnInValueNontidal(lu_ras_path, out_burnin_path, lc_clip, rail_clip, nontidal_clip, tc_clip, pond_clip)
    
    else:
        rail_clip= clip_dict['rail'][2]
        nontidal_clip=clip_dict['nontidal'][2]
        lc_clip= clip_dict['lc'][2]
        tc_clip= clip_dict['tc'][2]
        tidal_clip= clip_dict['tidal'][2]
        pond_clip= clip_dict['ponds'][2]

        if not os.path.exists(out_burnin_path):
            reclassBurnInValueTidal(lu_ras_path, out_burnin_path, lc_clip, rail_clip, nontidal_clip, tc_clip, tidal_clip, pond_clip)

    etime(cf, "Reclass burn done", st)
    st = time.time()


    #create final burn in array and pass to add symbology function to write
    # dst_array, burnin_meta = reclassBurnFinalStep(lu_ras_path, out_burnin_path, out_pre_fixforest_path, lc_clip)
    if not os.path.exists(out_pre_fixforest_path):
        reclassBurnFinalStep(lu_ras_path, out_burnin_path, out_pre_fixforest_path, lc_clip)
    etime(cf, "Burn in done", st)
    st = time.time()

    # fix forest pixels
    with rasterio.open(out_pre_fixforest_path, 'r') as burnin_src:
        burnin_meta = burnin_src.meta
        dst_array= burnin_src.read(1)
        tmp, fixForestFlag = fixForest(dst_array, burnin_meta['transform'])
        if fixForestFlag: # only update if there is data to update
            dst_array = np.where((tmp > 0)&(dst_array==3100), tmp, dst_array) # update your burn in array
        del tmp
    etime(cf, "Fix forest pixels done", st)
    st = time.time()

    
    #add symbology and write out final raster
    df = pd.read_csv(symb_table)
    df = df[['Value', 'LandUse', 'Red', 'Green', 'Blue']]
    addSymbologyandRAT(out_lu_burnin_path, df, dst_array, burnin_meta) #change metadata to clipped?

    #create Pyramids on final output raster
    createPyramids(out_lu_burnin_path)
    etime(cf, "Pyramids built, symbology and attribute table added", st)
    st = time.time()



    #delete clips and intermediates
    if os.path.exists(rail_clip):
        os.remove(rail_clip)
    if os.path.exists(nontidal_clip):
        os.remove(nontidal_clip)
    if os.path.exists(lc_clip):
        os.remove(lc_clip)
    if os.path.exists(tc_clip):
        os.remove(tc_clip)
    #if os.path.exists(tidal_clip):
     #   os.remove(tidal_clip)
    if os.path.exists(slr_clip):
        os.remove(slr_clip)
    if os.path.exists(pond_clip):
        os.remove(pond_clip)
    if int(gdf.loc[gdf['cf'] == cf]['tidal']) ==1:
        if os.path.exists(tidal_clip):
            os.remove(tidal_clip)
    

    etime(cf, "Total Run ", start)

#########################################################################################
####################################PREP FUNCTIONS#######################################
#########################################################################################


def prepTCT(lc_path, tc_path, proj_folder, cf, field_name):
    fn_ras = lc_path
    fn_vec = tc_path
    field_name=field_name

    layer_list = ['tct', 'tct_bufs', 'toa'] #, 'tct', 'tct_bufs', 'toa']
    for layer in layer_list:
        vec_ds = gpd.read_file(fn_vec, layer=layer)
        out_ras = f'{proj_folder}/{cf}/output/{layer}.tif'
        print(out_ras)

        if not os.path.exists(out_ras):
            rst = rasterio.open(fn_ras)
            meta = rst.meta.copy()
            meta.update(compress='lzw', dtype='uint16')

            with rasterio.open(out_ras, 'w+', **meta) as out:
                out_arr = out.read(1)

                # this is where we create a generator of geom, value pairs to use in rasterizing
                shapes = ((geom,value) for geom, value in zip(vec_ds.geometry, vec_ds[field_name]))

                tc_ras = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=out.transform)
                out.write_band(1, tc_ras)
            out.close()
            print(layer, " psegs with tc rasterized")


        # factors = [2, 4, 8, 16, 32, 64, 128, 256, 512]
        # with rasterio.open(out_ras, 'r+') as dst:
        #     dst.build_overviews(factors, Resampling.nearest)
        #     dst.close()
        # print(layer, " pyramids built")

def createTCComposite(proj_folder, cf, tc_composite_path):
    #make single tc raster

    tct_in = fr'{proj_folder}/{cf}/output/tct.tif'
    toa_in= fr'{proj_folder}/{cf}/output/toa.tif'
    tct_bufs = fr'{proj_folder}/{cf}/output/tct_bufs.tif'
    out_burnin= tc_composite_path

    with rasterio.open(tct_in, 'r') as tct_src:
        with rasterio.open(toa_in, 'r') as toa_src:
            with rasterio.open(tct_bufs, 'r') as tct_bufs_src:
                tct_clip_meta = tct_src.meta.copy()
                tct_clip_meta.update(compress='lzw', dtype='uint16')

                with rasterio.open(out_burnin, 'w', **tct_clip_meta) as dst:
                    tct_array = tct_src.read(1)
                    toa_array = toa_src.read(1)
                    tct_bufs_array= tct_bufs_src.read(1) 

                    #combine rasters to make tc raster
                    dst_array= np.where ( (tct_bufs_array== 2240), 2240, 0)
                    dst_array[np.where (tct_array==2240) ] = 2240
                    dst_array[np.where (toa_array==3200) ] = 3200
                    
                    dst_array = dst_array.astype("uint16")
                    dst.write(dst_array, 1)

    os.remove(tct_in)
    os.remove(toa_in)
    os.remove(tct_bufs)

def prepPonds(lc_path, pond_path, pond_ras_path, field_name):
    fn_ras = lc_path
    fn_vec = pond_path
    out_ras = pond_ras_path
    vec_ds = gpd.read_file(fn_vec, layer='ponds')
    field_name=field_name

    #rasval_code = 'w_type_code'

    #ponds
    vec_ds.loc[vec_ds[field_name] == 1] #select only the 1s
    #vec_ds[field_name] = 1
    
    vec_ds[field_name] = vec_ds[field_name].astype('int16')


    rst = rasterio.open(fn_ras)
    meta = rst.meta.copy()
    meta.update(compress='lzw')
    meta.update({"dtype": "uint16"})

    with rasterio.open(out_ras, 'w+', **meta) as out:
        out_arr = out.read(1)

        # this is where we create a generator of geom, value pairs to use in rasterizing
        shapes = ((geom, value) for geom, value in zip(vec_ds.geometry, vec_ds[field_name]))
        pond_ras = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=out.transform)
        
        out.write_band(1, pond_ras)
    out.close()
    print("ponds rasterized")

def prepNontidalWetlands(lc_path, nontidal_path, nontidal_ras_path, field_name):
    fn_ras = lc_path
    fn_vec = nontidal_path
    out_ras = nontidal_ras_path
    vec_ds = gpd.read_file(fn_vec, layer='nontidal_wetlands') #nwi_tidal_overlay
    field_name=field_name

    #rasval_code = 'w_type_code'

    #nontidal
    def wtype(x):
        if x == "riverine":
            return 52
        else:
            return 53
    vec_ds[field_name] = vec_ds['w_type'].map(wtype)
    vec_ds[field_name] = vec_ds[field_name].astype('int16')


    rst = rasterio.open(fn_ras)
    meta = rst.meta.copy()
    meta.update(compress='lzw')
    meta.update({"dtype": "uint16"})

    with rasterio.open(out_ras, 'w+', **meta) as out:
        out_arr = out.read(1)

        # this is where we create a generator of geom, value pairs to use in rasterizing
        shapes = ((geom, value) for geom, value in zip(vec_ds.geometry, vec_ds[field_name]))
        lu_ras = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=out.transform)
        
        out.write_band(1, lu_ras)
    out.close()
    print("nontidal wetlands rasterized")
    
    # factors = [2, 4, 8, 16, 32, 64, 128, 256, 512]
    # with rasterio.open(out_ras, 'r+') as dst:
    #     dst.build_overviews(factors, Resampling.nearest)
    #     dst.close()
    # print("pyramids built")

def prepTidalWetlands(lc_path, tidal_path, tidal_ras_path, field_name):
    fn_ras = lc_path
    fn_vec = tidal_path
    out_ras = tidal_ras_path
    vec_ds = gpd.read_file(fn_vec, layer='nwi_tidal_overlay')
    field_name=field_name

    #rasval_code = 'w_type_code'

    #tidal
    vec_ds[field_name] = 2
    
    vec_ds[field_name] = vec_ds[field_name].astype('int16')


    rst = rasterio.open(fn_ras)
    meta = rst.meta.copy()
    meta.update(compress='lzw')
    meta.update({"dtype": "uint16"})

    with rasterio.open(out_ras, 'w+', **meta) as out:
        out_arr = out.read(1)

        # this is where we create a generator of geom, value pairs to use in rasterizing
        shapes = ((geom, value) for geom, value in zip(vec_ds.geometry, vec_ds[field_name]))
        lu_ras = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=out.transform)
        
        out.write_band(1, lu_ras)
    out.close()
    print("tidal wetlands rasterized")
    
    # factors = [2, 4, 8, 16, 32, 64, 128, 256, 512]
    # with rasterio.open(out_ras, 'r+') as dst:
    #     dst.build_overviews(factors, Resampling.nearest)
    #     dst.close()
    # print("pyramids built")

def createTidalComposite(tidal_ras_path, slr_ras, slr_clip, tidal_composite_path):
    #clip SLR_1 baywide raster (1)to same extent as tidal raster (2)
    tidal_ras = tidal_ras_path
    slr_ras= slr_ras
    out_slr_clip = slr_clip
    out_tidal= tidal_composite_path

    with rasterio.open(tidal_ras, 'r') as tidal_src:
        with rasterio.open(slr_ras, 'r') as slr_src: #,**lc_src.profile
            bounds = tidal_src.bounds
            bounds_geom = box(*bounds)

            geo = gpd.GeoDataFrame({'geometry': bounds_geom}, index=[0], crs= tidal_src.crs)
            coords = getFeatures(geo)
            
            slr_clip, slr_transform = rasterio.mask.mask(slr_src, shapes=coords, filled=True, crop=True) #try crop = false
            slr_clip_meta = tidal_src.meta.copy()
            #epsg_code = 4269
            slr_clip_meta.update({"driver": "GTiff", "dtype": "uint8", "height": slr_clip.shape[1], "width": slr_clip.shape[2], "transform": slr_transform}) #or don't change shape to image, keep it as lc 
            
            with rasterio.open(out_slr_clip, 'w', **slr_clip_meta) as dst:
                dst.write(slr_clip)
                dst.close()

    #read in tidal_ras and slr_clip
    with rasterio.open(tidal_ras, 'r') as tidal_src:
        with rasterio.open(out_slr_clip, 'r') as slr_src:
            tidal_meta = tidal_src.meta.copy()
            tidal_meta.update({"dtype": "uint8"})

            with rasterio.open(out_tidal, 'w', **tidal_meta) as dst:
                #do band math with numpy arrays
                tidal_array = tidal_src.read(1)
                slr_array = slr_src.read(1)

                #if SLR is null:
                    #and tidal is null-->null
                    #and tidal is 2 --> 2
                #if SLR is not null
                    #and tidal is null-->1
                    #and tidal is 2 --> 3

                dst_array= np.where( (np.isnan(slr_array) & (tidal_array == 2)), 2, 0) 
                dst_array[np.where ( ((slr_array==1) & (np.isnan(slr_array))) )] = 1 
                dst_array[np.where ( ((slr_array==1) & (tidal_array==2)) )] = 3 

                #write out composite raster
                dst_array = dst_array.astype("uint8")
                dst.write(dst_array, 1)

    

def rasterizeLU(lc_path, lu_path, lu_ras_path, field_name):
    fn_ras = lc_path
    fn_vec = lu_path
    out_ras = lu_ras_path

    vec_ds = gpd.read_file(fn_vec, layer='psegs_lu')
    field_name=field_name

    rst = rasterio.open(fn_ras)
    meta = rst.meta.copy()
    meta.update(compress='lzw', dtype='uint16')

    with rasterio.open(out_ras, 'w+', **meta) as out:
        out_arr = out.read(1)

        # this is where we create a generator of geom, value pairs to use in rasterizing
        shapes = ((geom,value) for geom, value in zip(vec_ds.geometry, vec_ds[field_name])) #change field name here # lu_code

        lu_ras = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=out.transform)
        out.write_band(1, lu_ras)
    out.close()
    print("psegs with lu rasterized")


    factors = [2, 4, 8, 16, 32, 64, 128, 256, 512]
    with rasterio.open(out_ras, 'r+') as dst:
        dst.build_overviews(factors, Resampling.nearest)
        dst.close()
    print("pyramids built")


    ds = gdal.Open(out_ras)
    rb = ds.GetRasterBand(1)

    # Get unique values in the band
    u = np.unique(rb.ReadAsArray())
    u = u.tolist()
    print(u)

    # https://chrisalbon.com/python/data_wrangling/pandas_list_unique_values_in_column/
    # extract list of unique 'landuse' values
    landuse = vec_ds.lu.unique()
    print(landuse)

    # Create and populate the RAT
    rat = gdal.RasterAttributeTable()

    rat.CreateColumn('VALUE', gdal.GFT_Integer, gdal.GFU_Generic)
    rat.CreateColumn('LANDUSE', gdal.GFT_String, gdal.GFU_Generic)

    for i in range(len(u)-1):
        rat.SetValueAsInt(i, 0, u[i])
        rat.SetValueAsString(i, 1, landuse[i])

    # Associate with the band
    rb.SetDefaultRAT(rat)

    # Close the dataset and persist the RAT
    ds = None
    print("attribute table created")

#########################################################################################
####################################BURN IN FUNCTIONS####################################
#########################################################################################
def clip_to_lu(lu_ras_path, key, in_ras, out_ras, dtype):
    lu_ras = lu_ras_path
    if os.path.exists(out_ras): 
        print ("clip already exists. skipping: ", key)
    
    else:
        with rasterio.open(in_ras, 'r') as clip_src:
            with rasterio.open(lu_ras, 'r') as lu_src: #,**lc_src.profile
                new_bounds = lu_src.bounds
                new_bounds_geom = box(*new_bounds)

                geo = gpd.GeoDataFrame({'geometry': new_bounds_geom}, index=[0], crs=clip_src.crs)
                coords = getFeatures(geo)
                
                clip, transform = rasterio.mask.mask(clip_src, shapes=coords, filled=True, crop=True)
                clip_meta = lu_src.meta.copy()

                clip_meta.update({"driver": "GTiff", "dtype": dtype, "height": clip.shape[1], "width": clip.shape[2], "transform": transform})

                with rasterio.open(out_ras, 'w', **clip_meta) as dst:
                    dst.write(clip)
                    dst.close()
        print("Clip Complete: ", key)

def reclassComputeTidal(lc_array, rail_array, wetlands_array, lu_array, tc_array, tidal_array, pond_array):

    dst_array= np.where( (lu_array==2130), 2130, 0) #other impervious

    #other impervious
    dst_array[np.where ( ((lc_array==2) | (lc_array==4) | (lc_array ==5) | (lc_array==6)) & (rail_array == 1))] = 2130 

    #tree canopy over other impervious
    dst_array[np.where ( (lc_array==3) &  (rail_array == 1) )] = 2143

    dst_array[np.where( (lu_array==3100))] = 3100 #forest

    #add in tree canopy over classes from lc
    dst_array[np.where ( (lc_array==10))] = 2142 # TC over Structures
    dst_array[np.where ( (lc_array==11))] = 2143 # TC over Other Impervious
    dst_array[np.where ( (lc_array==12))] = 2141 # TC over Roads
    #more tc
    dst_array[np.where ( (tc_array==2240) & (lc_array==3))] = 2240 #TC over turf
    dst_array[np.where ( (tc_array==3200) & (lc_array==3))] = 3200 #TC over ag

    #ponds
    dst_array[np.where ( ((lu_array==1000) | (lc_array == 1)) & (pond_array==1) )] = 1120

    #add wetland logic
    #nontidal riverine (wetland = 52)
    dst_array[np.where ( ((lu_array==2220 ) | (lu_array==3410) |  (lu_array==2231) |  (lu_array==4111) |  (lu_array==4121) |  (lu_array==4131) | (lu_array ==5400) | (lu_array ==4101))  &  (wetlands_array == 52) )] = 5201
    dst_array[np.where ( ( (lu_array==3420) | (lu_array ==2232) | (lu_array ==4112) | (lu_array ==4122) | (lu_array ==4132) | (lu_array ==4102))  &  (wetlands_array == 52) )] = 5202
    dst_array[np.where ( ( lu_array==5000) & (wetlands_array == 52) )]= 5202
    dst_array[np.where ( ((lu_array==3430 ) | (lu_array ==2233) | (lu_array ==4123) | (lu_array ==4133) | (lu_array ==4103))  &  (wetlands_array == 52) )] = 5203
    dst_array[np.where ( ((lu_array==2240 ) | (lu_array==3200)) &  (wetlands_array == 52) )] = 5204
    dst_array[np.where ( ((lu_array==3100 ) | (lu_array==3000)) &  (wetlands_array == 52) )] = 5205

    #nontidal terrene (wetland = 53) 
    dst_array[np.where ( ((lu_array==2220 ) | (lu_array==3410) |  (lu_array==2231) |  (lu_array==4111) |  (lu_array==4121) |  (lu_array==4131) | (lu_array ==5400) | (lu_array ==4101))  &  (wetlands_array == 53) )] = 5301
    dst_array[np.where ( ((lu_array==3420) | (lu_array ==2232) | (lu_array ==4112) | (lu_array ==4122) | (lu_array ==4132) | (lu_array ==4102))  &  (wetlands_array == 53) )] = 5302
    dst_array[np.where ( ( lu_array==5000) & (wetlands_array == 53) )]= 5302
    dst_array[np.where ( ((lu_array==3430 ) | (lu_array ==2233) | (lu_array ==4123) | (lu_array ==4133) | (lu_array ==4103))  &  (wetlands_array == 53) )] = 5303
    dst_array[np.where ( ((lu_array==2240 ) | (lu_array==3200)) &  (wetlands_array == 53) )] = 5304
    dst_array[np.where ( ((lu_array==3100 ) | (lu_array==3000)) &  (wetlands_array == 53) )] = 5305

    #tidal wetlands
    dst_array[np.where ( ((lu_array==2220 ) |  (lu_array==4111) |  (lu_array==4121) |  (lu_array==4131) | (lu_array ==4101))  &  (tidal_array == 3) )] = 5101
    dst_array[np.where ( ((lu_array==3410 ) |  (lu_array==2231) |  (lu_array==5400) )  &  ((tidal_array == 1) | (tidal_array ==2)) )] = 5101

    dst_array[np.where ( ((lu_array==2210 )  | (lu_array ==4112) | (lu_array ==4122) | (lu_array ==4132) | (lu_array ==4102))  &  (tidal_array == 3) )] = 5102
    dst_array[np.where ( (lu_array==3420) & ( (tidal_array == 1) | (tidal_array ==2)  |  (lc_array == 2) )) ] = 5102
    dst_array[np.where ( (lu_array ==2232) & ((tidal_array == 1) | (tidal_array ==2)) )] = 5102
    dst_array[np.where ( ( lu_array==5000) & ((tidal_array == 1) | (tidal_array ==2)) )]= 5102

    #what do we do where emergent wetlands don't fall within any of the nontidal/tidal masks? this does not happen anymore with beebs wetlands update.
    #for tidal
    #dst_array[np.where ( (lc_array==2) & ((tidal_array != 1) & (tidal_array !=2) & (tidal_array != 3) ))] = 5102
    ##for nontidal
    #dst_array[np.where ( (lc_array==2) & ((wetlands_array != 52) & (wetlands_array !=53)) )] = 5102 #5302 or 5202?

    dst_array[np.where ( ( (lu_array ==4123) | (lu_array ==4133) | (lu_array ==4103))  &  (tidal_array == 3) )] = 5103
    dst_array[np.where ( ( (lu_array==3430 ) | (lu_array ==2233) ) & ((tidal_array == 1) | (tidal_array ==2)) )] = 5103

    dst_array[np.where ( ((lu_array==2240 ) | (lu_array==3200)) &  (tidal_array == 3) )] = 5104

    dst_array[np.where ( ((lu_array==3100 ) | (lu_array==3000)) &  (tidal_array == 3) )] = 5105


    dst_array = dst_array.astype("uint16")
    return dst_array

def reclassComputeNontidal(lc_array, rail_array, wetlands_array, lu_array, tc_array, pond_array):

    dst_array= np.where( (lu_array==2130), 2130, 0) #other impervious

    #other impervious
    dst_array[np.where ( ((lc_array==2) | (lc_array==4) | (lc_array ==5) | (lc_array==6)) & (rail_array == 1))] = 2130

    #tree canopy over other impervious
    dst_array[np.where ( (lc_array==3) &  (rail_array == 1) )] = 2143

    dst_array[np.where( (lu_array==3100))] = 3100 #forest

    #add in tree canopy over classes from lc
    dst_array[np.where ( (lc_array==10))] = 2142 # TC over Structures
    dst_array[np.where ( (lc_array==11))] = 2143 # TC over Other Impervious
    dst_array[np.where ( (lc_array==12))] = 2141 # TC over Roads
    #more tc
    dst_array[np.where ( (tc_array==2240) & (lc_array==3))] = 2240 #TC over turf
    dst_array[np.where ( (tc_array==3200) & (lc_array==3))] = 3200 #TC over ag

    # ponds
    dst_array[np.where ( ((lu_array==1000) | (lc_array == 1)) & (pond_array==1) )] = 1120

    #add wetland logic
    #nontidal riverine (wetland = 52) riverine
    dst_array[np.where ( ((lu_array==2220 ) | (lu_array==3410) |  (lu_array==2231) |  (lu_array==4111) |  (lu_array==4121) |  (lu_array==4131) | (lu_array ==5400) | (lu_array ==4101))  &  (wetlands_array == 52) )] = 5201
    dst_array[np.where ( ( (lu_array==3420) | (lu_array ==2232) | (lu_array ==4112) | (lu_array ==4122) | (lu_array ==4132) | (lu_array ==4102))  &  (wetlands_array == 52) )] = 5202
    dst_array[np.where ( ( lu_array==5000) & (wetlands_array == 52) )]= 5202
    dst_array[np.where ( ((lu_array==3430 ) | (lu_array ==2233) | (lu_array ==4123) | (lu_array ==4133) | (lu_array ==4103))  &  (wetlands_array == 52) )] = 5203
    dst_array[np.where ( ((lu_array==2240 ) | (lu_array==3200)) &  (wetlands_array == 52) )] = 5204
    dst_array[np.where ( ((lu_array==3100 )  | (lu_array==3000)) &  (wetlands_array == 52) )] = 5205

    #nontidal terrene (wetland = 53) 
    dst_array[np.where ( ((lu_array==2220 ) | (lu_array==3410) |  (lu_array==2231) |  (lu_array==4111) |  (lu_array==4121) |  (lu_array==4131) | (lu_array ==5400) | (lu_array ==4101))  &  (wetlands_array == 53) )] = 5301
    dst_array[np.where ( ((lu_array==3420) | (lu_array ==2232) | (lu_array ==4112) | (lu_array ==4122) | (lu_array ==4132) | (lu_array ==4102))  &  (wetlands_array == 53) )] = 5302
    dst_array[np.where ( ( lu_array==5000) & (wetlands_array == 53) )]= 5302
    dst_array[np.where ( ((lu_array==3430 ) | (lu_array ==2233) | (lu_array ==4123) | (lu_array ==4133) | (lu_array ==4103))  &  (wetlands_array == 53) )] = 5303
    dst_array[np.where ( ((lu_array==2240 ) | (lu_array==3200)) &  (wetlands_array == 53) )] = 5304
    dst_array[np.where ( ((lu_array==3100 )  | (lu_array==3000)) &  (wetlands_array == 53) )] = 5305

    


    dst_array = dst_array.astype("uint16")
    return dst_array

## Reclass-burninvalue
def reclassBurnInValueTidal(lu_ras_path, out_burnin_path, lc_clip, rail_clip, nontidal_clip, tc_clip, tidal_clip, pond_clip):
    lu_ras = lu_ras_path
    out_burnin = out_burnin_path

    with rasterio.open(lc_clip, 'r') as lc_src:
        with rasterio.open(rail_clip, 'r') as rail_src:
            with rasterio.open(nontidal_clip, 'r') as wetlands_src:
                with rasterio.open(lu_ras, 'r') as lu_src:
                    with rasterio.open(tc_clip, 'r') as tc_src:
                        with rasterio.open(tidal_clip, 'r') as tidal_src:
                            with rasterio.open(pond_clip, 'r') as pond_src:
                                lc_clip_meta = lc_src.meta.copy()
                                lc_clip_meta.update({"dtype": "uint16"})

                                with rasterio.open(out_burnin, 'w', **lc_clip_meta) as dst:
                                    #do band math with numpy arrays
                                    lc_array = lc_src.read(1)
                                    rail_array = rail_src.read(1)
                                    wetlands_array= wetlands_src.read(1) 
                                    lu_array = lu_src.read(1)
                                    tc_array= tc_src.read(1)
                                    tidal_array= tidal_src.read(1)
                                    pond_array = pond_src.read(1)

                                    result=reclassComputeTidal(lc_array, rail_array, wetlands_array, lu_array, tc_array, tidal_array, pond_array)
                                    dst.write(result, 1)

def reclassBurnInValueNontidal(lu_ras_path, out_burnin_path, lc_clip, rail_clip, nontidal_clip, tc_clip, pond_clip):
    lu_ras = lu_ras_path
    out_burnin = out_burnin_path


    with rasterio.open(lc_clip, 'r') as lc_src:
        with rasterio.open(rail_clip, 'r') as rail_src:
            with rasterio.open(nontidal_clip, 'r') as wetlands_src:
                with rasterio.open(lu_ras, 'r') as lu_src:
                    with rasterio.open(tc_clip, 'r') as tc_src:
                        with rasterio.open(pond_clip, 'r') as pond_src:
                            lc_clip_meta = lc_src.meta.copy()
                            lc_clip_meta.update({"dtype": "uint16"})

                            with rasterio.open(out_burnin, 'w', **lc_clip_meta) as dst:
                                #do band math with numpy arrays
                                lc_array = lc_src.read(1)
                                rail_array = rail_src.read(1)
                                wetlands_array= wetlands_src.read(1) 
                                lu_array = lu_src.read(1)
                                tc_array= tc_src.read(1)
                                pond_array = pond_src.read(1)

                                result=reclassComputeNontidal(lc_array, rail_array, wetlands_array, lu_array, tc_array, pond_array)
                                dst.write(result, 1)

#burn in burn in values to lc
def reclassBurnFinalStep(lu_ras_path, out_burnin_path, out_lu_burnin_path, lc_clip):
    lu_ras = lu_ras_path
    out_burnin = out_burnin_path
    out_lu_burnin= out_lu_burnin_path

    #burn in burn in values to lu
    with rasterio.open(lu_ras, 'r') as lu_src:
        with rasterio.open(out_burnin, 'r') as burnin_src:
            with rasterio.open(lc_clip, 'r') as lc_src:
            
                burnin_meta = burnin_src.meta.copy()
                burnin_meta.update({"compress": "lzw"})
                with rasterio.open(out_lu_burnin, 'w', **burnin_meta) as dst:
                    #do band math with numpy arrays
                    burnin_array = burnin_src.read(1)
                    lc_array = lc_src.read(1)
                    lu_array = lu_src.read(1)
                    
                    dst_array= np.where ( (burnin_array > 0), burnin_array, lu_array)
                    dst_array= np.where ( (dst_array==0),  lc_array,  dst_array)
                    dst.write(dst_array, 1)        
    
    #return out_lu_burnin
    # return dst_array, burnin_meta

#########################################################################################
####################################HELPERS##############################################
#########################################################################################


def getFeatures(gdf):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][0]['geometry']]

#########CLIP RASTER BY POLYGON FUNCTIONS#############

def getCountyGeom(county_shp, cf):
    """
    Method: getCountyGeom()
    Purpose: Get county boundary geometry from 20m buffered counties shapefile
    Params: drive - path to drive with ancillary folder structure
            cf - county fips
    Returns: cnty_geom - list of county geometry
    """
    cspath = county_shp
    cs = gpd.read_file(cspath, driver='shapefile')
    #Get CF Number
    cfnum = cf.split('_')[1]
    #Get only the row with CF Number and store in list
    cnty_geom = list(cs[cs['GEOID'] == cfnum]['geometry'])
    
    return cnty_geom

def clipRasByGeom(county_shp, cf, og_path, clip_path):
    """
    Method: clipRasByGeom()
    Purpose: Clip raster by county boundary.
    Params: drive - path to drive with ancillary folder structure
            cf - county fips
            rasPath - path to tif
    Returns: out_image - numpy array of change clipped to county boundary
    """
    geom = getCountyGeom(county_shp, cf)
    with rasterio.open(og_path) as src_co:
        out_image, out_transform = rasterio.mask.mask(src_co, geom, crop=True)
        out_meta = src_co.meta
        out_meta.update({"driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform}) 

    with rasterio.open(clip_path, 'w', **out_meta, COMPRESS= 'LZW') as dst:
        dst.write(out_image)
    #return out_image, out_meta

def createPyramids(out_lu_burnin_path):
    out_lu_burnin= out_lu_burnin_path

    #build pyramids
    factors = [2, 4, 8, 16, 32, 64, 128, 256, 512]
    with rasterio.open(out_lu_burnin, 'r+') as dst:
        dst.build_overviews(factors, Resampling.nearest)
        dst.close()

def addSymbologyandRAT(OUTPUT_DIR, df, ras, rio_meta):
    """
    Method: addSymbologyandRAT()
    Purpose: Build the raster attribute table for the array and add colormap. Write raster to
                destination.
    Params: OUTPUT_DIR - path to write the burned raster with symbology
            df - dataframe of raster values, descriptions/class, RGB values
            ras - the numpy array of LU
            rio_meta - the rasterio metadata to use for projections
    Returns: N/A
    """
    # Get unique vals and counts
    vals, counts = np.unique(ras, return_counts=True)
    vals = list(vals)
    counts = list(counts)

    # convert transform info from rasterio format to gdal format
    trans_rio = list(rio_meta['transform'])
    trans = (trans_rio[2], trans_rio[0], trans_rio[1], trans_rio[5], trans_rio[3], trans_rio[4])

    # create raster
    d = gdal.GetDriverByName('GTIFF')
    myRas = d.Create(OUTPUT_DIR, ras.shape[1], ras.shape[0], 1, gdal.GDT_UInt16, options=['COMPRESS=LZW']) # Creates empty dataset
    myRas.SetGeoTransform(trans)
    myRas.SetProjection(rio_meta['crs'].to_wkt())
    band = myRas.GetRasterBand(1)
    band = createRAT(band, df, vals, counts) #add symbology and RAT
    band.SetNoDataValue(0)
    band.WriteArray(ras)
    myRas.FlushCache()

def createRAT(band, df, vals, counts):
    """
    Method: createRAT()
    Purpose: Build raster attribute table containing raster values, counts and
             the Red, Green and Blue values for the colormap.
    Params: band - raster band to create RAT for
            df - dataframe with columns:
                    Value - integer raster value
                    LU - string name of LU class
                    Red - integer value for RGB
                    Green - integer value for RGB
                    Blue - integer value for RGB
            vals - list of unique values in raster band
            counts - list of counts of unique values in raster band
    Returns: band - raster band with RAT attached
    """
    # Create columns you want in RAT
    rat = gdal.RasterAttributeTable()
    rat.CreateColumn("Value", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    #rat.CreateColumn("Count", gdalconst.GFT_Integer, gdalconst.GFU_PixelCount)
    rat.CreateColumn("Red", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    rat.CreateColumn("Green", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    rat.CreateColumn("Blue", gdalconst.GFT_Integer, gdalconst.GFU_MinMax)
    rat.CreateColumn("LandUse", gdalconst.GFT_String, gdalconst.GFU_Name) 

    # populate the columns
    ct = 0
    for idx, row in df.iterrows():
        if row['Value'] in vals:
            rat.SetValueAsInt(ct, 0, int(row['Value']))
            #rat.SetValueAsInt(ct, 1, int(counts[vals.index(row['Value'])]))
            rat.SetValueAsInt(ct, 1, int(row['Red']))
            rat.SetValueAsInt(ct, 2, int(row['Green']))
            rat.SetValueAsInt(ct, 3, int(row['Blue']))
            rat.SetValueAsString(ct, 4, row['LandUse'])
            ct += 1
        
    # set the default Raster Attribute Table for src_ds band 1 to the newly modified rat
    band.SetDefaultRAT(rat)
    return band


###################### FIX FOREST SPECKLES FUNCTIONS ####################################
def fixForest(ary, transform):
    """
    Method: fixForest()
    Purpose: Find forest speckles that should be TCT or TOA and reclass them as such.
    Params: ary - burn in ary last step before writing out?
    Returns: for_zones - array of reclassed forest speckles - NEEDS TO BE ADDED TO ORIGINAL BURN IN ARRAY
    """ 
    sh = (ary.shape[0], ary.shape[1])

    # get forest zones
    forest_gdf = vectorizeRaster(np.where(ary == 3100, 1, 0), transform)
    forest_gdf.loc[:, 'area'] = forest_gdf.geometry.area
    forest_gdf = forest_gdf[forest_gdf['area'] < 100]
    forest_gdf = forest_gdf[['zone', 'geometry']]

    if len(forest_gdf) == 0: # no speckles to update - move on
        return None, False

    # Get TCT zones
    tct_gdf = vectorizeRaster(np.where(ary == 2240, 1, 0), transform) 
    tct_list = sjoin_mp6(forest_gdf, 10000, 'intersects', ['zone'], tct_gdf[['geometry']])
    tct_list = list(tct_list['zone'])
    del tct_gdf

    # Get TOA zones
    toa_gdf = vectorizeRaster(np.where(ary == 3200, 1, 0), transform) 
    toa_list = sjoin_mp6(forest_gdf, 10000, 'intersects', ['zone'], toa_gdf[['geometry']])
    toa_list = list(toa_list['zone'])
    del toa_gdf

    # update forest zones with burn
    forest_gdf.loc[:, 'burn'] = 0
    if len(tct_list) > 0:
        forest_gdf.loc[forest_gdf['zone'].isin(tct_list), 'burn'] = 2240
    if len(toa_list) > 0:
        forest_gdf.loc[forest_gdf['zone'].isin(toa_list), 'burn'] = 3200
    forest_gdf = forest_gdf[forest_gdf['burn'] != 0]
    del tct_list
    del toa_list
        
    if len(forest_gdf) > 0:
        # Step 3 - rasterize the speckles of forest as tct or toa
        geoms = [(feature['geometry'],feature['burn']) for idx, feature in forest_gdf.iterrows()]
        del forest_gdf
        ary = rasterize(geoms, out_shape=sh, fill=0,  transform=transform, all_touched=False)

        return ary.astype(np.uint16), True
    else:
        return None, False

def vectorizeRaster(unique_array, transform):
    """
    Method: vectorizeRaster()
    Purpose: Create polygon geometries for each unique zone in the raster.
    Params: unique_array - numpy array of zones
            transform - rasterio transform of array that is to be vectorized
    Returns: zones_gdf - geodataframe of vectorized raster zones with unique field 'zone'
    """
    unique_array = unique_array.astype(np.int16)
    geoms = []
    for i, (s, v) in enumerate(shapes(unique_array, mask=unique_array.astype(bool) , connectivity=4, transform=transform)): 
        geoms.append(Polygon(s['coordinates'][0]))
    zones_gdf = gpd.GeoDataFrame(geometry=geoms, crs="EPSG:5070")
    zones_gdf['zone'] = [int(x) for x in range(1, len(zones_gdf)+1)]
    return zones_gdf


def sjoin_mp6(df1, batch_size, sjoin_op, sjoinCols, df2):
    """
    Method: sjoin_mp6()
    Purpose: Chunk and mp a sjoin function on specified geodataframes for specified operation,
             retaining specified columns.
    Params: df1 - geodataframe of data to chunk and sjoin (left gdf)
            batch_size - integer value of max number of records to include in each chunk
            sjoin_op - string of sjoin operation to use; 'intersects', 'within', 'contains'
            sjoinCols - list of column names to retain
            df2 - geodataframe of data to sjoin (right gdf)
    Returns: sjoinSeg - df (or gdf) of sjoined data, with sjoin columns retained
    """
    NUM_CPUS = mp.cpu_count() - 2
    c = list(df1)
    df1 = df1.reset_index()
    df1 = df1[c]
    if len(df1) == 0:
        print('df1 is empty')
 
    num_chunks = int(len(df1) / batch_size) + 1

    #make cols a string for now to pass as 4th arg to sjoin
    tmpCols = ''
    for s in range(len(sjoinCols)):
        tmpCols += sjoinCols[s]
        if s+1 != len(sjoinCols):
            tmpCols += ' '
 
    chunk_iterator = []
    for i in range(0, num_chunks):
        mn, mx = i * batch_size, (i + 1) * batch_size
        gdf_args = df1[mn:mx], df2, sjoin_op, tmpCols
        chunk_iterator.append(gdf_args)

    pool = mp.Pool(processes=NUM_CPUS)
    sj_results = pool.map(sjoin_mp_pt5, chunk_iterator)
    pool.close()
    sj_results = pd.concat(sj_results)
    sj_results.drop_duplicates(inplace=True)
    return sj_results

def sjoin_mp_pt5(args):
    """
    Method: sjoin_mp_pt5()
    Purpose: Run sjoin on specified geodataframes for specified operation,
             retaining specified columns.
    Params: args - tuple of arguments
                df1 - geodataframe of data to sjoin (left gdf)
                df2 - geodataframe of data to sjoin (right gdf)
                sjoin_op - string of sjoin operation to use; 'intersects', 'within', 'contains'
                sjoinCols - string of column names to retain, separated by a space
    Returns: sjoinSeg - df (or gdf) of sjoined data, with sjoin columns retained
    """
    df1, df2, sjoin_op, sjoinCols = args 
    cols = sjoinCols.split(' ') #list of column names to keep
    sjoinSeg = gpd.sjoin(df1, df2, op=sjoin_op)
    sjoinSeg = sjoinSeg[cols]
    sjoinSeg.drop_duplicates(inplace=True)
    return sjoinSeg