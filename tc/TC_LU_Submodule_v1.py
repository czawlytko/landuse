# python B:/landuse_dev/TC_Submodule_v1.py
"""
File: TC_Submodule_v1.py
Purpose: Create TCT (Tree Canopy over Turf) and TOA (Trees in Agriculture) polygons and
         write to psegs geopackage as layer 'tct_toa'
Author: Sarah McDonald, Geographer, USGS
Contact: smcdonald@chesapeakebay.net
"""

#########################################################################################
#################################IMPORT LIBS############################################
#########################################################################################

import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
import time
import multiprocessing as mp
import multiprocessing.pool
from multiprocessing import freeze_support
from shapely.geometry import mapping, shape, MultiPolygon, Point, Polygon
from shapely.ops import unary_union
import shutil
from pathlib import Path

import tc.dense_mp_v1 as env_pkg
from tc.dense_mp_v1 import dense as callDense
import tc.QGIS_geoprocessing as qgis_pkg
import luconfig
from helpers import etime


#####################################################################################
#------------------------------- MAIN ------------- --------------------------------#
#####################################################################################
def run_trees_over_submodule(NUM_CPUS, cf):
    """
    Method: run_trees_over_submodule()
    Purpose: This is the method to be called from the LU model main script. This method
             calls all functions needs to create the trees_over.gpkg. If this function fails,
             the exception is caught and it will not kill the lu model run. The burn in code
             must add a check to ensure the trees_over.gpkg file was created.
    Parms: NUM_CPUS - the number of cpus to give to each thread/tile. For 64 processors, 5 is the recommended
                        value. This would run 10 tiles simulataneously.
            cf - the county fips string; first four letters of the county name, underscore, county fips
    Returns: Flag for if module ran properlyy;
                0 - module ran and created trees over geopackage
                -1 - exception was thrown and data may not have been created
    """
    # Calculate number of tiles to run at once
    NUM_TILES = int((mp.cpu_count() - 2) / (NUM_CPUS + 1))
    if NUM_TILES < 1:
        print("ERROR: Core distribution would result in 0 tiles running at a time")
        print("\tIncrease cores or decrease NUM_CPUS")
        raise TypeError("TC ERROR: Core distribution would result in 0 tiles running at a time")
    elif NUM_TILES == 1:
        print("ERROR: Core distribution would result in 1 tile running at a time")
        print("\tIf you want to run serially, comment out sys.exit below this message")
        raise TypeError("TC ERROR: Core distribution would result in 1 tile running at a time")
    else:
        print("Running ", NUM_TILES, " tiles at a time")
        print("Each tile will have ", NUM_CPUS, " cores")

    folder = luconfig.folder
    anci_folder = luconfig.anci_folder

    psegsLayer = 'psegs_lu'
    parcels_layer = 'parcels_vectorized'

    urban_shp = f"{anci_folder}/census/urban_area_albers.shp"
    psegsPath = f"{folder}/{cf}/output/data.gpkg"
    parcels_gpkg = f"{folder}/{cf}/temp/temp_dataprep.gdb"
    out_gpkg = f"{folder}/{cf}/output/trees_over.gpkg" 
    tiles_shp = f"{folder}/{cf}/temp/tc_tiles.shp" 
    
    #location of temp files needed for QGIS workflows will be written - can delete these after
    qgis_temp_files_path = Path(folder,cf,"temp_qgis_files")
    if not os.path.exists(qgis_temp_files_path):
        os.makedirs(qgis_temp_files_path)

    print("\n***********************************")
    print(  "********* TREE CANOPY *************")
    print(  "***********************************\n")


    # try: #if a county fails - don't kill thread
    start = time.time()
    densePIDs = callDense.runDense(parcels_gpkg, parcels_layer, urban_shp) #get dense parcels
    etime(cf, "Got Dense Parcels", start)
    st = time.time()

    data = []
    tiles = gpd.read_file(tiles_shp)
    note = str(len(tiles)) + ' number of tiles'
    etime(cf, note, start)
    st = time.time()

    chunk_iterator = []
    for idx, row in tiles.iterrows():
        psegs = env_pkg.readPolysMask(psegsPath, psegsLayer, row['geometry'].envelope) # read in psegs layer
        note = "Read psegs gpkg for tile " + str(row['id'])
        etime(cf, note, st)
        st = time.time()
        crs = psegs.crs

        psegs = psegs[['PID', 'PSID', 'Class_name', 'lu', 'geometry']] #remove columns I don't need for workflow
        psegs = psegs.explode()
        note = str(row['id']) + ' tile has ' + str(len(psegs)) + ' psegs'
        etime(cf, note, st)
        st = time.time()

        psegs = env_pkg.runEnviro(psegs, densePIDs) #create and update EnvType column
        etime(cf, "Categorized by environment", st)
        st = time.time()

        invalid_geoms = psegs[~psegs['geometry'].is_valid] #get invalid geoms
        cols = list(psegs)
        if len(invalid_geoms) > 0:
            for idx, row in invalid_geoms.iterrows():
                clean = shape(row['geometry']).buffer(0.0)
                newRow = []
                for c in cols:
                    if c != 'geometry':
                        newRow.append(row[c])
                newRow += [clean]
                psegs.loc[idx] = newRow
            note = "Fixed " + len(invalid_geoms) + " invalid pseg geometries"
            etime(cf, note, st)
            st = time.time()

        # get unique ag and turf lu values
        allAg = []
        allLUS = list(set(list(psegs['lu'])))
        oldAg = ['Cropland', 'Pasture/Hay', 'Orchard/Vineyard']  # ['crop', 'orchard vineyard', 'pasture'] 
        for a in allLUS:
            al = a
            if a == None:
                al = 'None'
            al = al.lower()
            for o in oldAg:
                if o in al:
                    allAg.append(a)

        allTurf = [x for x in allLUS if x != None and 'turf' in x.lower()]

        #get list of data needed for each tile
        chunk_iterator.append( (psegs, allAg, allTurf, row['id'], cf, NUM_CPUS) )

    #pull processors
    print("\n")
    if platform.system() == 'Linux':
        pool = NestablePool
    elif platform.system() == 'Windows':
        pool = MyPool(NUM_TILES)
    else:
        raise TypeError(f"OS System not Windows or Linux: {platform.system()}")
    data = pool.map(runTCT, chunk_iterator)
    pool.close()
    pool.join()

    etime(cf, "Trees Over Workflow for all tiles", st)
    st = time.time()

    bufs, tct, toa = [], [], []

    for d in range(len(data)):
        if len(data[d][1]) > 0:
            tct.append(data[d][1][data[d][1]['lu'] == 'tct'])
            toa.append(data[d][1][data[d][1]['lu'] == 'toa'])
        for t in data[d][0]:
            if len(t) > 0:
                tct_path = t.split('|')
                gpkg = tct_path[0]
                layerN = tct_path[1].split('=')[-1]
                tmp = gpd.read_file(gpkg, layer=layerN) #replace gpkg path with tct data
                tmp['lu'] = 'tct'
                tmp['logic'] = 'buffer workflow'
                tmp['lu_code'] = 2240
                bufs.append(tmp)
    
    #concat and write buffers layer
    bufs = gpd.GeoDataFrame(pd.concat(bufs, ignore_index=True), crs=crs)
    bufs = bufs[['lu_code', 'lu', 'logic', 'geometry']]
    bufs = bufs.explode()
    bufs = bufs.reset_index()[['lu_code', 'lu', 'logic', 'geometry']]
    if len(bufs) > 0:
        bufs.to_file(out_gpkg, layer='tct_bufs', driver="GPKG")
    del bufs
    #concat and write tct layer
    tct = gpd.GeoDataFrame(pd.concat(tct, ignore_index=True), crs=crs)
    tct = tct[['lu_code', 'lu', 'logic', 'geometry']]
    tct = tct.explode()
    if len(tct) > 0:
        tct = tct.reset_index()[['lu_code', 'lu', 'logic', 'geometry']]
        tct.to_file(out_gpkg, layer='tct', driver="GPKG")
    del tct
    #concat and write toa layer
    toa = gpd.GeoDataFrame(pd.concat(toa, ignore_index=True), crs=crs)
    toa = toa[['lu_code', 'lu', 'logic', 'geometry']]
    toa = toa.explode()
    if len(toa) > 0:
        toa = toa.reset_index()[['lu_code', 'lu', 'logic', 'geometry']]
        toa.to_file(out_gpkg, layer='toa', driver="GPKG")
    del toa

    etime(cf, "Total write ", st)
    st = time.time()

    # deleteTempFiles(cf)
    # etime(cf, "Deleted temp folder", st)

    etime(cf, "Tree Canopy Model - run_trees_over_submodule()", start)

#########################################################################################
#############################CLASSES NEEDED TO MP TILES##################################
#########################################################################################
"""
Got these from: https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
"""
class NoDaemonProcess(mp.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(mp.pool.Pool):
    freeze_support()
    Process = NoDaemonProcess

#########################################################################################
#################################RUN WORKFLOWS###########################################
#########################################################################################

def runTCT(args):
    """
    Method: runTCT()
    Purpose: Separate data by the 4 environment types (dense, less dense, forested, and agricultural)
             and run TCT buffer workflow on each. Then run the TCT/TOA workflow on each. Return one
             gdf of polygons with 'lu' field of 'tct' or 'toa' for tree canopy over turf or tree canopy
             over agriculture.
    Params: N/A
    Returns : list of 
                tct - paths to buffers to use for tct
                toa - geodataframe of TCT and TOA polygons from rules 1-3
    """
    psegs, allAg, allTurf, tile, cf, NUM_CPUS = args
    crs = psegs.crs
    lusToKeep = allAg + allTurf + ['Buildings', 'Other Impervious Surface']
    st = time.time()
    roads = psegs[(psegs['lu'] == 'Roads') | (psegs['Class_name'] == 'Roads')]
    psegs = psegs[(psegs['lu'] != 'Roads') & (psegs['Class_name'] != 'Roads')]

    roads = roads[['geometry']]
    roads = unary_union(roads['geometry'])
    
    #Subset Environments
    # get dense - remove records that are dense and not a lu to keep
    dense = getSubsetEnv(psegs, 1, allTurf, tile, cf)
    psegs = psegs[(psegs['EnvType'] != 1)|( (psegs['EnvType'] == 1) & (psegs['lu'].isin(lusToKeep)) )]
    note = str(tile) + ' -- Subset ' + str(len(psegs[psegs['EnvType']==1])) + " dense segments"
    etime(cf, note, st)
    st = time.time()

    #get less dense - remove records that are less dense and not a lu to keep
    notDense = getSubsetEnv(psegs, 2, allTurf, tile, cf) 
    psegs = psegs[(psegs['EnvType'] != 2)|( (psegs['EnvType'] == 2) & (psegs['lu'].isin(lusToKeep)) )]
    note = str(tile) + ' -- Subset ' + str(len(psegs[psegs['EnvType']==2])) + " less dense segments"
    etime(cf, note, st)
    st = time.time()

    # get forested - remove records that are forested and not a lu to keep
    forested = getSubsetEnv(psegs, 3, allTurf, tile, cf)
    psegs = psegs[(psegs['EnvType'] != 3)|( (psegs['EnvType'] == 3) & (psegs['lu'].isin(lusToKeep)) )]
    note = str(tile) + ' -- Subset ' + str(len(psegs[psegs['EnvType']==3])) + " forested segments"
    etime(cf, note, st)
    st = time.time()

    #get ag - remove records that are not a lu to keep
    ag = getSubsetAg(psegs, 4, allTurf, allAg, tile, cf, NUM_CPUS) #returns 3rd argument - tct_forested to calc tct and forest for forest frag
    psegs = psegs[psegs['lu'].isin(lusToKeep)] #NEED FOR FRAG FOREST
    note = str(tile) + ' -- Subset ' + str(len(psegs[psegs['EnvType']==4])) + " agricultural segments"
    etime(cf, note, st)
    st = time.time()

    tct = []
    tct.append(calcTCT4(dense[0], roads, 20, 1, tile, cf, NUM_CPUS)) #dense tct
    del dense[0] #gets rid of gdf - leaves forest
    tct.append(calcTCT4(notDense[0], roads, 10, 2, tile, cf, NUM_CPUS)) #not dense tct
    del notDense[0] #get rid of gdf - leaves forest
    tct.append(calcTCT4(forested[0], roads, 10, 3, tile, cf, NUM_CPUS)) #not dense tct - roads_union
    del forested[0] #gets rid of gdf - leaves forest
    tct.append(calcTCT4(ag[0], roads, 10, 4, tile, cf, NUM_CPUS)) #not dense tct
    del ag[0] #gets rid of gdf - leaves forest

    note = str(tile) + " -- Ran Buffer Workflow for all environments"
    etime(cf, note, st)
    st = time.time()

    forest = [dense[0], notDense[0], forested[0], ag[0]]
    del dense
    del notDense
    del forested
    del ag

    forest = groupForest(forest, tct, tile, cf, NUM_CPUS)
    note = str(tile) + " -- Created Forest Layer -- "
    etime(cf, note, st)
    st = time.time()

    note = str(tile) + " -- Removed TCT Buffers and Dissolved Forest -- "
    etime(cf, note, st)
    st = time.time()

    toa =  getForestFrag(forest, psegs, allTurf, tile, cf, NUM_CPUS)

    if len(toa) > 0:
        toa = toa[['lu_code', 'lu', 'logic', 'geometry']]
    
    note = str(tile) + " -- Ran Rules 1-3 -- "
    etime(cf, note, st)
    st = time.time()

    return [tct, toa]

#########################################################################################
############################TCT BUFFERS WORKFLOW#########################################
#########################################################################################
def calcTCT4(gdf, roads, bufferSize, envType, tile, cf, NUM_CPUS):
    """
    Method: calcTCT4()
    Purpose: Find low veg segments that touch a building or other imp; dissolve these low veg segments with
            buildings and other imp based on their unique parcel IDs (PID). Buffer the dissolved data
            by specified value. Difference buffers with roads. Remove Results that do not contain a building
            with the same PID. Results are buffers.
    Params: gdf - geodataframe of turf, buildings and other impervious for parcels of the same env type
            roads - gdf of roads
            bufferSize - integer value to buffer dissolved gdf (20 for dense, 10 for all others)
            envType - integer value of environment type
            tile - int tile id
            cf - string of county fips
    Returns : output_layer - path to gpkg of dissolved buffers with removed cross roads sections
                                 or empty string if no buffers to return
    """
    folder = luconfig.folder
    st = time.time()
    #1. Create 3 geodataframes - buildings + imp, low veg, and tree canopy
    if len(gdf) > 0:
        print("\n\n")
        note = str(tile) + "--Calculating TCT for " + str(len(list(set(list(gdf['PID']))))) + " parcels and " + str(len(gdf)) + " segments..."
        etime(cf, note, st)
        st = time.time()
        buildings = gdf[gdf['Class_name'].isin(['Buildings'])]
        buildings = buildings[['PID', 'geometry']]

        #2.Dissolve low veg segments that touch a building or other imp with buildings and other imp based on parcel ID
        layer_name = 'tct_bufs_'+str(envType)+'_'+str(tile)
        input_layer = f"{folder}/{cf}/temp_qgis_files/temp_" + layer_name + ".gpkg|layername=" + layer_name
        output_layer = f"{folder}/{cf}/temp_qgis_files/" + layer_name+".gpkg"
        gdf.to_file(f"{folder}/{cf}/temp_qgis_files/temp_" + layer_name + ".gpkg", layer=layer_name, driver="GPKG")
        gdf = qgis_pkg.dissolve(input_layer, output_layer, True, fields=['PID'])
        gdf = gdf[['PID', 'geometry']]
        note = str(tile) + "--Unioned gdf"
        etime(cf, note, st)
        st = time.time()
        gdf = bufIntBuilding(gdf, buildings, NUM_CPUS)
        note = str(tile) + "--Removed polys not touching building"
        etime(cf, note, st)
        st = time.time()
    
        #3. Buffer the dissolved lv, buildings and other imp 
        gdf['geometry'] = gdf.geometry.buffer(bufferSize)
        note = str(tile) + "--Buffered buildings and turf"
        etime(cf, note, st)
        st = time.time()
    
        #5. split buffers by roads and remove sections that are across roads
        gdf = spatial_overlay_mp(gdf, roads, 'difference', 'PID', NUM_CPUS)
        note = str(tile) + "--Differenced buffers and roads"
        etime(cf, note, st)
        st = time.time()
        cols = ['PID', 'geometry'] #list(gdf)
        gdf = gdf.reset_index()
        gdf = gdf[cols]
        gdf = bufIntBuilding(gdf, buildings, NUM_CPUS)
        gdf = gdf[cols]
        note = str(tile) + "--Removed cross-roads buffers"
        etime(cf, note, st)
        st = time.time()
        del buildings
        gdf = gdf.explode()
        gdf = gdf[['geometry']]
        if len(gdf) > 0:
            layerName = 'no_roads' + str(envType)+'_'+str(tile)
            gdf.to_file(f"{folder}/{cf}/temp_qgis_files/temp_" + layerName + ".gpkg", layer=layerName, driver='GPKG')
            return f"{folder}/{cf}/temp_qgis_files/temp_" + layerName + ".gpkg|layername=" + layerName
    return ''

#########################################################################################
#############################TCT/TOA WORKFLOW - Rules 1 - 3##############################
#########################################################################################
def getForestFrag(under_acre, psegs, allTurf, tile, cf, NUM_CPUS):
    """
    Method: getForestFrag()
    Purpose: Remove TCT from forest. Split remaining forest by < an acre and >= an acre.
             For < an acre, class as TOA or TCT depending on what it is touching. If touching both,
             use shared border analysis to determine which has the majority border (tbd).
             For >= an acre, class as TOA or TCT if the width is < 72 meters wide. If < 72 meters wide
             and touching both ag and turf, use shared border analysis to determine which has the
             majority border (tbd).
    Params: under_acre - gdf of forest with tct bufs removed and dissolved for all envs
            psegs - all psegs records for ag classes and turf
            allTurf - list of psegs lus that are turf
            tile - integer tile value
            cf - county fips code
    Returns : under_acre - geodataframe of TCT and TOA polygons
    """
    st = time.time()
    print("\n\n")
    note = str(tile) + "--Calculating TCT and TOA for " + str(len(under_acre)) + " forest polys"
    etime(cf, note, st)
    st = time.time()
    if len(under_acre) > 0:
        # dev classes
        dev = ['Buildings', 'Other Impervious Surfaces'] + allTurf
        under_acre.loc[:, 'area'] = under_acre['geometry'].area #get segmented forest area

        #separate into 2 dfs - less than and acre and over an acre
        over_acre = under_acre[under_acre['area'] >= 4047] #get patches over an acre
        under_acre = under_acre[under_acre['area'] < 4047] #get the forest chunks less than an acre
        note = str(tile) + "--Separated < acre and >= acre forest patches"
        etime(cf, note, st)
        st = time.time()

        #for < acre - determine what will be tct and what will be toa
        under_acre = under_acre.reset_index()
        under_acre = under_acre[['Id', 'geometry']]
        sb_nonag = shared_border_mp(psegs[psegs['lu'].isin(dev)][['PSID', 'geometry']], under_acre, 'maj', NUM_CPUS)
        note = str(tile) + "--shared border length forest < an acre and dev psegs"
        etime(cf, note, st)
        st = time.time()

        # Class < acre segments based on what they are touching
        if not under_acre.empty:
            under_acre.loc[:, 'lu'] = 'toa'
            under_acre.loc[under_acre['lu'] == 'toa', 'logic'] = '< acre shared border < half dev'
            under_acre.loc[under_acre['lu'] == 'toa', 'lu_code'] = 3200
            if len(sb_nonag) > 0:
                under_acre.loc[under_acre.Id.isin(sb_nonag), 'lu'] = 'tct' 
                under_acre.loc[under_acre['lu'] == 'tct', 'logic'] = '< acre shared border > half dev'
                under_acre.loc[under_acre['lu'] == 'tct', 'lu_code'] = 2240

        # find patches > acre and < 72m wide
        over_acre = over_acre.reset_index()
        over_acre = over_acre[['Id', 'geometry']]
        isWB = getWidth_mp(over_acre, 'Id', NUM_CPUS)
        over_acre = over_acre[over_acre['Id'].isin(isWB)]
        note = str(tile) + "--Got width for patches > an acre"
        etime(cf, note, st)
        st = time.time()

        # use shared border to split by tct and toa
        sb_nonag = shared_border_mp(psegs[psegs['lu'].isin(dev)][['PSID', 'geometry']], over_acre, 'maj', NUM_CPUS)
        note = str(tile) + "--shared border forest >= an acre and < 72m wide with dev psegs"
        etime(cf, note, st)
        st = time.time()
        over_acre_nodev = over_acre[~over_acre['Id'].isin(sb_nonag)][['geometry']] #patches > acre not touching dev
        over_acre = over_acre[over_acre['Id'].isin(sb_nonag)][['geometry']] #patches > acre touching dev

        # update columns
        if not over_acre_nodev.empty:
            over_acre_nodev.loc[:, 'lu'] = 'toa'
            over_acre_nodev.loc[:, 'logic'] = '> acre < 72m wide not touching dev'
            over_acre_nodev.loc[:, 'lu_code'] = 3200
        if not over_acre.empty:
            over_acre.loc[:, 'lu'] = 'tct'
            over_acre.loc[:, 'logic'] = '> acre < 72m wide touching dev'
            over_acre.loc[:, 'lu_code'] = 2240

        #put all data back into 1 gdf
        under_acre = under_acre.append(over_acre)
        del over_acre
        under_acre = under_acre.append(over_acre_nodev)
        del over_acre_nodev

    return under_acre

#########################################################################################
###############################SUBSET DATA BY ENVIRONMENT################################
#########################################################################################
def getSubsetEnv(gdf, envType, allTurf, tile, cf):
    
    """
    Method: getSubsetEnv()
    Purpose: To separate out the records of the image segments that are a certain environment type. Of the subset,
            separate into 2 geodataframes: forest (tree canopy) and fields that will be buffered for TCT analysis 
            (buildings, other imp and turf).
    Params: gdf - data to separate by environment type
            envType - environment type to keep (1 is dense, 2 is less dense, 3 is forested) - ag is run in separate function
            allTurf - list of psegs lus that are turf
            tile - integer tile value
            cf - county fips
    Returns: list of:
                gdf - geodataframe of records of the env types specified that will be buffered for TCt analysis
                forest - path to gpkg of records of the env types specified that are tree canopy
                                OR empty string if no forest exists
    """
    folder = luconfig.folder
    gdf = gdf[gdf['EnvType'] == envType]
    if len(gdf) > 0:
        forest = gdf[gdf['Class_name'] == 'Tree Canopy']
        gdf = gdf[(gdf['Class_name'].isin(['Buildings', 'Other Impervious Surfaces'])) | (gdf.lu.isin(allTurf))] #classes to be buffered
        buildings = list(gdf[gdf['Class_name'].isin(['Buildings'])]['PID'])
        gdf = gdf[gdf['PID'].isin(buildings)]
        forest = forest[['PID', 'Class_name', 'geometry']]

        #GGIS Dissolve 
        if len(forest) > 0:
            forest['Id'] = [int(i) for i in range(1, len(forest)+1)]
            for_layer_name = 'forest_'+str(envType)+'_'+str(tile)
            input_layer = f"{folder}/{cf}/temp_qgis_files/temp_" + for_layer_name + ".gpkg|layername=" + for_layer_name
            output_layer = f"{folder}/{cf}/temp_qgis_files/" + for_layer_name + ".gpkg"
            forest.to_file(f"{folder}/{cf}/temp_qgis_files/temp_" + for_layer_name + ".gpkg", layer=for_layer_name, driver="GPKG")
            qgis_pkg.dissolve(input_layer, output_layer, False) 

            return [gdf, output_layer]
    return [gpd.GeoDataFrame(), '']

def getSubsetAg(gdf, envType, allTurf, allAg, tile, cf, NUM_CPUS):
    
    """
    Method: getSubsetAg()
    Purpose: To separate out the records of the image segments that are an ag environment type. Of the subset,
            separate into 2 geodataframes: forest (all tree canopy in ag parcels to use in TCT/TOA workflow) 
            and fields that will be buffered for TCT analysis (buildings, other imp and turf).
    Params: gdf - data to separate by environment type
            envType - environment type to keep (4 is agriculture)
            allTurf - list of psegs lus that are turf
            allAg - list of psegs lus that are ag
            tile - integer tile value
            cf - county fips
    Returns: list of:
                gdf - geodataframe of records of the env types specified that will be buffered for TCt analysis
                forest - path to gpkg of all forest in ag parcels
                    fields: RmFor - 0 (remove TCT buffers from these) and 1 (do NOT remove TCT buffers from these)
                            Dis - 0 (Dissolve with other envs forest) and 1 (do NOT dissolve with anything)
    """
    folder = luconfig.folder
    #get subsets of data
    gdf = gdf[gdf['EnvType'] == envType]
    if len(gdf) > 0:
        forest = gdf[gdf['Class_name'] == 'Tree Canopy'][['Class_name', 'geometry']]
        
        # dissolve forest that is going to be used for TOA and TCT Rules 1-3
        # This does not dissolve skinny segments with larger segments (don't join windbreaks with large segs)
        # determine what segments not to dissolve
        forest['Id'] = [int(x) for x in range(1, len(forest)+1)]
        sb_ag = shared_border_mp(gdf[gdf['lu'].isin(allAg)][['PSID', 'geometry']], forest, 'all_ag', NUM_CPUS)
        forest_ddis = forest[forest['Id'].isin(sb_ag)]
        forest = forest[~forest['Id'].isin(sb_ag)]
        if len(forest) > 0:
            for_layer_name = 'forest_'+str(envType)+'_'+str(tile)
            input_layer =f"{folder}/{cf}/temp_qgis_files/temp_" + for_layer_name + ".gpkg|layername=" + for_layer_name
            output_layer = f"{folder}/{cf}/temp_qgis_files/" + for_layer_name+".gpkg"
            forest.to_file(f"{folder}/{cf}/temp_qgis_files/temp_" + for_layer_name + ".gpkg", layer=for_layer_name, driver="GPKG")
            forest = qgis_pkg.dissolve(input_layer, output_layer, True, fields=None)
            forest = forest.explode()
            forest = forest[['Id', 'geometry']]
            if len(forest_ddis) > 0:
                forest = forest.append(forest_ddis[['Id', 'geometry']])
        elif len(forest_ddis) > 0:
            forest = forest_ddis[['Id', 'geometry']]
        if len(forest) > 0:
            forest = forest.reset_index()[['Id', 'geometry']]
            #determine which forest polys are not touching any turf
            #update this to only use gdf['Class_name'] == Low Veg, then find which are in full list only touching ag
            tmp = sjoin_mp6(gdf[['lu', 'geometry']], 15000, 'intersects', ['Id', 'lu'], forest[['Id', 'geometry']], NUM_CPUS)
            turf = list(set(list(tmp[tmp.lu.isin(allTurf)]['Id'])))
            notTurf = list(set(list(tmp[~tmp.lu.isin(allTurf)]['Id'])))
            del tmp
            rmFor = list( set(notTurf) - set(turf) ) # forest Id where forest is touching low veg and none is turf (all ag?)
            forest.loc[:, 'RmFor'] = 0
            forest.loc[forest['Id'].isin(rmFor), 'RmFor'] = 1
            forest.loc[:, 'Dis'] = 0
            forest.loc[forest['Id'].isin(sb_ag), 'Dis'] = 1 #don't dissolve if 1
            forest['Id'] = [int(x) for x in range(1, len(forest)+1)]
            
            forest.to_file(f"{folder}/{cf}/temp_qgis_files/" + for_layer_name + ".gpkg", layer=for_layer_name, driver="GPKG")

            #only keep pseg records needed for TCT workflow
            gdf = gdf[(gdf['Class_name'].isin(['Buildings', 'Other Impervious Surfaces'])) | (gdf.lu.isin(allTurf))] #classes to be buffered
            gdf = gdf[gdf['PID'].isin(list(gdf[gdf['Class_name'].isin(['Buildings'])]['PID']))]
            return [gdf, output_layer] #tct_forest to use for TCT calculation - forest to use for frag forest stuff
    return [gpd.GeoDataFrame(), '']

#########################################################################################
####################################HELPERS##############################################
#########################################################################################
def bufIntBuilding(gdf, buildings, NUM_CPUS):
    """
    Method: bufIntBuilding()
    Purpose: Remove cross-road segments. For each buffer split by roads, determine if a building of the same PID is within it. If not, remove the buffer as it is
            a cross-road segment.
    Params: gdf - geodataframe of buffers
            buildings - geodataframe of buildings
    Returns: gdf - buffers with cross-road segments removed
    """
    idxToRemove = polyContains_mp(gdf, buildings, 'PID', NUM_CPUS)
    gdf.drop(gdf.index[idxToRemove], inplace=True)
    return gdf

def deleteTempFiles(cf):
    """
    Method: deleteTempFiles()
    Purpose: Delete temp_qgis_files folder
    Params: cf - county fips code
    Returns: N/A
    """
    folder = luconfig.folder
    qgis_temp_files_path = Path(folder,cf,"temp_qgis_files")
    if os.path.isdir(qgis_temp_files_path):
        try:
            shutil.rmtree(qgis_temp_files_path)
        except:
            etime(cf, 'Remove temp qgis folder failed', time.time()) 

def groupForest(forest_list, tct_list, tile, cf, NUM_CPUS):
    """
    Method: groupForest()
    Purpose: Loop through each environment type and remove its tct buffers.
             Dissolve remaining forest into one gdf to run rules 1-3.
    Params: forest_list - list of paths to forest gpkg for each environment type (dense, less dense, forested, ag)
            tct_list - list of dissolved tct buffers gpkg paths for each environment type (dense, less dense, forested, ag)
            tile - integer tile value
            cf - county fips
    Returns: forest_group - gdf of all forest in tile dissolved together with tct bufs removed
    """
    folder = luconfig.folder

    forest_group = []
    addWindbreak, forest = gpd.GeoDataFrame(), gpd.GeoDataFrame()
    #1. Remove tct buffers by env type
    for f in range(len(forest_list)):
        if os.path.isfile(forest_list[f]): #if there is forest of this env type in tile
            layer = forest_list[f].split('/')[-1].split('.')[0]
            if os.path.isfile(tct_list[f].split('|layername=')[0]): #if there are tct bufs of this env type in tile
                tct_p = tct_list[f].split('|layername=')[0]
                o_layer = tct_list[f].split('|layername=')[1]
                overlayGDF = gpd.read_file(tct_p, layer=o_layer)
                if f+1 == 4: # if ag env
                    try:
                        forest_tmp = gpd.read_file(forest_list[f], layer=layer)
                    except:
                        print("Failed to open ", forest_list[f])
                        continue # go to next in forest_list
                    tmp_out = forest_tmp[forest_tmp['RmFor']==0]
                    tmp_out = tmp_out.reset_index()
                    forest = spatial_overlay_mp(tmp_out, unary_union(overlayGDF['geometry']), 'difference', 'Id', NUM_CPUS)
                    forest = forest.reset_index().explode()[['Id', 'geometry']]
                    forest = forest.append(forest_tmp[forest_tmp['RmFor']==1][['Id', 'geometry']])
                    forest = forest.merge(forest_tmp[['Id', 'Dis']], on='Id', how='left') #add Dis field back
                    forest_group.append(forest[forest['Dis'] == 0][['geometry']]) #section of ag to be dissolved
                    addWindbreak = forest[forest['Dis'] == 1][['geometry']]
                else: # if non-ag env
                    forest = gpd.read_file(forest_list[f], layer=layer)
                    forest = spatial_overlay_mp(forest, unary_union(overlayGDF['geometry']), 'difference', 'Id', NUM_CPUS)
                    forest_group.append(forest[['geometry']])
            else: #no tct bufs - just read in forest
                forest = gpd.read_file(forest_list[f], layer=layer)
                if f+1 == 4: # if ag env
                    forest_group.append(forest[forest['Dis'] == 0][['geometry']]) #section of ag to be dissolved
                    addWindbreak = forest[forest['Dis'] == 1][['geometry']]
                else:
                    forest_group.append(forest[['geometry']])
    #2. Concat remaining forest and dissolve
    if len(forest_group) > 0: #check if forest exists in tile
        forest = gpd.GeoDataFrame(pd.concat(forest_group, ignore_index=True), crs="EPSG:5070")
        forest = forest.reset_index()[['geometry']]
        for_layer_name = 'all_forest_'+str(tile)
        input_layer =  f"{folder}/{cf}/temp_qgis_files/temp_" + for_layer_name + ".gpkg|layername=" + for_layer_name
        output_layer = f"{folder}/{cf}/temp_qgis_files/" + for_layer_name + ".gpkg"
        forest.to_file(f"{folder}/{cf}/temp_qgis_files/temp_" + for_layer_name + ".gpkg", layer=for_layer_name, driver="GPKG")
        forest = qgis_pkg.dissolve(input_layer, output_layer, True)
        forest = forest[['geometry']] #need to remove level_1
        forest = forest.reset_index()
        forest = forest.explode()
        forest = forest[['geometry']]
    if len(addWindbreak) > 0:
        if len(forest) > 0:
            forest = forest.append(addWindbreak)
        else:
            forest = addWindbreak
    if len(forest) > 0:
        forest['Id'] = [int(i) for i in range(1, len(forest)+1)]
        forest = forest.reset_index()[['Id', 'geometry']]
        return forest
    else:
        return gpd.GeoDataFrame()

#####################################################################################
############### MULTI-PROCESSING FUNCTIONS ##########################################
#####################################################################################

#####################################################################################
#---------------------------- MP CONTAINS FUNCTIONS --------------------------------#
#####################################################################################
def polyContains_mp(gdf1, gdf2, idField, NUM_CPUS):
    """
    Method: polyContains_mp()
    Purpose: Chunk and mp a contains function on specified geodataframes, retaining 
             specified column as ID field.
    Params: gdf1 - geodataframe of data to chunk and run contains (left gdf - poly that WILL contain gdf2)
            gdf2 - geodataframe of data to run contains (right gdf - polys that will be contained by gdf1)
            idField - string name of column to use as ID field
    Returns: fin - list of index values of gdf1 that do NOT contain a poly with the same id value
    """
    folder = luconfig.folder
    cols = list(gdf1)
    gdf1 = gdf1.reset_index()
    gdf1 = gdf1[cols]
    cpus_minus_1 = NUM_CPUS
    if len(gdf1) < cpus_minus_1: #don't mp if only 1 dissolve values < 10
        cpus_minus_1 = 1

    batch_size = len(gdf1) / cpus_minus_1
    if batch_size % 1 != 0:
        batch_size = int((batch_size) + 1)
    else:
        batch_size = int(batch_size)

    chunk_iterator = []
    for i in range(cpus_minus_1):
        mn, mx = i * batch_size, (i + 1) * batch_size
        chunk = gdf1[mn:mx]
        gdf_args = chunk, gdf2, idField
        chunk_iterator.append(gdf_args)
    
    pool = mp.Pool(processes=cpus_minus_1)
    results = pool.map(polyContains, chunk_iterator)
    pool.close()

    fin = []
    for r in results:
        fin += r

    return fin

def polyContains(args):
    """
    Method: polyContains()
    Purpose: Chunk and mp a contains function on specified geodataframes, retaining 
             specified column as ID field.
    Params: gdf1 - chunk of geodataframe gdf1 (left gdf - poly that WILL contain gdf2)
            gdf2 - geodataframe of data to run contains (right gdf - polys that will be contained by gdf1)
            idField - string name of column to use as ID field
    Returns: idxToRemove - list of index values that do not contain a poly with the same ID field value
    """
    folder = luconfig.folder
    gdf1, gdf2, idField = args 
    idxToRemove = []
    for idx, row in gdf1.iterrows():
        polyContains = list(gdf2[gdf2[idField] == row[idField]]['geometry'])
        x = False
        for b in polyContains: 
            if row['geometry'].contains(b):
                x = True
                break
        if not x:
            idxToRemove.append(idx)
    return idxToRemove

#####################################################################################
#----------------------- MP SHARED BORDER FUNCTIONS --------------------------------#
#####################################################################################
def shared_border_mp(ag_segs, forest, r_field, NUM_CPUS):
    """
    Method: shared_border_mp()
    Purpose: Chunk ag segments, forest patches and their relational table and multiprocess calls to shared_border().
             Returns list of forest segment Ids that will be classed as Trees in Ag due to shared border being > half of 
             forest patch perimeter.
    Params: ag_segs - gdf of ag segments
            forest - gdf of forest patch segments
            r_field - string denoting if to return majority ag Ids (maj) or Ids encompassed by ag (all_ag)
    Returns: results - list of forest patch Ids that will be classed as trees in ag (> half of border is shared with ag)
    """
    st = time.time() # ONLY NEED FOR TESTING
    ag_for = sjoin_mp6(ag_segs, luconfig.batch_size, 'intersects', ['Id', 'PSID'], forest, NUM_CPUS)
    elapsed = time.time()-st # ONLY NEED FOR TESTING
    print("Shared border sjoin time: ", elapsed) # ONLY NEED FOR TESTING

    if len(ag_for) > 0:
        ag = list(set(list(ag_for['Id']))) #unique forest patches
        cpus_minus_1 = NUM_CPUS
        if len(ag) < cpus_minus_1:
            cpus_minus_1 = len(ag)
        batch_size = len(ag) / cpus_minus_1
        if batch_size % 1 == 0:
            batch_size = int(batch_size)
        else:
            batch_size = int(batch_size) + 1

        st = time.time() # ONLY NEED FOR TESTING
        chunk_iterator = []
        for i in range(cpus_minus_1):
            mn, mx = i * batch_size, (i+1) * batch_size
            t_for = forest[forest['Id'].isin(ag[mn:mx])]
            t_sj = ag_for[ag_for['Id'].isin(ag[mn:mx])]
            t_ag = ag_segs[ag_segs['PSID'].isin(list(t_sj['PSID']))]
            args = t_ag, t_for, t_sj, r_field
            chunk_iterator.append(args)
        elapsed = time.time()-st # ONLY NEED FOR TESTING
        print("Shared border build chunks time: ", elapsed) # ONLY NEED FOR TESTING

        print("Forest count: ",len(forest))
        print("sjoin count: ",len(ag_for))

        pool = mp.Pool(processes=cpus_minus_1)
        sb_results = pool.map(shared_border, chunk_iterator)
        pool.close()
        results = []
        for res in sb_results:
            results += res
        return results
    else:
        return []

def shared_border(args):
    """
    Method: shared_border()
    Purpose: Calculate the amount of shared border between forest patches and specific psegs
             and determine if it is > half of the patch border. Returns list of patch Ids
             that should be classed as trees in agriculture.
    Params: args
                df1 - gdf of ag segments
                df2 - gdf of forest patch segments
                sjoinSeg - df of relationship between patch id (Id) and psegs (PSID)
    Returns: forest_patches - list of forest patch Ids that shared borders with the passed psegs
    """
    folder = luconfig.folder
    df1, df2, sjoinSeg, r_field = args

    forestList = list(set(list(sjoinSeg['Id'])))  # get unique forest patch segs
    count = 0
    ecount = 0
    forest_patches = []
    for patch in forestList:
        totalShrdBorder = 0
        pseg_list = list(sjoinSeg[sjoinSeg['Id'] == patch]['PSID'])  
        patchGeo = list(df2[df2['Id'] == patch]['geometry'])[0]  
        half_border = patchGeo.length / 2 # if tot count exceeds this it is majority
        for pseg in pseg_list:
            count += 1
            psegGeo = list(df1[df1['PSID'] == pseg]['geometry'])[0]
            try:
                border = patchGeo.intersection(psegGeo) 
                totalShrdBorder += border.length
                if r_field == 'maj' and totalShrdBorder > half_border:
                    forest_patches.append(patch)
                    break # once it exceeds majority - break from current forest patch
            except:
                ecount += 1

        if r_field == 'all_ag':
            if totalShrdBorder / patchGeo.length > 0.85: 
                forest_patches.append(patch)
                
    return forest_patches

#####################################################################################
#------------------------------- MP SJOIN FUNCTIONS --------------------------------#
#####################################################################################
def sjoin_mp6(df1, batch_size, sjoin_op, sjoinCols, df2, NUM_CPUS):
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
    folder = luconfig.folder
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
    dft = time.time()
    df1, df2, sjoin_op, sjoinCols = args 
    cols = sjoinCols.split(' ') #list of column names to keep
    sjoinSeg = gpd.sjoin(df1, df2, how='inner', op=sjoin_op)
    sjoinSeg = sjoinSeg[cols]
    sjoinSeg.drop_duplicates(inplace=True)
    return sjoinSeg

#####################################################################################
#--------------------- MP SPATIAL OVERLAY FUNCTIONS --------------------------------#
#####################################################################################
def spatial_overlay_mp(gdf, overlayGDF, operation, Id, NUM_CPUS):
    """
    Method: spatial_overlay_mp()
    Purpose: Group gdf data by Id and mp call spatial overlay function.
    Params: gdf - geodataframe of data to overlay
            overlayGDF - multipolygon to use as overlay
            operation - overlay operation (difference or intersection)
            Id - string field name in gdf to retain
    Returns: results - geodataframe of overlaid data with Id field retained
    """
    gdf_list = gdf.groupby(Id) #replaced PID
    if len(gdf) > 0:
        arg_list = [(gdf[1], overlayGDF, operation, Id) for gdf in gdf_list]
        pool = mp.Pool(processes=NUM_CPUS)
        results = pool.map(spatial_overlay, arg_list)
        pool.close()
    
        # write output
        results = pd.concat(results).pipe(gpd.GeoDataFrame)
        results.crs = gdf.crs
        return results
    return gdf

def spatial_overlay(args):
    """
    Method: spatial_overlay()
    Purpose: Multiprocess implementation of spatial overlay with support for 
                intersection and difference
    Params: gdf (geodataframe): gdf of geometries you want to modify
            overlay (shapely (multi)polygon): needs to be a unary_union shapely geometry 
            operation (string): intersection or difference
    Returns: geodataframe: gdf with updated geometries after applying spatial overlay (multi polys are exploded to polys)
    """
    gdf, overlay, operation, Id = args
  
    indices = []
    geoms = []
 
    for idx, row in gdf.iterrows():
        geom = shape(row['geometry'])
        if operation == 'intersection':
            geom_result = geom.intersection(overlay)
        elif operation == 'difference':
            geom_result = geom.difference(overlay)
        if not geom_result.is_empty:
            indices.append(row[Id])
            geoms.append(geom_result)
        
    data = {Id: indices, 'geometry': geoms}
    if len(geoms) > 0:
        gdf = gpd.GeoDataFrame(data, crs=gdf.crs)
        gdf = gdf.explode()
        return gdf
    else:
        return gpd.GeoDataFrame(columns=[Id, 'geometry'], crs=gdf.crs)
#####################################################################################
#------------------------------- MP WIDTH FUNCTIONS --------------------------------#
#####################################################################################
def getWidth_mp(gdf, IDfield, NUM_CPUS):
    """
    Method: getWidth()
    Purpose: Find all segments with a width less than 37m.
    Params: gdf - geodataframe of segments to find widths for
            IDfield - the field name to record for segments with widths < 37m
    Returns: wind_break - list of IDs for segments with <37m width
    """
    cols = list(gdf)
    gdf = gdf.reset_index()
    gdf = gdf[cols]
    cpus_minus_1 = NUM_CPUS
    if len(gdf) < cpus_minus_1:
        cpus_minus_1 = len(gdf) 

    if len(gdf) > 0:
        batch_size = int(len(gdf) / cpus_minus_1) + 1
        chunk_iterator = []
        for i in range(cpus_minus_1):
            mn, mx = i * batch_size, (i + 1) * batch_size
            chunk = gdf[mn:mx]
            gdf_args = chunk, IDfield
            chunk_iterator.append(gdf_args)
        
        pool = mp.Pool(processes=cpus_minus_1)
        results = pool.map(getWidth, chunk_iterator)
        pool.close()

        fin = []
        for r in results:
            fin += r
        return fin
    else:
        return []  

def getWidth(args):
    """
    Method: getWidth()
    Purpose: Find all segments with a width less than 72m.
    Params: gdf - geodataframe of segments to find widths for
            IDfield - the field name to record for segments with widths < 72m
    Returns: wind_break - list of IDs for segments with <72m width
    """
    gdf, IDfield = args
    wind_break = []
    for idx, row in gdf.iterrows():
        minx, miny, maxx, maxy = row['geometry'].bounds
        bounds = (minx, miny, maxx, maxy)
        maxDist, maxPt = findCenter(row['geometry'], bounds, True) # first pass
        if maxDist*2 < 72: #only need to do second pass if first pass is under threshold
            mx_b = maxPt.bounds[0:2]
            bounds = (mx_b[0] - maxDist, mx_b[1] - maxDist, mx_b[0] + maxDist, mx_b[1] + maxDist)
            if bounds[0] < 0:
                bounds = (0.0, bounds[1], bounds[2], bounds[3])
            if bounds[1] < 0:
                bounds = (bounds[0], 0.0, bounds[2], bounds[3])
            maxDist, maxPt = findCenter(row['geometry'], bounds, False) # second pass

            if maxDist*2 < 72:
                wind_break.append(row[IDfield])
    return wind_break

def findCenter(poly, bounds, firstPass):
    """
    Method: findCenter()
    Purpose: Create a grid of points based on polygons bounding box, find the point within the poly with the greatest
            minimum distance to polygon edge and return it and its distance.
    Params: poly - shapely polygon
            fact - factor to alter spacing by (different for first and second pass)
            bounds - tuple of corners of bounding box
    Returns: maxDist - max minumum distance to edge from the grid of points
            maxPt - Shapely Point from the grid that is farthest from edge
    Steps:
        1. Get height and width of poly bounding box and divide by divDims
            to get number of points by height and width
        2. Use number of points and grid to determine spacing of points
        3. Create shapely points and store in list
    """
    minx, miny, maxx, maxy = bounds
    width = maxx - minx
    height = maxy - miny

    wSpace, hSpace = 1, 1 #1 meter increments
    #if it is first pass (orig poly bb) - get proportion of poly area and bbox area
    if firstPass:
        poly_area = poly.area
        bb_area = width * height
        prop = poly_area / bb_area
        if prop > 0.8:
            wSpace, hSpace = width / 15, height / 15
        elif prop > 0.5:
            wSpace, hSpace = width / 25, height / 25
        else:
            wSpace, hSpace = width / 50, height / 50
    else:
        wSpace, hSpace = width / (25 / 1.4), height / (25 / 1.4)

    edge = poly.exterior
    maxDist = 0
    maxPt = Point(0,0)

    for i in range(0, int(width/wSpace) + 2):
        for j in range(0, int(height/hSpace) + 2):
            p = Point(minx+(i*wSpace), miny+(j*hSpace))
            if poly.contains(p): #point is in poly - get distance to edge
                d = edge.distance(p)
                if d > maxDist: #distance is largest so far - record it
                    maxDist = d
                    maxPt = p
                    if maxDist* 2 >= 72: # stop looping if it exceeds the min threshold
                        return maxDist, maxPt

    return maxDist, maxPt
