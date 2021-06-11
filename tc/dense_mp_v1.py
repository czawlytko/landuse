# python -W ignore /media/imagery/usgs_sc/smcdonald/Scripts/dense_mp_14_cnty.py

"""
Script: dense_mp_14_cnty.py
Purpose: Separate the landscape into 4 environment types: forest (3), dense (1), less dense (2) 
         and agricultural (4). Dense will be determine using census urban area data. Forested 
         will include parcels that are at least 25% forest, contain a structure, and do not contain
         any agriculture. Agricultural are any parcels containing crop, pasture, or orchard/vineyard lu
         classes. Less Dense will be all parcels that are left after the other 3 environment types have
         been determined. Roads will be classes as 0.
Authors: Sarah McDonald, Geographer, U.S. Geological Survey, smcdonald@chesapeakebay.net
         Labeeb Ahmed, Geographer, U.S. Geological Survey, lahmed@chesapeakebay.net
NOTE: This version is to be used for the 14 trial counties. This version excludes Patrick's additional
      dense methods.
"""



import os
import sys
import time
import geopandas as gpd
import pandas as pd
import multiprocessing as mp
from shapely.geometry import shape, box
from shapely.ops import unary_union
from timeit import default_timer as timer
import itertools

class dense:
    def runDense(parcels_gpkg, parcels_layer, urban_shp):
        print(parcels_gpkg, parcels_layer, urban_shp)
        start = float(time.time())
        parcels = readPolys(parcels_gpkg, parcels_layer)
        bnds = parcels.total_bounds
        cnty_mask = (bnds[0], bnds[1], bnds[2], bnds[3])
        urban = readPolysMask(urban_shp, '', cnty_mask)
        if len(urban) > 0:
            urban = checkGeoms(urban)
            parcels = checkGeoms(parcels)
            densePIDs = dense.getDense(parcels, urban)
            return densePIDs
        else:
            print("No urban polys - exiting")
            sys.exit(1)

    def spatial_join(arg):
        """
        Method: spatial_join()
        Purpose: Run spatial join on 2 geodataframes and return unique PIDs.
        Params: arg:
                    gdf - geodataframe of data (parcels)
                    overlay - geodataframe of data (census urban area)
                    operation - string for op argument of sjoin (intersects)
        Returns: list of PIDs that intersect census urban area
        """
        gdf, overlay, operation, idField = arg

        tmp_spatial_join = gpd.sjoin(gdf, overlay, how='inner', op=operation)

        return tmp_spatial_join.PID.to_list()

    def create_chunk_iterator(gdf, tuple_args):
        """
        Method: create_chunk_iterator()
        Purpose: Create chunks of data and create iterator to be used in mapping
                data to processors.
        Params: gdf - geodataframe of data to create chunks for
                tuple_args - tuple of arguments needed to add for each entry in the iterator after the
                            chunk of gdf
        Returns: chunk_iterator - list of tuples, where each tuple contains the a chunk of gdf and 
                                    the remaining arguments needed for the function.
        """
        cols = list(gdf) #get columns
        gdf = gdf.reset_index() #reset index so we can split by index
        gdf = gdf[cols] #ignore new column if it was created
        cpus_minus_1 = mp.cpu_count() - 1 #testing
        batch_size = int(len(gdf) / cpus_minus_1) + 1
        chunk_iterator = []
        for i in range(cpus_minus_1):
            mn, mx = i * batch_size, (i + 1) * batch_size #even chunks 
            chunk = gdf[mn:mx] #separate
            gdf_args = (chunk,) + tuple_args
            chunk_iterator.append(gdf_args)
        
        return chunk_iterator

    def getDense(parcels, urban):
        """
        Method: getDense()
        Purpose: Find parcels intersecting census urban areas and return their PIDs
        Params: parcels - geodataframe of parcels 
                urban - geodataframe of census urban area polygons
        Returns: pids - list of PIDs intersecting census urban area
        """

        # filter parcel columns
        parcels = parcels[['PID', 'geometry']]

        # create iterator 
        args = dense.create_chunk_iterator(parcels, (urban, "intersects", 'PID'))

        # multiprocessing
        iterations = mp.cpu_count() - 1 #tested ~50% cpu usage is fastest
        pool = mp.Pool(processes=iterations)
        res = pool.map(dense.spatial_join, args) 
        pool.close()

        results = []
        for r in res:
            results += r
        
        return results

def readPolys(gdbPath, layerName):
    """
    Method: ReadPolys()
    Purpose: Read data from shapefile, geodatabase, or geopackage into a geodataframe.
    Arguments: gdbPath - path to shapefile, geodatabase or geopackage
                layerName - name of layer in geodatabase or geopackage (not used for shapefile)
    Returns: lcSegGDF - geodataframe of data
    """
    if gdbPath[-3:] == 'gdb' or gdbPath[-4:] == 'gpkg':
        if os.path.isdir(gdbPath) or os.path.isfile(gdbPath):
            lcSegGDF = gpd.read_file(gdbPath, layer=layerName, crs="EPSG:5070")#, bbox=mask)
        else:
            print("The geodatabase does not exist: ", gdbPath)
            sys.exit()
    elif gdbPath[-3:] == 'shp':
        if os.path.isfile(gdbPath):
            lcSegGDF = gpd.read_file(gdbPath, crs="EPSG:5070")#, bbox=mask)
        else:
            print("The shapefile does not exist: ", gdbPath)
            sys.exit()   
    else:
        print("Expecting shapefile (shp) OR ")
        print(layerName, " as file stored in geodatabase (gdb) ", gdbPath)
        sys.exit()
    if lcSegGDF.crs == {}:
        lcSegGDF.crs = 'epsg:5070'
    return lcSegGDF #ONLY FOR TESTING - SHOULD NOT NEED THIS AFTER FIX GEOMS IS CORRECT

def readPolysMask(gdbPath, layerName, mask):
    """
    Method: ReadPolysMask()
    Purpose: Read data from shapefile, geodatabase, or geopackage into a geodataframe.
    Arguments: gdbPath - path to shapefile, geodatabase or geopackage
                layerName - name of layer in geodatabase or geopackage (not used for shapefile)
    Returns: lcSegGDF - geodataframe of data
    """
    if gdbPath[-3:] == 'gdb' or gdbPath[-4:] == 'gpkg':
        # print("Starting ", layerName, "...")
        if os.path.isdir(gdbPath) or os.path.isfile(gdbPath):
            lcSegGDF = gpd.read_file(gdbPath, layer=layerName, bbox=mask, crs="EPSG:5070")
        else:
            print("The geodatabase does not exist: ", gdbPath)
            sys.exit()
    elif gdbPath[-3:] == 'shp':
        # print("Starting ", gdbPath.split('/')[-1], "...")
        if os.path.isfile(gdbPath):
            lcSegGDF = gpd.read_file(gdbPath, bbox=mask, crs="EPSG:5070")
        else:
            print("The shapefile does not exist: ", gdbPath)
            sys.exit()   
    else:
        print("Expecting shapefile (shp) OR ")
        print(layerName, " as file stored in geodatabase (gdb) ", gdbPath)
        sys.exit()
    if lcSegGDF.crs == {}:
        # print("Empty crs - defining as epsg 5070")
        lcSegGDF.crs = 'epsg:5070'
    return lcSegGDF

def checkGeoms(gdf):
    """
    Method: checkGeoms()
    Purpose: Verify dissolved/exploded geoms are valid, if not it corrects them
    Params: gdf - geodataframe to validate
    returns: gdf - geodataframe with valid geoms
    """
    #make sure index is unique
    cols = list(gdf)
    gdf = gdf.reset_index()
    gdf = gdf[cols]
    #df to store fixed geoms
    newGeoms = gpd.GeoDataFrame(columns=cols, crs=gdf.crs)
    #list to store indexes of geoms to remove
    idxToRemove = []
    for idx, row in gdf.iterrows():
        geom = shape(row['geometry'])
        if not geom.is_valid:
            clean = geom.buffer(0.0)
            assert clean.is_valid
            idxToRemove.append(idx)
            toAdd = [] #build list for row entry
            for c in cols:
                if c != 'geometry':
                    toAdd.append(row[c])
                else:
                    toAdd.append(clean)
            newGeoms.loc[len(newGeoms)] = toAdd #add row with updated geom
    gdf.drop(gdf.index[idxToRemove], inplace=True) #remove rows we updated
    gdf = gdf.append(newGeoms) #append fixed geoms
    gdf = gdf.reset_index() #reset index
    gdf = gdf[cols] #remove new index column
    return gdf

def checkForest(gdf):
    """
    Method: checkForest()
    Purpose: Select parcels that contain a building and are in a forested landscape based on forest % and absence of ag data
    Params: gdf - pseg geodataframe
    Returns: forParcels - list of PID (parcel IDs) meeting forested parcel requirements
    """
    #calculate total parcel area
    t = gdf[['PID', 'geometry']]
    t.loc[:,'p_area'] = t.geometry.area
    t = t.groupby(['PID']).sum() #total forest area by parcel
    t = t.reset_index()
    gdf = gdf.merge(t, on='PID', how='left')
    del t
    #calculate total forest area
    tmp = gdf[gdf['Class_name'] == 'Tree Canopy'] #copy tree canopy segments 
    tmp.loc[:,'ps_area'] = tmp.geometry.area
    tmp = tmp[['PID', 'ps_area']]
    tmp = tmp.groupby(['PID']).sum() #total forest area by parcel
    tmp = tmp.reset_index()
    #get % forest area by parcel
    tmp = pd.merge(tmp, gdf[['PID', 'p_area']], on='PID', how='left') #p_area is same for all segments in a parcel -- tmp holds parcels with forest
    tmp.loc[:,'pctFor'] = tmp['ps_area'] / tmp['p_area']
    forParcels = list(set(list(tmp[tmp['pctFor'] >= 0.25]['PID']))) #unique list of parcel IDs who meet criteria for analysis
    return forParcels

def runEnviro(psegs, densePIDs):
    """
    Method: runEnviro()
    Purpose: Class all segments environment type by PIDs and store in new column 'EnvType' in psegs.
            0 - roads
            1 - dense
            2 - less dense
            3 - forested
            4 - agriculture
    Params: psegs - gdf of full psegs
            densePIDs - list of dense PIDs according to census urban area
    Returns: psegs - psegs with new EnvType column
    """
    #create environment column
    psegs.loc[:,'EnvType'] = [0 for x in range(len(psegs))]
    #Class ag parcels
    allLUS = list(set(list(psegs['lu'])))
    allAg = []
    # LU landuse land use values
    oldAg = ['Cropland', 'Orchard/Vineyard', 'Pasture']  # ['crop', 'orchard vineyard', 'pasture']  
    for a in allLUS:
        al = a
        if a == None:
            al = 'None'
        al = al.lower()
        for o in oldAg:
            if o in al:
                allAg.append(a)
    agPar = list(psegs[psegs['lu'].isin(allAg)]['PID'])
    psegs.loc[psegs['PID'].isin(agPar), 'EnvType'] = 4
    del agPar
    #class dense parcels (ag and roads excluded)
    psegs.loc[(psegs['EnvType'] == 0) & (psegs['PID'].isin(densePIDs)), 'EnvType'] = 1
    #get forested PIDs
    forest = checkForest(psegs[psegs['EnvType'] == 0]) #only need to check where EnvType hasn't been declared
    psegs.loc[(psegs['EnvType'] == 0) & (psegs['PID'].isin(forest)), 'EnvType'] = 3
    del forest
    #remaining non-roads are less dense
    psegs.loc[(psegs['lu'] != 'Roads') & (psegs['Class_name'] != 'Roads') & (psegs['EnvType'] == 0), 'EnvType'] = 2
    #return psegs with new EnvType column
    return psegs



