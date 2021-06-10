from multiprocessing.spawn import freeze_support
import time
import osgeo
from osgeo import ogr
import os
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, shape
import rasterio
from rasterio import features
from rasterio.enums import Resampling
from rasterio.windows import Window
import fiona
from fiona.crs import from_epsg
from fiona.transform import transform_geom
from shapely.geometry import mapping, shape
from shapely.validation import explain_validity
import gdal
import numpy as np
import argparse
import sys

from sqlalchemy.sql.functions import count
sys.path.append('../')
from lib import dataPrepHelper
from lib import extraHelper
from lib import sysHelper
from shapely import wkt
from shapely.geometry import mapping, shape, MultiPolygon
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import psycopg2
import concurrent.futures
import dask
from dask.distributed import Client
from dask.distributed import wait


system = sysHelper.commandLine()
            
helper = dataPrepHelper.DataPrepHelper()
time_helper = extraHelper.ExtraHelper()

if __name__ == '__main__':

    project_folder = system.project_folder
    county = system.county
    anci_folder = system.anci_folder

    print('Project data is located here:', project_folder, "This county's data is being prepped:", county)
    st = time.time()
    print('Start time:', st)
    # define parameters to pass to the dataPrepHelper
    p = {
        "gdb":'{0}/{1}/input/segments.gdb'.format(project_folder,county),
        "county":county,
        "fields":['Id', 'Class_name'],
        "gpkg":'{0}/{1}/input/segments.gpkg'.format(project_folder,county)
    }

    # Check and make sure that the GeoDatabase exists
    helper.is_connect(p)
    print('creating a geopackage')
#     #Take the params defined above and make the geopackage, this will return the path to the created geopackage
    st = time.time() #Start timer for creating geopackage
    x = helper.gdbTogpkg(p)
    time_helper.etime(county,'created geopackage', st)

# # #Explode the polygons
    print('exploding polygons from geopackage')
    st=time.time() #start the timer for explode polygons
    lc_segs, after_len = helper.explodeMultipolygon(x)
    lc_segs['SID']= [int(x) for x in range(1, after_len+1)]
    time_for_explode = time_helper.etime(county,'exploded segs and assigned unique id', st)

#     # # do same thing with parcel dataset
#     # read in parcels
    parcels_path1 = '{0}/{1}/input/parcels.shp'.format(project_folder, county)
    st=time.time() #start timer for doing land parcels
    print('exploding parcels polygon layer')
    parcels, after_len = helper.explodeMultipolygonShape(parcels_path1, ['OBJECTID', 'geometry'])
    parcels['PID']= [int(x) for x in range(1, after_len+1)]
    time_helper.etime(county,'exploded parcels and assigned unique id', st)


# # # # # # # # #     # # # *********************************************************************************
    snap_ras = '{0}/{1}/input/{1}_landcover_2017_June2021.tif'.format(project_folder,county) 
    out_ras = '{0}/{1}/input/{1}_landcover_2017_TEMP_PARCELS.tif'.format(project_folder,county)



    st=time.time() #start timer for creating a temp raster

    # Create a temp raster
    print('creating a temp raster with parcels')
    ras_arr = helper.prepRaster(snap_ras, parcels, 'PID', out_ras)
    time_helper.etime(county,'parcels rasterized', st)
    
    # Create a vector geopackage from the temp raster
    print('creating a vector from the temp raster')
    st = time.time()
    parcels = helper.vectorizeRaster(ras_arr, out_ras)
    time_helper.etime(county,'parcels raster is vectorized', st)

    print('sending parcels and segments to database tables')
    st = time.time()
    segs_woked = helper.to_database(lc_segs,'lc_segs')
    print('Segments sent to DB table:',segs_woked)
    parcs_worked = helper.to_database(parcels,'parcels')
    print('Parcels sent to DB table:',parcs_worked)

    time_helper.etime(county,'parcels and lc_segs to database tables', st)
    print('starting the union in SQL') 
    st = time.time()
    helper.db_overlay('psegs')
    time_helper.etime(county,'created psegs',st)
    so_gdf = helper.fix_SID('psegs')
    time_helper.etime(county,'validated geometries',st)


    so_gdf['PSID']= [int(x) for x in range(1, len(so_gdf)+1)]
    time_helper.etime(county, 'assigned PSIDs to all psegs geometries', st)

    out_ras_PID = '{0}/{1}/input/ps_parcels.tif'.format(project_folder,county)
    out_ras_SID = '{0}/{1}/input/ps_segs.tif'.format(project_folder,county)


    print('making a raster based on parcels')
    st = time.time()
    helper.prepRaster(snap_ras, so_gdf, 'pid',out_ras_PID)
    time_helper.etime(county, 'Raster created based on PID', st)

    st = time.time()
    print('making a raster based on segments')
    helper.prepRaster(snap_ras, so_gdf, 'sid',out_ras_SID)
    time_helper.etime(county, 'Raster created based on SID', st)


# Look for  _landcover_, 
    tab_dict = {f'P:/{county}/input/{county}_landcover_2017_June2021.tif':[{'PID':'p_lc'}],
    f'{project_folder}/{anci_folder}/CDL/CDL_2017_2019_4class_maj_1m.tif':[{'SID':'s_c1719'},{'PID':'p_c1719'}],
    f'{project_folder}/{anci_folder}/CDL/cdl_2018_4class_maj_1m.tif':[{'SID':'s_c18'},{'PID':'p_c18'}],
    f'{project_folder}/{anci_folder}/NLCD/NLCD_2016_pashay_maj_1m.tif':[{'SID':'s_n16'},{'PID':'p_n16'}],
    f'{project_folder}/{county}/input/cbp_lu_mask.tif':[{'SID':'s_luz'},{'PID':'p_luz'}]
    }

    zone_dict = {
        'SID': out_ras_SID,
        'PID': out_ras_PID
    }

    st = time.time()
    helper.tabulateAll(tab_dict,zone_dict)
    # client = Client(processes = False)
    # results = []
    # tab_list = list(tab_dict.keys())
    # idx = 0
    # for tab in tab_list:
    #     print(tab_dict[tab])
    #     functions = list(tab_dict[tab][idx].keys())
    #     for f in functions:
    #         if f == 'SID':
    #             results.append((tab,zone_dict['SID'],tab_dict[tab][idx]['SID']))
    #             print('future object created')
    #         if f == 'PID':
    #             results.append((tab,zone_dict['PID'],tab_dict[tab][idx]['PID']))
    #             print('future object created')
    # futures = client.map(helper.tabulateArea,results)
    # wait(futures)
    # client.close()
    # time_helper.etime(county, 'all tabulate areas have been calculated', st)

    st = time.time()
    print('psegs and parcels to geopackage')

    client = Client(processes = False)
    dfs = []
    dfs.append(("{0}/{1}/input/psegs.gpkg".format(project_folder,county),'psegs'))
    dfs.append(("{0}/{1}/input/temp_dataprep.gpkg".format(project_folder,county),'vectorized_parcels'))
    futures = client.map(helper.to_gpkg,dfs)
    wait(futures)
    # helper.to_gpkg(so_gdf,"{0}/{1}/input/psegs.gpkg".format(project_folder,county),'psegs')
    # helper.to_gpkg(parcels,"{0}/{1}/input/temp_dataprep.gpkg".format(project_folder,county),'vectorized_parcels')


    print('dropping temporary tables, all other data has been prepared')
    conDB = psycopg2.connect(host='localhost',
    database='landuse_gis',
    user='postgres',
    password = 'landuse_dev')
    cursor = conDB.cursor()
    sql_statement = '''DROP TABLE IF EXISTS "psegs"'''
    cursor.execute(sql_statement)
    sql_statement = '''DROP TABLE IF EXISTS "parcels"'''
    cursor.execute(sql_statement)
    sql_statement = '''DROP TABLE IF EXISTS "lc_segs"'''
    cursor.execute(sql_statement)
    conDB.commit()
    cursor.close()
    conDB.close()
    print('this county',county, 'has been prepared, machine is ready for next county. goodbye :)')
    sys.exit()