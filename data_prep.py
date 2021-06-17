import time
from typing import OrderedDict
import osgeo
from osgeo import ogr
import os
import pandas
import geopandas as gpd
import rasterio
from shapely.geometry import Polygon, MultiPolygon, shape, mapping
from shapely.ops import unary_union
from fiona.crs import from_epsg
from fiona.transform import transform_geom
import rasterio
from rasterio import features
from rasterio.transform import from_bounds
from rasterio.coords import BoundingBox as bbox
from rasterio.features import shapes
from rasterio.enums import Resampling
from rasterio.windows import Window
from rasterio.io import MemoryFile
from collections import defaultdict
import fiona

import numpy as np
import math
import sys
import collections
import itertools
from itertools import chain
import geoalchemy2
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import psycopg2
import concurrent.futures
import dask
from dask.distributed import Client
from dask.distributed import wait
import qgis
from qgis.core import *
import argparse
import sys
import fnmatch



class commandLine:
    def __init__(self):
        if len(sys.argv) == 1:
            print('This program requires 3 arguments: project folder command -p, county code -c, and ancillary folder -a in order to run')
            sys.exit()

        parser = argparse.ArgumentParser(description='Prepare data using open source libraries')
        parser.add_argument('-p',
        action = "store",
        help ='The path to your project folder, which will contain the county and its data',
        )

        parser.add_argument('-c',
        action = "store",
        help ='The name of the county with code, for example: cumb_42041',
        )

        parser.add_argument('-a',
        action = "store",
        help ='The ancillary folder which will contain rasters for tabulate area',
        )

        args = parser.parse_args()

        if args.c == None:
            print('This program requires 3 arguments: project folder command -p, county code -c, and ancillary folder -a in order to run')
            sys.exit()

        if args.p == None:
            print('This program requires 3 arguments: project folder command -p, county code -c, and ancillary folder -a in order to run')
            sys.exit()

        if args.a == None:
            print('This program requires 3 arguments: project folder command -p, county code -c, and ancillary folder -a in order to run')
            sys.exit()

        self.project_folder = args.p
        self.county = args.c
        self.anci_folder = args.a


class DataPrepHelper:
  def __init__(self):
    return None
  
  def rasFinder(self, dir, pattern):
    files = os.listdir(dir)
    for entry in files:
        if fnmatch.fnmatch(entry, pattern):
            print(f"Found raster: {entry}")
            foundRas = f'{dir}/{entry}'
            break
    if not os.path.exists(foundRas):
        print(f"ERROR! Failed to open foundRas: {foundRas}\n--dir: {dir}\n--pattern: {pattern}" )
        sys.exit()
    return foundRas

  def is_connect(self, params):
    self.params = params
    gdb_path = params['gdb']
    in_driver= ogr.GetDriverByName("OpenFileGDB")
    try:
      gdb = in_driver.Open(gdb_path, 0)
      print('gdb exists')
    except Exception as e:
        print (e, 'gdb does not exist')
    return True
  
  def validate_layer(self,layer_info):
    self.layer_info = layer_info
    layer_path = layer_info['layer_path']
    layer_name = layer_info['layer_name']
    qgs = QgsApplication([], False)

    qgs.initQgis()

    # # initialize processing to access native QGIS algorithms
    from qgis import processing
    from processing.core.Processing import Processing
    from qgis.analysis import QgsNativeAlgorithms
    Processing.initialize()
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
    if '.gdb' in layer_path:
      in_layer = f"{layer_path}|layername={layer_name}"
    elif '.gpkg' in layer_path:
      in_layer = f"{layer_path}|layername={layer_name}"
    else:
      in_layer = f"{layer_path}"
    
    med_layer = f'{project_folder}/{county}/input/temp_{layer_name}.gpkg'
    final_layer = f'{project_folder}/{county}/input/{layer_name}_FINAL.gpkg'
    fix_params = { 
      'INPUT' : QgsProcessingFeatureSourceDefinition(source=in_layer, 
      selectedFeaturesOnly=False, 
      featureLimit=-1, 
      flags=QgsProcessingFeatureSourceDefinition.FlagOverrideDefaultGeometryCheck, 
      geometryCheck=QgsFeatureRequest.GeometryNoCheck), 
      'OUTPUT' : med_layer
      }

    operations = {
        'fix': "native:fixgeometries",
        'single': "native:multiparttosingleparts",
    }

    feedback = QgsProcessingFeedback()

    result = processing.run(operations['fix'], fix_params,feedback=feedback)

    single_params = {
      'INPUT':med_layer,
      'OUTPUT': final_layer
    }
    result = processing.run(operations['single'], single_params,feedback=feedback)

    qgs.exitQgis()

    return final_layer

  def qgisrastervector(self, raster_layer,out_path):
    self.raster_layer = raster_layer
    self.out_path = out_path
    
    qgs = QgsApplication([], False)

    qgs.initQgis()
    from qgis import processing
    from processing.core.Processing import Processing
    from qgis.analysis import QgsNativeAlgorithms
    Processing.initialize()
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

    params = {
      'INPUT_RASTER':raster_layer,
      'RASTER_BAND':1,
      'FIELD_NAME':'VALUE',
      'OUTPUT':out_path
    }
    feedback = QgsProcessingFeedback()
    processing.run("native:pixelstopolygons", params,feedback = feedback)
    qgs.exitQgis()
    return out_path


  def prepRaster(self, input_path, parcels_gpd, target_field, output_path):
    self.input_path = input_path
    self.parcels_gpd = parcels_gpd
    self.target_field = target_field
    self.output_path = output_path
    rst = rasterio.open(input_path)
    meta = rst.meta.copy()
    meta.update(compress='lzw', dtype='int32') #trying different dtypes here
    # windows= rst.block_windows(1)
    with rasterio.open(output_path, 'w+', **meta) as out:
      # Maybe just write profile instead of building new raster
        # for idx, window in windows:
      out_arr = out.read(1)
      # this is where we create a generator of geom, value pairs to use in rasterizing
      if target_field == 'PID':
        shapes1 = ((geom,value) for geom, value in zip(parcels_gpd.geometry, parcels_gpd.PID))
      if target_field == 'pid':
        shapes1 = ((geom,value) for geom, value in zip(parcels_gpd.geom, parcels_gpd.pid))
      if target_field == 'SID':
        shapes1 = ((geom,value) for geom, value in zip(parcels_gpd.geometry, parcels_gpd.SID))
      if target_field == 'sid':
        shapes1 = ((geom,value) for geom, value in zip(parcels_gpd.geom, parcels_gpd.sid))
      par_ras = features.rasterize(shapes=shapes1, fill=0, out=out_arr, transform=out.transform)
      out.write_band(1, par_ras)
    out.close()
    return out_arr

  def vectorizeRaster(self, unique_array, raster_path):
    self.unique_array = unique_array
    self.raster_path = raster_path
    with rasterio.open(raster_path) as src:
      transform = src.transform
    unique_array = unique_array.astype(np.int32)
    zones, geoms = [], [] #zones will be LC raster values and geoms will be Polygons
    for i, (s, v) in enumerate(shapes(unique_array, mask=unique_array.astype(bool) , connectivity=4, transform=transform)): 
        geoms.append(Polygon(s['coordinates'][0]))
        zones.append(v)
    zones_gdf = gpd.GeoDataFrame(data={'zone':zones}, geometry=geoms, crs="EPSG:5070")
    zones_gdf['PID'] = [int(x) for x in range(1, len(zones_gdf)+1)] #id for sjoin
    return zones_gdf

  def to_database(self, geo_dict):
    self.geo_dict = geo_dict
    geo_df = geo_dict['geo_df']
    name = geo_dict['name']
    engine = create_engine(f'postgresql://{postgres_user}:{postgres_pass}@{postgres_host}:5432/{postgres_db}')
    geo_df['geom'] = geo_df['geometry'].apply(lambda x: WKTElement(x.wkt, srid=5070))
    geo_df.drop('geometry', 1, inplace=True)
    print('got to dropping duplicative column')
    geo_df.to_sql(name, engine, if_exists='replace', index=False, 
                            dtype={'geom': Geometry('POLYGON', srid= 5070)})
    return True


  def db_overlay(self, new_table):
    conDB = psycopg2.connect(host=postgres_host,
    database=postgres_db,
    user=postgres_user,
    password = postgres_pass)
    cursor = conDB.cursor()
    self.new_table = new_table
    sql_statement = '''CREATE TABLE if not exists {0}(SID bigint, Class_name text, PID bigint, geom geometry)'''.format(new_table)
    cursor.execute(sql_statement)
    sql_statement = '''CREATE INDEX IF NOT EXISTS idx_parcelsPID on "parcels"("PID")'''
    cursor.execute(sql_statement)
    sql_PID = '''SELECT MAX("PID") from "parcels"'''
    cursor.execute(sql_PID)
    max_pid = cursor.fetchall()
    max_pid = max_pid[0]
    max_pid = max_pid[0]
    conDB.commit()
    cursor.close()
    conDB.close()
    sql_statements = []
    num = math.ceil(max_pid/2500)
    for x in range(num):
      if x > 0:
        w = x-1
        if x < num:
          sql_statements.append('''INSERT INTO {0}(SID,Class_name, PID, geom) SELECT
          "lc_segs"."SID",
          "lc_segs"."Class_name",
          "parcels"."PID",
          ST_Intersection("lc_segs".geom,"parcels".geom) as geom
          FROM "lc_segs", "parcels"
          WHERE "lc_segs".geom && "parcels".geom AND ST_Intersects("lc_segs".geom, "parcels".geom) and "parcels"."PID" > {1} and "parcels"."PID" < {2}'''.format(new_table,w*2500,x*2500))

    client = Client(processes=False)
    futures = client.map(curser, sql_statements)
    wait(futures)
    client.close()
    print('overlay complete')
    return True


  def read_psegs(self, new_table):
    self.new_table = new_table
    conDB = psycopg2.connect(host=postgres_host,
    database=postgres_db,
    user=postgres_user,
    password = postgres_pass)
    cursor = conDB.cursor()
    print('reading in psegs as a geodataframe')
    sql = "select * from {0}".format(new_table)
    df = gpd.read_postgis(sql, conDB, crs = 5070)
    cursor.close()
    conDB.close()
    return df

  def to_gpkg(self,geo_df, outpath, layer_name):
    self.geo_df = geo_df
    self.outpath = outpath
    self.layer_name = layer_name
    geo_df.to_file(outpath, layer=layer_name, driver='GPKG')
    return True


class ExtraHelper:
  def __init__(self):
    return None

  def etime(self, cf, note, starttime):
    self.cf = cf
    self.note = note
    self.starttime = starttime
    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    # f = open(f"{project_folder}/{cf}/etime_log.txt", "a")
    if elapsed > 60:
        # f.write(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n\n')
        print(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}')
    else:
        # f.write(f'{cf}--{note} runtime - {round(elapsed,2)} sec\n\n')
        print(f'{cf}--{note} runtime - {round(elapsed,2)} sec')
    # f.close()


system = commandLine()
project_folder = system.project_folder
county = system.county
anci_folder = system.anci_folder

postgres_db = 'landuse_gis' 
postgres_user = 'postgres'
postgres_pass = 'landuse_dev'
postgres_host = 'localhost'

def curser(params):
  conDB = psycopg2.connect(host=postgres_host,
  database=postgres_db,
  user=postgres_user,
  password = postgres_pass)
  cursor = conDB.cursor()
  rezzy = cursor.execute(params)
  conDB.commit()
  cursor.close()
  conDB.close()
  return rezzy
            
helper = DataPrepHelper()
time_helper = ExtraHelper()

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

# # # # # # # # # # #     # # # *********************************************************************************
    snap_ras = helper.rasFinder(f"{project_folder}/{county}/input",f"{county}_landcover_*.tif") 
    out_ras = '{0}/{1}/input/{1}_landcover_TEMP_PARCELS.tif'.format(project_folder,county)

    print('reading in parcels')
    parcels_path1 = '{0}/{1}/input/parcels.shp'.format(project_folder, county)
    parcels = gpd.read_file(parcels_path1) 
    parcels['PID']= [int(x) for x in range(1, len(parcels)+1)]
    st=time.time() #start timer for creating a temp raster
    
    ### Create a temp raster
    print('creating a temp raster with parcels')
    ras_arr = helper.prepRaster(snap_ras, parcels, 'PID', out_ras)
    time_helper.etime(county,'parcels rasterized', st)
    
    # Create a vector geopackage from the temp raster
    print('creating a vector from the temp raster')
    st = time.time()
    parcels_gpkg = helper.qgisrastervector(out_ras, f"{project_folder}/{county}/input/vectorized_parcels.gpkg")
    # parcels = helper.vectorizeRaster(ras_arr, out_ras)
    # parcels.to_file('{0}/{1}/input/temp_dataprep.gpkg'.format(project_folder,county), layer = 'vectorized_parcels',driver = 'GPKG')
    time_helper.etime(county,'parcels raster is vectorized and sent to gpkg', st)
    st = time.time()
    print('fixing geometries for segments and parcels')

    layer_info_segs = {
      'layer_path':f'{project_folder}/{county}/input/segments.gdb',
      'layer_name':'lc_segs'
    }
    layer_info_parcels = {
      'layer_path':parcels_gpkg,
      'layer_name':'vectorized_parcels'
    }
    segs_layer = helper.validate_layer(layer_info_segs)
    print('segments validated')
    parcs_layer = helper.validate_layer(layer_info_parcels)
    print('parcels validated')

    time_helper.etime(county, 'parcels and segments geometries fixed', st)

    lc_segs = gpd.read_file(segs_layer)
    lc_segs['SID']= [int(x) for x in range(1, len(lc_segs)+1)]
    parcels = gpd.read_file(parcs_layer)

    print('sending parcels and segments to database tables')
    st = time.time()

    segs_dict = {
      'geo_df':lc_segs,
      'name':'lc_segs'
    }
    parcs_dict = {
      'geo_df':parcels,
      'name':'parcels'
    }
    segsw = helper.to_database(segs_dict)
    print('segments to DB:',segsw)
    parcsw = helper.to_database(parcs_dict)
    print('parcels to DB:',parcsw)


    time_helper.etime(county,'parcels and lc_segs to database tables', st)
    print('starting the union in SQL') 
    st = time.time()
    helper.db_overlay('psegs')
    time_helper.etime(county,'created psegs',st)
    
    
    st = time.time()
    print('reading in psegs, adding PSIDs, validating all geometries, and writing to gpkg')
    so_gdf = helper.read_psegs('psegs')

    so_gdf['PSID']= [int(x) for x in range(1, len(so_gdf)+1)]

    helper.to_gpkg(so_gdf,"{0}/{1}/input/psegs_temp.gpkg".format(project_folder,county),'psegs')

    psegs_path = helper.validate_layer({'layer_path':"{0}/{1}/input/psegs_temp.gpkg".format(project_folder,county),
    'layer_name':'psegs'})

    time_helper.etime(county, 'assigned all PSIDs, validated all geometries, and written to gpkg', st)

    out_ras_PID = '{0}/{1}/input/ps_parcels.tif'.format(project_folder,county)
    out_ras_SID = '{0}/{1}/input/ps_segs.tif'.format(project_folder,county)

    so_gdf = gpd.read_file(psegs_path)
    print('making a raster based on parcels')
    st = time.time()
    helper.prepRaster(snap_ras, so_gdf, 'pid',out_ras_PID)
    time_helper.etime(county, 'Raster created based on PID', st)


    st = time.time()
    print('making a raster based on segments')
    helper.prepRaster(snap_ras, so_gdf, 'sid',out_ras_SID)
    time_helper.etime(county, 'Raster created based on SID', st)

    print('dropping temporary tables, all other data has been prepared')
    conDB = psycopg2.connect(host='localhost',
    database=postgres_db,
    user=postgres_user,
    password = postgres_pass)
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