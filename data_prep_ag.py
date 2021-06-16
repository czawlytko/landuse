import time
from collections import OrderedDict
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
import gdal
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
# from lib import sysHelper
import argparse
import sys


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
  
  def gdbTogpkg(self, params):
    # self.gdb = gdb
    # self.fields = fields
    # self.gpkg = gpkg
    self.params = params
    gdb = params['gdb']
    county = params['county']
    fields = params['fields']
    gpkg = params['gpkg']
    ogr.UseExceptions()
    inDriver = ogr.GetDriverByName("OpenFileGDB")
    inDataSource = inDriver.Open(gdb, 0)
    inLayer = inDataSource.GetLayer()

    # Create the output Layers    
    outDriver = ogr.GetDriverByName("GPKG")

    # Remove output GPKG if it already exists
    if os.path.exists(gpkg):
        outDriver.DeleteDataSource(gpkg)

    # add a coordinate system for the outLayer
    dest_srs = ogr.osr.SpatialReference()
    dest_srs.ImportFromEPSG(5070)

    # Create the output shapefile
    outDataSource = outDriver.CreateDataSource(gpkg)
    out_lyr_name = os.path.splitext( os.path.split( gpkg )[1] )[0]
    outLayer = outDataSource.CreateLayer( out_lyr_name, srs = dest_srs, geom_type=ogr.wkbMultiPolygon ) #changed from ogr.wkbPolygon

    # Add input Layer Fields to the output Layer if it is the one we want
    inLayerDefn = inLayer.GetLayerDefn()
    for i in range(0, inLayerDefn.GetFieldCount()):
        fieldDefn = inLayerDefn.GetFieldDefn(i)
        fieldName = fieldDefn.GetName()
        if fieldName not in fields:
            continue
        outLayer.CreateField(fieldDefn)

    # Get the output Layer's Feature Definition
    outLayerDefn = outLayer.GetLayerDefn()

    # Add features to the ouput Layer
    for inFeature in inLayer:
        # Create output Feature
        outFeature = ogr.Feature(outLayerDefn)

        # Add field values from input Layer
        for i in range(0, outLayerDefn.GetFieldCount()):
            fieldDefn = outLayerDefn.GetFieldDefn(i)
            fieldName = fieldDefn.GetName()
            if fieldName not in fields:
                continue

            outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(),
                inFeature.GetField(i))

        # Set geometry as centroid
        geom = inFeature.GetGeometryRef()
        outFeature.SetGeometry(geom.Clone())

        # Add new feature to output Layer
        outLayer.CreateFeature(outFeature)
        outFeature = None

    # Save and close DataSources
    inDataSource = None
    outDataSource = None

    return gpkg

  def explodeMultipolygon(self, gpkg_path):
    self.gpkg_path = gpkg_path

    #Get the layer name from the path and convert the geopackage to a geopandas dataframe
    layer = os.path.splitext( os.path.split( gpkg_path )[1] )[0]
    lc_segs = gpd.read_file(gpkg_path, layer=layer)

    #explode multipolygon to singe polygon
      #get length
    before_len=len(lc_segs)
    lc_segs=lc_segs.explode()
    #get length

    after_len=len(lc_segs)

    print("Multipolygon of length: ", before_len, " exploded to Polygon of length: ", after_len)
    return lc_segs, after_len

  def explodeMultipolygonShape(self, shp_path, target_fields):
    self.shp_path = shp_path
    self.target_fields = target_fields

    #Convert the shapefile to a geopandas dataframe
    parcels_gpd = gpd.read_file(shp_path)
    parcels_gpd = parcels_gpd[target_fields] 
    #explode multipolygon to singe polygon
      #get length
    before_len=len(parcels_gpd)
    parcels_gpd=parcels_gpd.explode()
    #get length
    after_len=len(parcels_gpd)

    print("Multipolygon of length: ", before_len, " exploded to Polygon of length: ", after_len)
    return parcels_gpd, after_len


  def prepRaster(self, input_path, parcels_gpd, target_field, output_path):
    self.input_path = input_path
    self.parcels_gpd = parcels_gpd
    self.target_field = target_field
    self.output_path = output_path
    rst = rasterio.open(input_path)
    meta = rst.meta.copy()
    meta.update(compress='lzw', dtype='float64') #trying different dtypes here
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
    unique_array = unique_array.astype(np.int16)
    zones, geoms = [], [] #zones will be LC raster values and geoms will be Polygons
    for i, (s, v) in enumerate(shapes(unique_array, mask=unique_array.astype(bool) , connectivity=4, transform=transform)): 
        geoms.append(Polygon(s['coordinates'][0]))
        zones.append(v)
    zones_gdf = gpd.GeoDataFrame(data={'zone':zones}, geometry=geoms, crs="EPSG:5070")
    zones_gdf['PID'] = [int(x) for x in range(1, len(zones_gdf)+1)] #id for sjoin
    return zones_gdf

  def to_database(self, geo_df, name):
    self.geo_df = geo_df
    engine = create_engine(f'postgresql://{postgres_user}:{postgres_pass}@{postgres_host}:5432/{postgres_db}')
    geo_df['geom'] = geo_df['geometry'].apply(lambda x: WKTElement(x.wkt, srid=5070))
    geo_df.drop('geometry', 1, inplace=True)
    print('got to dropping duplicative column')
    geo_df.to_sql(name, engine, if_exists='replace', index=False, 
                            dtype={'geom': Geometry('MULTIPOLYGON', srid= 5070)})
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


  def fix_SID(self, new_table):
    self.new_table = new_table
    conDB = psycopg2.connect(host=postgres_host,
    database=postgres_db,
    user=postgres_user,
    password = postgres_pass)
    cursor = conDB.cursor()
    print('validating all geometries')
    sql_statement = '''update {0} set geom = (select st_transform(st_makevalid(geom), 5070))'''.format(new_table)
    cursor.execute(sql_statement)
    conDB.commit()
    print('validated geometries')
    print('reading in psegs as a geodataframe')
    sql = "select * from {0}".format(new_table)
    df = gpd.read_postgis(sql, conDB, crs = 5070)
    cursor.close()
    conDB.close()
    return df

  def dict2Table(self, tabAreaDict, rasVals, zoneIDName):
    self.tabAreaDict = tabAreaDict
    self.rasVals = rasVals
    self.zoneIDName = zoneIDName
    print(rasVals)
    cols = [str(x) for x in rasVals] #list to store column names
    print(cols)
    df = pandas.DataFrame.from_dict(tabAreaDict, orient='index', columns=cols) # removed someDict and replaced with tabAreaDict
    df[zoneIDName] = df.index
    df = df.sort_values(by=[zoneIDName])
    finCols = [zoneIDName] + cols
    df = df[finCols]
    df.to_csv('{0}/{1}/input/{2}.csv'.format(project_folder,county,zoneIDName))
    return df

  def tabulateArea(self, zoneRaster, valueRaster, idField):
    self.zoneRaster = zoneRaster
    self.valueRaster = valueRaster
    self.idField = idField
    # todo add logging to tabulateArea()
    tabArea = {} #dict to store data in
    with rasterio.open(zoneRaster) as zSrc:
        with rasterio.open(valueRaster) as src:
            # 1. construct window bounds to mask out value raster
            # left, bottom, right, top = zSrc.bounds
            height = zSrc.height
            width = zSrc.width
            # clip_window = from_bounds(left, bottom, right, top, width, height)
            for ji, window in zSrc.block_windows(1):
              # print(ji, window)
              #Read rasters into nmpy arrays
              zoneRas = zSrc.read(1,window = window)
              valueRas = src.read(1, window=window)
              #Compare array shapes - should be same
              if zoneRas.shape == valueRas.shape:
                  #get unique class values in value raster - ignoring nodata
                  noData = src.nodatavals
                  valueRas[valueRas == noData] = 0
                  classes = list(np.unique(valueRas)) #unique classes
                  if noData in classes: #ignore NoData
                      classes.remove(noData)
                  classes.sort() #sort classes
                  # print("Classes: ", classes)
                  #2. Loop through each zone and get cell counts
                  zones = list(np.unique(zoneRas))
                  ct = 0
                  for z in zones:
                      ct += 1
                      # if ct % 100 == 0:
                          # print(ct)
                          # print(f'{int(ct/len(zones)*100)}%')
                      vals = bbox(zoneRas, valueRas, z, classes)
                      #3. Store cell counts in dictionary
                      tabArea[z] = vals
              else:
                  #what error do you want to throw?
                  print("Shapes are different")
                  # sys.exit()

    #4. Convert dictionary to Dataframe
    finTable = DataPrepHelper.dict2Table(self, tabArea, classes, idField)
    
    return finTable

  def tabulateAll(self, tab_dict,zone_dict):
    self.tab_dict = tab_dict
    self.zone_dict = zone_dict
    tab_list = list(tab_dict.keys())
    for tab in tab_list:
        idx = 0
        print(tab_dict[tab])
        functions = list(tab_dict[tab][idx].keys())
        for f in functions:
            if f == 'SID':
                DataPrepHelper.tabulateArea(self,tab,zone_dict['SID'],tab_dict[tab][idx]['SID'])
                print('future object created')
            if f == 'PID':
                DataPrepHelper.tabulateArea(self,tab,zone_dict['PID'],tab_dict[tab][idx]['PID'])
                print('future object created')
        idx += 1
    return True

  def to_gpkg(self, geo_df, outpath, layer_name):
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
    print('creating a geopackage')
#     #Take the params defined above and make the geopackage, this will return the path to the created geopackage
    st = time.time() #Start timer for creating geopackage
    # x = 'P:/cumb_42041/input/segments.gpkg'
    x = helper.gdbTogpkg(p)
    time_helper.etime(county,'created geopackage', st)

# # # #Explode the polygons
#     print('exploding polygons from geopackage')
    print('reading polygons from geopackage')
#     st=time.time() #start the timer for explode polygons
#     lc_segs, after_len = helper.explodeMultipolygon(x)
    lc_segs = gpd.read_file(x)  
    lc_segs['SID']= [int(x) for x in range(1, len(lc_segs)+1)]
#     time_for_explode = time_helper.etime(county,'exploded segs and assigned unique id', st)

# #     # # do same thing with parcel dataset
# #     # read in parcels
    parcels = '{0}/{1}/input/parcels.shp'.format(project_folder, county)
    st=time.time() #start timer for doing land parcels
    print('reading parcels polygon layer')
    parcels = gpd.read_file(parcels) 
    # parcels, after_len = helper.explodeMultipolygonShape(parcels_path1, ['OBJECTID', 'geometry'])
    # target_fields = ['OBJECTID','geometry']
    # parcels = parcels[target_fields]
    parcels['PID']= [int(x) for x in range(1, len(parcels)+1)]
    time_helper.etime(county,'exploded parcels and assigned unique id', st)


# # # # # # # # #     # # # *********************************************************************************
    snap_ras = '{0}/{1}/input/{1}_landcover_2017_June2021.tif'.format(project_folder,county) 
    out_ras = '{0}/{1}/input/{1}_landcover_2017_TEMP_PARCELS.tif'.format(project_folder,county)



    # st=time.time() #start timer for creating a temp raster

    # Create a temp raster
    print('creating a temp raster with parcels')
    ras_arr = helper.prepRaster(snap_ras, parcels, 'PID', out_ras)
    time_helper.etime(county,'parcels rasterized', st)
    
    # Create a vector geopackage from the temp raster
    print('creating a vector from the temp raster')
    st = time.time()
    parcels = helper.vectorizeRaster(ras_arr, out_ras)
    time_helper.etime(county,'parcels raster is vectorized', st)
    parcels.to_file('{0}/{1}/input/temp_dataprep_unexp.gpkg'.format(project_folder,county), layer = 'vectorized_parcels',driver = 'GPKG')


    print('sending parcels and segments to database tables')
    st = time.time()
    segs_woked = helper.to_database(lc_segs,'lc_segs')
    print('Segments sent to DB table:',segs_woked)
    parcs_worked = helper.to_database(parcels,'parcels')
    print('Parcels sent to DB table:',parcs_worked)

    time_helper.etime(county,'parcels and lc_segs to database tables', st)
    print('starting the union in SQL') 
    st = time.time()
    helper.db_overlay('psegs_unexp')
    time_helper.etime(county,'created psegs',st)
    so_gdf = helper.fix_SID('psegs_unexp')
    time_helper.etime(county,'validated geometries',st)


    so_gdf['PSID']= [int(x) for x in range(1, len(so_gdf)+1)]
    # time_helper.etime(county, 'assigned PSIDs to all psegs geometries', st)
    helper.to_gpkg(so_gdf,"{0}/{1}/input/psegs_unexp.gpkg".format(project_folder,county),'psegs')



    out_ras_PID = '{0}/{1}/input/ps_parcels_unexp.tif'.format(project_folder,county)
    out_ras_SID = '{0}/{1}/input/ps_segs_unexp.tif'.format(project_folder,county)


    print('making a raster based on parcels')
    st = time.time()
    helper.prepRaster(snap_ras, so_gdf, 'pid',out_ras_PID)
    time_helper.etime(county, 'Raster created based on PID', st)

    st = time.time()
    print('making a raster based on segments')
    helper.prepRaster(snap_ras, so_gdf, 'sid',out_ras_SID)
    time_helper.etime(county, 'Raster created based on SID', st)

    print('unexploded test complete. goodbye :)')
    sys.exit()


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
    # futures = client.map(helper.tabulateArea,*results)
    # wait(futures)
    # client.close()
    # time_helper.etime(county, 'all tabulate areas have been calculated', st)
    helper.tabulateAll(tab_dict,zone_dict)
    st = time.time()
    print('psegs and parcels to geopackage')

    # client = Client(processes = False)
    # dfs = []
    # dfs.append(("{0}/{1}/input/psegs.gpkg".format(project_folder,county),'psegs'))
    # dfs.append(("{0}/{1}/input/temp_dataprep.gpkg".format(project_folder,county),'vectorized_parcels'))
    # futures = client.map(helper.to_gpkg,dfs)
    # wait(futures)
    # helper.to_gpkg(so_gdf,"{0}/{1}/input/psegs.gpkg".format(project_folder,county),'psegs')
    # helper.to_gpkg(parcels,"{0}/{1}/input/temp_dataprep.gpkg".format(project_folder,county),'vectorized_parcels')


    # print('dropping temporary tables, all other data has been prepared')
    # conDB = psycopg2.connect(host='localhost',
    # database=postgres_db,
    # user=postgres_user,
    # password = postgres_pass)
    # cursor = conDB.cursor()
    # sql_statement = '''DROP TABLE IF EXISTS "psegs"'''
    # cursor.execute(sql_statement)
    # sql_statement = '''DROP TABLE IF EXISTS "parcels"'''
    # cursor.execute(sql_statement)
    # sql_statement = '''DROP TABLE IF EXISTS "lc_segs"'''
    # cursor.execute(sql_statement)
    # conDB.commit()
    # cursor.close()
    # conDB.close()
    print('this county',county, 'has been prepared, machine is ready for next county. goodbye :)')
    # sys.exit()