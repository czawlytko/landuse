"""
Script: QGIS_geoprocessing.py
Purpose: Run QGIS spatial overlay and dissolve functions in a shell command and produce data as a geopandas geodataframe.
Author: Labeeb Ahmed, Geographer, U.S. Geological Survey
Contact: lahmed@chesapeakebay.net
"""

from timeit import default_timer as timer
from pathlib import Path
import geopandas as gpd
from qgis.core import *
from qgis.PyQt.QtCore import QVariant
from shapely.geometry import shape, MultiPolygon, Polygon
import multiprocessing as mp
import itertools
import sys
import multiprocessing as mp #testing lock

def read_gpkg(gpkg_path):
    """
    Method: read_gpkg()
    Purpose: Read in qgis output from geopackage as geopandas geodataframe and return it. Returns None if file 
             is empty.
    Params: gpkg_path: path to ouput geopackage; layer name is same as geopackage name
    Returns: gdf - geodataframe of output layer or None if file is empty
    """
    gpkg = Path(gpkg_path)

    if gpkg.is_file():
        gdf = gpd.read_file(gpkg_path, layer=gpkg.stem, driver='GPKG')
        # if not gdf.is_empty[0]:
        return gdf
    else:
        print(f'{gpkg} does not exist. Read attempt failed.')
        sys.exit(1)

def spatial_overlay(input_layer, overlay_layer, op, output_layer, writeResultsFlag):
    """
    Method: spatial_overlay()
    Purpose: Run QGIS spatial overlay functions (difference, intersection or union) on specified data and
             write results to new geopackage (must be new for this version of qgis). Calls read_gpkg to 
             create and return results as geopandas geodataframe.
    Params: input_layer: path to layer of data to run overlay on, in the form r"Path\test_qgis.gpkg|layername=name_of_layer"
            overlay_layer: path to overlay layer, in the form r"Path\test_qgis.gpkg|layername=name_of_layer"
            op: string of overlay operation, options are difference, intersection or union
            output_layer: path to NEW geopackage to write overlay results to - will automatically create layer of same name
            writeResultsFlag: boolean flag if results need to be returned as gdf
    Returns: geodataframe of output layer or None if file is empty
    """
    # initialize Qgs application
    qgs = QgsApplication([], False)
    qgs.initQgis()

    # initialize processing to access native QGIS algorithms
    import processing
    from processing.core.Processing import Processing
    from qgis.analysis import QgsNativeAlgorithms
    Processing.initialize()
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

    # read in layers
    input_layer = QgsVectorLayer(input_layer, "input layer", "ogr")
    overlay_layer = QgsVectorLayer(overlay_layer, "overlay layer", "ogr")

    # tool parameters
    params = {
        'INPUT' : input_layer, 
        'OUTPUT' : 'TEMPORARY_OUTPUT', 
        'OVERLAY' : overlay_layer
    }

    # operations dict
    operations = {
        'difference': "qgis:difference",
        'intersection': "qgis:intersection",
        'union': "qgis:union",
    }

    st = timer()
    # print(f'Running operation: {op}')
    result = processing.run(operations[op], params)
    # print('Operation Complete. Processing time (s) :', round(timer()-st, 2))

    st = timer()
    writer = QgsVectorFileWriter.writeAsVectorFormat(
        result['OUTPUT'],
        output_layer, 
        "UTF-8", 
        input_layer.crs()
    )
    # print('GPKG save time (s) :', round(timer()-st, 2))

    #close qgis app
    qgs.exitQgis()

    # read and return gdf
    if writeResultsFlag:
        return read_gpkg(output_layer)
    else:
        return None


def dissolve(input_layer, output_layer, writeResultsFlag, fields=None):
    """
    Method: dissolve()
    Purpose: Run QGIS dissolve function on input data. User can pass a list of dissolve field name, otherwise fields is None. 
             Writes results to new geopackage (must be new for this version of qgis). Calls read_gpkg to 
             create and return results as geopandas geodataframe.
    Params: input_layer: path to layer of data to run overlay on, in the form r"Path\test_qgis.gpkg|layername=name_of_layer"
            output_layer: path to NEW geopackage to write overlay results to - will automatically create layer of same name
            writeResultsFlag: boolean flag if results need to be returned as gdf
            fields: OPTIONAL - list of string field names to dissolve on.
    Returns: geodataframe of output layer or None if file is empty
    """
    # initialize Qgs application
    qgs = QgsApplication([], False)
    qgs.initQgis()

    # initialize processing to access native QGIS algorithms
    import processing
    from processing.core.Processing import Processing
    from qgis.analysis import QgsNativeAlgorithms
    Processing.initialize()
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

    # read in layers
    input_layer = QgsVectorLayer(input_layer, "input layer", "ogr")

    # tool parameters
    params = {
        'INPUT' : input_layer, 
        'OUTPUT' : 'TEMPORARY_OUTPUT', 
    }

    if fields:
        if isinstance(fields, list):
            params.update({'FIELD': fields})

    st = timer()
    # print(f'Running operation: dissolve')
    result = processing.run("native:dissolve", params)
    # print('Operation Complete. Processing time (s) :', round(timer()-st, 2))

    st = timer()
    
    writer = QgsVectorFileWriter.writeAsVectorFormat(
        result['OUTPUT'],
        output_layer, 
        "UTF-8", 
        input_layer.crs()
    )
    # print('GPKG save time (s) :', round(timer()-st, 2))

    #close qgis app
    # qgs.exitQgis()

    # read and return gdf
    if writeResultsFlag:
        return read_gpkg(output_layer)
    else:
        return None
