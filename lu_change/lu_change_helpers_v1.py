"""
Script: lu_change_helpers_v1.py
Purpose: Helper functions to be called in lu_change_vector script.
Author: Sarah McDonald, Geographer, U.S. Geological Survey
Contact: smcdonald@chesapeakebay.net
"""

from lu_change import readXML
import rasterio as rio 
import rasterio.mask
from rasterio.features import rasterize, shapes
import numpy as np 
import geopandas as gpd
import multiprocessing as mp
from shapely.geometry import Polygon, Point
import pandas as pd
import sys
from rasterio.windows import from_bounds

import luconfig

##########################################################################################
#---------------------- GETTERS ---------------------------------------------------------#
##########################################################################################

def get_lu_code(description, getKey):
    """
    Method: get_lu_code()
    Purpose: Test if passed value is in LU dictionary and return the description or 
             LU code based on getKey.
    Params: description - string LU name or int LU code; pass 'ALL' if you want 
                            full dict returned
            getKey - boolean; True if passing LU code and want LU description;
                              False if passing LU description and want LU code
    Returns: description/code or -1 if key/description is not in dict
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

def getWetlandTypes(val):
    """
    Method: getWetlandTypes()
    Purpose: Test if passed value is in wetlands dict and return all wetlands
                LU classes within the nested wetland Class.
    Params: val - string wetland type; pass 'ALL' if you want 
                            full dict returned
    Returns: list of lu codes or -1 if val does not exist
    """
    wetland_types_dict = {
        'Tidal' : [
                        get_lu_code('Tidal Wetlands Barren', False),
                        get_lu_code('Tidal Wetlands Forest', False),
                        get_lu_code('Tidal Wetlands Herbaceous', False),
                        get_lu_code('Tidal Wetlands Scrub/Shrub', False),
                        get_lu_code('Tidal Wetlands Tree Canopy', False)
                    ],
        'Floodplain' : [
                        get_lu_code('Floodplain Barren', False),
                        get_lu_code('Floodplain Forest', False),
                        get_lu_code('Floodplain Herbaceous', False),
                        get_lu_code('Floodplain Scrub/Shrub', False),
                        get_lu_code('Floodplain Tree Canopy', False),
                        get_lu_code('Riverine (Non-Tidal) Wetlands Barren', False),
                        get_lu_code('Riverine (Non-Tidal) Wetlands Forest', False),
                        get_lu_code('Riverine (Non-Tidal) Wetlands Herbaceous', False),
                        get_lu_code('Riverine (Non-Tidal) Wetlands Scrub/Shrub', False),
                        get_lu_code('Riverine (Non-Tidal) Wetlands Tree Canopy', False),
                        get_lu_code('Headwater Barren', False),
                        get_lu_code('Headwater Forest', False),
                        get_lu_code('Headwater Herbaceous', False),
                        get_lu_code('Headwater Scrub/Shrub', False),
                        get_lu_code('Headwater Tree Canopy', False)
                        ],
        'Other' : [
                    get_lu_code('Terrene/Isolated Wetlands Barren', False),
                    get_lu_code('Terrene/Isolated Wetlands Forest', False),
                    get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
                    get_lu_code('Terrene/Isolated Wetlands Scrub/Shrub', False),
                    get_lu_code('Terrene/Isolated Wetlands Tree Canopy', False)
                    ]
    }
    if val == 'ALL':
        return wetland_types_dict
    elif val in wetland_types_dict:
        return wetland_types_dict[val]
    else:
        print("\nERROR: ", val, " not in wetland_types_dict; returning -1\n")
        return -1

def get_lu_errors(val):
    """
    Method: get_lu_errors()
    Purpose: Test if passed value is in bad lus dict and return the LU class it should be in.
             This should not need to be done in final LU since the LC classes in the burn in
             should not exist.
    Params: val - int LC ras value; pass 'ALL' if you want 
                            full dict returned
    Returns: int LU code or -1
    """
    bad_lu = { 1 : 1000, #'Water LC': 
                2 : 5000, #'Emergent Wetlands'
                3 : 3100, #'Tree Canopy'
                4: 3430, #Scrub Shrub
                5 : 3420,     #'Herbaceous'
                6 : 2220, #'Bare Developed (LC VALUE)'
                7 : 2120, #'Structures LC'
                8 : 2130, #'Other Impervious LC'
                9 :  2110 #'Impervious Roads LC'
    }
    if val == 'ALL':
        return bad_lu
    elif val in bad_lu:
        return bad_lu[val]
    else:
        return -1

def getCDL(val):
    """
    Method: getCDL()
    Purpose: Test if passed value is in cdl dict and return the class name.
    Params: val - int CDL ras value; pass 'ALL' if you want 
                            full dict returned
    Returns: string cdl class name or -1
    """
    CDL_dict = {    
                # '0'   :   'NoData',
                '1'	:	'Crops',
                '2'	:	'Fallow',
                '3'	:	'Pasture',
                '4'	:	'Orchards',
                '92'	:	'Aquaculture',
                '111'	:	'OpenWater',
                '121'	:	'DevOS',
                '122'	:	'DevLI',
                '123'	:	'DevMI',
                '124'	:	'DevHI',
                '131'	:	'Barren-NLCD',
                '141'	:	'DeciduousForest',
                '142'	:	'EvergreenForest',
                '143'	:	'MixedForest',
                '152'	:	'Shrubland-NLCD',
                '176'	:	'GrasslandHerbaceous',
                '190'	:	'WoodyWetlands',
                }
    if str(val) == 'ALL':
        return CDL_dict
    elif val in CDL_dict:
        return CDL_dict[val]
    else:
        print("ERROR: ", val, " is not not CDL dict; Returning -1")
        return -1

def getNLCD(val):
    """
    Method: getNLCD()
    Purpose: Test if passed value is in nlcd dict and return the class name.
    Params: val - int nlcd ras value; pass 'ALL' if you want 
                            full dict returned
    Returns: string nlcd class name or -1
    """
    nlcd_dict = {
                # 0:'NADA',
                  11:'Open Water',
                  12:'Perennial Ice/Snow',
                  21:'Developed, Open Space',
                  22:'Developed, Low Intensity',
                  23:'Developed, Medium Intensity',
                  24:'Developed, High Intensity',
                  31:'Barren Land (Rock/Sand/Clay)',
                  41:'Deciduous Forest',
                  42:'Evergreen Forest',
                  43:'Mixed Forest',
                  52:'Shrub/Scrub',
                  71:'Grassland/Herbaceous',
                  81:'Pasture/Hay',
                  82:'Cultivated Crops',
                  90:'Woody Wetlands',
                  95:'Emergent Herbaceous Wetlands'
                  }
    if str(val) == 'ALL':
        return nlcd_dict
    elif val in nlcd_dict:
        return nlcd_dict[val]
    else:
        print("ERROR: ", val, " is not not NLCD dict; Returning -1")
        return -1

def getLCMAP(val):
    """
    Method: getLCMAP()
    Purpose: Test if passed value is in lcmap dict and return the class name.
    Params: val - int lcmap ras value; pass 'ALL' if you want 
                            full dict returned
    Returns: string lcmap class name or -1
    """
    lcmap_dict = {
                # 0:'NADA',
                  1:'Afforestation',
                  2:'Deforestation',
                  3:'Timber Harvest – Tree Covered',
                  4:'Timber Harvest – Low Veg/barren',
                  5:'Developed; no pre development class',
                  6:'Crop Rotation',
                  7:'Wetland',
                  8:'Pre-Development/construction',
                  9:'Water',
                  51:'Developed; Previously Low Veg',
                  52:'Developed; Previously Tree Cover',
                  53:'Developed; Previously Wetland',
                  100:'Pattern Detected and Not Reclassed'
                  }
    if str(val) == 'ALL':
        return lcmap_dict
    elif val in lcmap_dict:
        return lcmap_dict[val]
    else:
        print("ERROR: ", val, " is not not LCMAP dict; Returning -1")
        return -1

def getAgAndTurf(join_table):
    """
    Method: getAgAndTurf()
    Purpose: In this general approach, any parcel who NLCD 2011 or 2016 has a majority value of 
             Cultivated Crops or Pasture/Hay or who LCMAP Patterns majority data says was Developed; Previously Low Veg AND
             CDL contained an ag class. These parcels will be crop or pasture depending on the T2 LU majority, if it is not
             an ag class it will be other. 
    Params: join_table - geodataframe of parcels with summarized NLCD11, NLCD16, LCMAP Patterns, CDL data and 2017 LU
    Returns: join_table - table with PID column and TYPE column which says if parcel should be turf, crop or pasture
    """
    # Find parcels whose 2013/2014 LU is ag
    nlcdAgClasses = [81, 82]
    lu_crops = [4111, 4112, 4131, 4132, 4133, 4101, 4102, 4103]
    lu_pas = [4121, 4122, 4141, 4142]
    lcmap_ag = 51 #getLCMAP('Developed; Previously Low Veg')

    # check T2 LU first
    join_table.loc[(join_table['ParT2LUmaj'].isin(lu_crops)), 'TYPE'] = 'crop'
    join_table.loc[(join_table['ParT2LUmaj'].isin(lu_pas)), 'TYPE'] = 'pasture'
    join_table.loc[(join_table['ParT2LUmaj'].isin(lu_crops)), 'Type_log'] = 'T2 LU majority'
    join_table.loc[(join_table['ParT2LUmaj'].isin(lu_pas)), 'Type_log'] = 'T2 LU majority'

    # NLCD11 and CDL13 say pasture
    join_table.loc[(join_table['NLCD11_pas'] > 0)&(join_table['Pasture']>0), 'TYPE'] = 'pasture'
    join_table.loc[(join_table['NLCD11_pas'] > 0)&(join_table['Pasture']>0), 'Type_log'] = 'NLCD11 and CDL pas > 0'

    # NLCD11 says > 50% pasture
    join_table.loc[:, 'pct_pas'] = join_table['NLCD11_pas'] / join_table['pid_area']
    pas_pids = list(join_table[join_table['pct_pas'] > 0.5]['PID'])
    join_table.loc[(join_table['PID'].isin(pas_pids))&(join_table['TYPE'].isna()), 'TYPE'] = 'pasture'
    join_table.loc[(join_table['PID'].isin(pas_pids))&(join_table['Type_log'].isna()), 'Type_log'] = 'NLCD11 > 50%' 

    # possible ag based on lcmap and nlcd - use CDL vals
    cdl_ag = list(join_table[(join_table['NLCD11_crop'] > 0) |  (join_table['NLCD11_pas'] > 0) | (join_table['LCMAPmaj'] == lcmap_ag)]['PID'])
    join_table.loc[:, 'pct_crop'] = (join_table['Crops'] + join_table['Orchards']) / join_table['pid_area']
    join_table.loc[:, 'pct_pascdl'] = join_table['Pasture']/ join_table['pid_area']
    join_table.loc[(join_table['PID'].isin(cdl_ag))&(join_table['TYPE'].isna())&(join_table['pct_crop'] > 0.5), 'TYPE'] = 'crop'
    join_table.loc[(join_table['PID'].isin(cdl_ag))&(join_table['Type_log'].isna())&(join_table['pct_crop'] > 0.5), 'Type_log'] = 'lcmap/nlcd detect ag, cdl is maj crop/orchard'
    join_table.loc[(join_table['PID'].isin(cdl_ag))&(join_table['TYPE'].isna())&(join_table['pct_pascdl'] > 0.5), 'TYPE'] = 'pasture'
    join_table.loc[(join_table['PID'].isin(cdl_ag))&(join_table['Type_log'].isna())&(join_table['pct_pascdl'] > 0.5), 'Type_log'] = 'lcmap/nlcd detect ag, cdl is maj pasture'

    # Give remaining parcels TYPE other
    join_table.loc[join_table['TYPE'].isna(), 'TYPE'] = 'other'
    join_table.loc[join_table['Type_log'].isna(), 'Type_log'] = 'not captured as ag in ancillary'
    join_table = join_table[['PID', 'TYPE', 'Type_log', 'pid_area']]

    return join_table

def getCntyBoundary(anci_folder, cf):
    """
    Method: getCntyBoundary()
    Purpose: Get county boundary geometry from 20m buffered counties shapefile
    Params: drive - path to drive with ancillary folder structure
            cf - county fips
    Returns: cnty_geom - list of county geometry
    """
    cspath = f'{anci_folder}/census/BayCounties20m_project.shp'
    cs = gpd.read_file(cspath, driver='shapefile')
    #Get CF Number
    cfnum = cf.split('_')[1]
    #Get only the row with CF Number and store in list
    cnty_geom = list(cs[cs['GEOID'] == cfnum]['geometry'])
    
    return cnty_geom

def getContextChange(val):
    """
    Method: getContextChange()
    Purpose: Test if passed value is in context dict and return the dict of rules.
    Params: val - string lc change class name; pass 'ALL' if you want 
                            full dict returned
    Returns: dict of rules for lc change class
    """
    context_dict = {
        'Barren to Low Vegetation'      : {
            'Turf Herbaceous'   : get_lu_code('Turf Herbaceous', False), # WAS BARE DEVELOPED
            'AG - crop'    : get_lu_code('Cropland Barren', False),
            'AG - pas'     : get_lu_code('Pasture/Hay Barren', False),
            'Wetland - Tidal' : get_lu_code('Tidal Wetlands Barren', False),
            'Wetland - Floodplain' : get_lu_code('Floodplain Barren', False),
            'Wetland - Other' : get_lu_code('Terrene/Isolated Wetlands Barren', False),
            'else'         : get_lu_code('Natural Succession Barren', False)
        },
        'Barren to Structures'          : {
            'AG - crop'    : get_lu_code('Cropland Barren', False),
            'AG - pas'     : get_lu_code('Pasture/Hay Barren', False),
            'else'         : get_lu_code('Bare Developed', False)
        },
        'Barren to Tree Canopy'         : { 
            'Tree Canopy over Turf'        : get_lu_code('Suspended Succession Barren', False),
            'Forest'                    : get_lu_code('Suspended Succession Barren', False),
            'Forest Forest'             : get_lu_code('Suspended Succession Barren', False),
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Barren', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Barren', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Barren', False),
            'AG - crop'                 : get_lu_code('Cropland Barren', False),
            'AG - pas'                  : get_lu_code('Pasture/Hay Barren', False),
            'else'                      : get_lu_code('Natural Succession Barren', False)
        },
        'Emergent Wetlands to Barren' : { #5/7 update
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Herbaceous', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Herbaceous', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
            'else'         :       get_lu_code('Natural Succession Herbaceous', False)
        },
        'Emergent Wetlands to Low Vegetation' : {#5/7 update
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Herbaceous', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Herbaceous', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
            'else'         :       get_lu_code('Natural Succession Herbaceous', False)
        },
        'Emergent Wetlands to Tree Canopy' : {#5/7 update
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Herbaceous', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Herbaceous', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
            'else'         :       get_lu_code('Natural Succession Herbaceous', False)
        },
        'Emergent Wetlands to Water' : {#5/7 update
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Herbaceous', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Herbaceous', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
            'else'         :       get_lu_code('Natural Succession Herbaceous', False)
        },
        'Emergent Wetlands to Structures' : {
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Herbaceous', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Herbaceous', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
            'AG - crop'    :       get_lu_code('Cropland Herbaceous', False),      
            'AG - pas'     :       get_lu_code('Pasture/Hay Herbaceous', False),
            'else'         :       get_lu_code('Suspended Succession Herbaceous', False)
        },
        'Low Vegetation to Scrub\Shrub' : {
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Herbaceous', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Herbaceous', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
            'AG - crop'    :       get_lu_code('Cropland Herbaceous', False),      
            'AG - pas'     :       get_lu_code('Pasture/Hay Herbaceous', False),
            'else'         :       get_lu_code('Natural Succession Herbaceous', False)
        },
        'Low Vegetation to Tree Canopy' : {
            'Tree Canopy over Turf' : get_lu_code('Turf Herbaceous', False),
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Herbaceous', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Herbaceous', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Herbaceous', False),
            'AG - crop'    :       get_lu_code('Cropland Herbaceous', False),      
            'AG - pas'     :       get_lu_code('Pasture/Hay Herbaceous', False),
            'else'         :       get_lu_code('Natural Succession Herbaceous', False)
        },
        'Scrub\Shrub to Barren'         : {
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Scrub/Shrub', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Scrub/Shrub', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Scrub/Shrub', False),
            'AG - crop'    :         get_lu_code('Orchard/Vineyard Scrub/Shrub', False),      
            'AG - pas'     :         get_lu_code('Pasture/Hay Scrub/Shrub', False),      
            'else'         :         get_lu_code('Natural Succession Scrub/Shrub', False)   
        },
        'Scrub\Shrub to Low Vegetation' : {
            'Wetland - Tidal'           : get_lu_code('Tidal Wetlands Scrub/Shrub', False),
            'Wetland - Floodplain'      : get_lu_code('Floodplain Scrub/Shrub', False),
            'Wetland - Other'           : get_lu_code('Terrene/Isolated Wetlands Scrub/Shrub', False),
            'AG - crop'    :                        get_lu_code('Orchard/Vineyard Scrub/Shrub', False),      
            'AG - pas'     :                        get_lu_code('Pasture/Hay Scrub/Shrub', False),      
            'Suspended Succession Herbaceous'     :  get_lu_code('Suspended Succession Scrub/Shrub', False),
            'else'         :                        get_lu_code('Natural Succession Scrub/Shrub', False) 
        },
        'Scrub\Shrub to Other Impervious Surfaces'  : {
            'AG - crop'    :            get_lu_code('Orchard/Vineyard Scrub/Shrub', False),      
            'AG - pas'     :            get_lu_code('Pasture/Hay Scrub/Shrub', False),      
            'else'         :            get_lu_code('Natural Succession Scrub/Shrub', False) 
        },
        'Scrub\Shrub to Roads'          : {
            'AG - crop'    :            get_lu_code('Orchard/Vineyard Scrub/Shrub', False),      
            'AG - pas'     :            get_lu_code('Pasture/Hay Scrub/Shrub', False),      
            'else'         :            get_lu_code('Natural Succession Scrub/Shrub', False)        
        },
        'Scrub\Shrub to Structures'             : {
            'AG - crop'    :            get_lu_code('Orchard/Vineyard Scrub/Shrub', False),      
            'AG - pas'     :            get_lu_code('Pasture/Hay Scrub/Shrub', False),      
            'else'         :            get_lu_code('Natural Succession Scrub/Shrub', False) 
        }  
    }
    if val == 'ALL':
        return context_dict
    elif val in context_dict:
        return context_dict[val]
    else:
        print('\tERROR: ', val, ' not in context_dict - returning {}')
        return {}

def getDirectChange(val):
    """
    Method: getDirectChange()
    Purpose: Test if passed value is in direct dict and return the lu code.
    Params: val - string lc change class name; pass 'ALL' if you want 
                            full dict returned
    Returns: int lu code
    """
    direct_dict = {
            'Other Impervious Surfaces to Tree Canopy' : get_lu_code('Other Impervious Surface', False),
            'Other Impervious Surfaces to Structures' : get_lu_code('Other Impervious Surface', False),
            'Other Impervious Surfaces to Roads' : get_lu_code('Other Impervious Surface', False),
            'Other Impervious Surfaces to Barren'   : get_lu_code('Other Impervious Surface', False),
            'Other Impervious Surfaces to Low Vegetation'   : get_lu_code('Other Impervious Surface', False),
            'Other Impervious Surfaces to Tree Canopy Over Other Impervious Surfaces'   : get_lu_code('Other Impervious Surface', False),
            'Other Impervious Surfaces to Water'   : get_lu_code('Other Impervious Surface', False),
            'Roads to Barren'   : get_lu_code('Roads', False),
            'Roads to Low Vegetation'   : get_lu_code('Roads', False),
            'Roads to Tree Canopy Over Roads'   : get_lu_code('Roads', False),
            'Roads to Water'    : get_lu_code('Roads', False),
            'Roads to Tree Canopy' : get_lu_code('Roads', False),
            'Roads to Structures' : get_lu_code('Roads', False),
            'Roads to Other Impervious Structures': get_lu_code('Roads', False), # REPLACE WITH Surfaces
            'Roads to Other Impervious Surfaces' : get_lu_code('Roads', False),
            'Scrub\Shrub to Tree Canopy'        : get_lu_code('Natural Succession Scrub/Shrub', False),
            'Structures to Barren'  : get_lu_code('Buildings', False),
            'Structures to Low Vegetation'  : get_lu_code('Buildings', False),
            'Structures to Tree Canopy Over Structures' : get_lu_code('Buildings', False),
            'Structures to Water'   : get_lu_code('Buildings', False),
            'Structures to Tree Canopy' : get_lu_code('Buildings', False),
            'Structures to Other Impervious Surfaces' : get_lu_code('Buildings', False),
            'Structures to Roads' : get_lu_code('Buildings', False),
            'Tree Canopy Over Other Impervious Surfaces to Structures' : get_lu_code('Tree Canopy over Other Impervious', False),
            'Tree Canopy Over Other Impervious Surfaces to Other Impervious Surfaces' : get_lu_code('Tree Canopy over Other Impervious', False),
            'Tree Canopy Over Other Impervious Surfaces to Roads' : get_lu_code('Tree Canopy over Other Impervious', False),
            'Tree Canopy Over Other Impervious Surfaces to Barren'  : get_lu_code('Tree Canopy over Other Impervious', False),
            'Tree Canopy Over Other Impervious Surfaces to Low Vegetation'  : get_lu_code('Tree Canopy over Other Impervious', False),
            'Tree Canopy Over Other Impervious Surfaces to Water'   : get_lu_code('Tree Canopy over Other Impervious', False),
            'Tree Canopy Over Roads to Structures' : get_lu_code('Tree Canopy over Roads', False),
            'Tree Canopy Over Roads to Other Impervious Surfaces' : get_lu_code('Tree Canopy over Roads', False),
            'Tree Canopy Over Roads to Roads' : get_lu_code('Tree Canopy over Roads', False),
            'Tree Canopy Over Roads to Barren'  : get_lu_code('Tree Canopy over Roads', False),
            'Tree Canopy Over Roads to Low Vegetation'  : get_lu_code('Tree Canopy over Roads', False),
            'Tree Canopy Over Roads to Water'   : get_lu_code('Tree Canopy over Roads', False),
            'Tree Canopy Over Structures to Barren' : get_lu_code('Tree Canopy over Structures', False),
            'Tree Canopy Over Structures to Water'  : get_lu_code('Tree Canopy over Structures', False),
            'Tree Canopy Over Structures to Tree Canopy' : get_lu_code('Tree Canopy over Structures', False),
            'Tree Canopy Over Structures to Structures' : get_lu_code('Tree Canopy over Structures', False),
            'Tree Canopy Over Structures to Other Impervious Surfaces' : get_lu_code('Tree Canopy over Structures', False),
            'Tree Canopy Over Structures to Roads' : get_lu_code('Tree Canopy over Structures', False),      
            'Tree Canopy Over Structures to Low Vegetation'  : get_lu_code('Tree Canopy over Structures', False),    
            'Water to Barren'   : get_lu_code('Water', False),
            'Water to Buildings'    : get_lu_code('Water', False),
            'Water to Low Vegetation'   : get_lu_code('Water', False),
            'Water to Other Impervious Surfaces'    : get_lu_code('Water', False),
            'Water to Scrub\Shrub'  : get_lu_code('Water', False),
            'Water to Structures'   : get_lu_code('Water', False),
            'Water to Tree Canopy'  : get_lu_code('Water', False)
        }
    if val == 'ALL':
        return direct_dict
    elif val in direct_dict:
        return direct_dict[val]
    else:
        print('\tERROR: ', val, ' not in direct_dict - returning {}')
        return {}

def getIndirectChange():
    """
    Method: getIndirectChange()
    Purpose: Store and return list of LC change classes that need to be captured
             in the indirect approach.
    Params: N/A
    Returns: list of LC change classes to run through indirect methods
    """
    indirect_change = [
        'Barren to Other Impervious Surfaces',
        'Barren to Roads',
        'Barren to Tree Canopy',
        'Barren to Water',
        'Emergent Wetlands to Other Impervious Surfaces',
        'Emergent Wetlands to Roads',
        'Low Vegetation to Barren',
        'Low Vegetation to Other Impervious Surfaces',
        'Low Vegetation to Roads',
        'Low Vegetation to Water',
        'Scrub\Shrub to Water',
        'Tree Canopy to Barren',
        'Tree Canopy to Low Vegetation',
        'Tree Canopy to Other Impervious Surfaces',
        'Tree Canopy to Roads',
        'Tree Canopy to Structures',
        'Tree Canopy to Water'
    ]
    return indirect_change

###############################################################################
#------------------- RASTER FUNCTIONS-----------------------------------------#
###############################################################################
def clipRasByCounty(cnty_geom, changeonlypath):
    """
    Method: clipRasByCounty()
    Purpose: Clip raster by county boundary.
    Params: cnty_geom - list of county geometry
            changeonlypath - path to tif of binary change raster
    Returns: out_image - numpy array of change clipped to county boundary
    """
    #Mask change only raster using county set shapefile
    with rio.open(changeonlypath) as src_co:
        out_image, out_transform = rio.mask.mask(src_co, cnty_geom, crop=True)
        out_meta = src_co.meta
        out_meta.update({"driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform}) 
    return out_image, out_meta

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

###############################################################################
#----------------------- MP FUNCTIONS-----------------------------------------#
###############################################################################
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
    sjoinSeg = gpd.sjoin(df1, df2, how='inner', op=sjoin_op)
    sjoinSeg = sjoinSeg[cols]
    sjoinSeg.drop_duplicates(inplace=True)
    return sjoinSeg

def zonal_stats_mp(shapes, stat, rasPath, rasVals, cols, isT2LU, isLCChange): #isT2LU is temp for reclassing the error classes from burn in
    """
    Method: zonal_stats_mp()
    Purpose: Calculate the zonal stats / tabulate area for the specified raster and polygons.
    Params: shapes - dict of polygons
            stat - 'MAJ' for zonal majority
            stat - '' for tabulate area
            rasPath - path to raster
            rasVals - unique raster values
    Return: result - dict of keys and zonal stats
    """
    cpus_minus_1 = 10 #int(mp.cpu_count() / 2)
    if cpus_minus_1 == 0:
        cpus_minus_1 = 1

    pool = mp.Pool(processes=cpus_minus_1)

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
        gdf_args = keys, geoms, rasPath, rasVals, stat, isT2LU, isLCChange #delete isT2LU
        chunk_iterator.append(gdf_args)

    results = pool.map(run_zonal_stats, chunk_iterator) #list of dictionaries
    pool.close()

    result = {} #create one dict from list of dicts
    for d in results:
        result.update(d)
    del results

    df = pd.DataFrame.from_dict(result, orient='index', columns=cols[1:len(cols)])
    df[cols[0]] = df.index

    return df

def run_zonal_stats(args):
    """
    Method: aggregateRas()
    Purpose: calculate raster statistics for each catchment, find the upstream network for each catchment and
            accumulate the raster statistics.
    Params: shapes - dict of fiona geometries for the catchments
            stat - statistic type to be calculated for the catchments (only needed for continuous data)
            rasPath - path to the raster data
            rasVals - list containing unique class values for a classed raster (empty list for continuous)
    Returns: sumDict - dictionary where the catchment ID is the key and the data is a list of catchment statistics and
                    accumulated statistics
    """
    keys, geoms, rasPath, rasVals, stat, isT2LU, isLCChange = args #detlete isT2LU
    # print("Calculating Catchment Statistics for ", len(keys), " catchments...")
    sumDict = {}
    #Calculate statistic for each catchment
    with rio.open(rasPath) as src:
        noData = src.nodatavals[0]
        if len(stat) > 0: #continuous raster
            for s in range(len(keys)):
                val = zonal_stats(src, geoms[s], stat, noData, isT2LU, isLCChange) #delete is T2LU
                sumDict[keys[s]] = [val]
        else: #classed raster
            rasVals = [int(r) for r in rasVals]
            for s in range(len(keys)):
                vals = tabArea(src, geoms[s], noData, rasVals)
                if vals[0] != -1:
                    sumDict[keys[s]] = vals
    return sumDict
         
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
        vals = list(vals)
        counts = list(counts)
        if noData in vals:
            i = vals.index(noData)
            vals.remove(noData) #update to ignore no data value
            del counts[i]
        finVals = [0 for x in range(len(rasVals))] #empty list with same length as unique raster values
        if len(vals) > 0:
            for idx, v in enumerate(vals): #for each class found within the catchment
                if int(v) in rasVals:
                    finVals[rasVals.index(int(v))] = counts[idx] #set count of unique class in same order as the unique raster values
        return finVals #returns list of counts
    except:
        return [0] * len(rasVals)

def zonal_stats(src, geom, statType, noData, isT2LU, isLCChange): #delete is T2LU
    """
    Method: zonal_stats()
    Purpose: Mask the continuous raster to the current catchment and calculate the specified statistic
            and raster area within the catchment. The statistic options are: MAX, MIN, MEAN, MEDIAN and SUM.
    Params: src - Rasterio open DatasetReader object for the raster
            geom - fiona geometry of the catchment
            statType - string denoting the statistic type
            convFact - conversion factor to be applied to the area
            noData - raster noData value 
    Returns: zonal_stat - catchment statistic
            area - catchment area
    """
    try:
        ary, t = rio.mask.mask(src, [geom], crop=True, all_touched=False) #mask by current catchment
        if ary.any():
            if isT2LU:
                all_errors_dict = get_lu_errors('ALL')
                for e in all_errors_dict: #reclass errors for T2 LU
                    ary = np.where(ary==e, all_errors_dict[e], ary)
            if isLCChange:
                ary = np.where(ary<=12, noData, ary)
            #set no data value to nan for float rasters and 0 for integer rasters
            if statType != 'MAJ':
                if type(ary[0][0][0]) == np.float32 or type(ary[0][0][0]) == np.float64:
                    ary[ary == noData] = np.nan #set nodata to nan 
                else:
                    ary[ary == noData] = 0
            if statType == "MAX":
                zonal_stat = float(np.amax(ary)) 
            elif statType == "MIN":
                zonal_stat = float(np.amin(ary)) 
            elif statType == "MEAN":
                zonal_stat = float(np.nanmean(ary)) 
            elif statType == "MEDIAN":
                zonal_stat = float(np.nanmedian(ary)) 
            elif statType == "SUM":
                zonal_stat = float(np.sum(ary))
            elif statType == 'MAJ':
                vals, counts = np.unique(ary, return_counts=True) 
                vals = list(vals)
                counts = list(counts)
                if noData in vals:
                    i = vals.index(noData)
                    vals.remove(noData) #update to ignore no data value
                    del counts[i]
                if len(vals) > 0:
                    zonal_stat = vals[counts.index(np.amax(counts))]
                else:
                    zonal_stat = 0
            return zonal_stat #returns value
        else:
            return 0
    except Exception as e:
        print(e)
        # sys.exit(1)
        # print("\n", ary, "\n")
        return -1

def maskRasByGeom_mp(rasPath, rasPath2, geoms, vals):
    """
    Method: maskRasByGeom_mp()
    Purpose: Build chunks of geometries and multiprocess maskRasByGeom().
    Params: rasPath - ras path to be masked
            rasPath2 - ras path to mask by
            geoms - list of geometries
            vals - list of raster values to limit rasPath2
    Returns: gdf - geodataframe of vectorized raster data
    """
    cpus_minus_1 = mp.cpu_count() - 2
    if cpus_minus_1 == 0:
        cpus_minus_1 = 1

    pool = mp.Pool(processes=cpus_minus_1)

    batch_size = len(geoms) / cpus_minus_1
    if batch_size % 1 != 0:
        batch_size = int((batch_size) + 1)
    else:
        batch_size = int(batch_size)

    chunk_iterator = []
    for i in range(cpus_minus_1): # was num_chunks
        mn, mx = i * batch_size, (i + 1) * batch_size
        data = geoms[mn:mx]
        gdf_args = rasPath, rasPath2, data, vals
        chunk_iterator.append(gdf_args)

    results = pool.map(maskRasByGeom, chunk_iterator) #list of dictionaries
    pool.close()

    data = gpd.GeoDataFrame(pd.concat(results, ignore_index=True), crs="EPSG:5070")
    data['PID'] = data.PID.astype(int)

    return data

def maskRasByGeom(args):
    """
    Method: maskRasByGeom()
    Purpose: For each geom in the list, mask both rasters by geometry. Then mask
             LC change raster by T2 LU raster where T2 LU is in list of vals. Vectorize
             the change results.
    Params: rasPath - ras path to be masked
            rasPath2 - ras path to mask by
            geoms - list of geometries
            vals - list of raster values to limit rasPath2
    Returns: gdf - geodataframe of vectorized raster data
    """
    rasPath, rasPath2, geoms, vals = args
    id_list, geom_list = [], []
    turf_val = get_lu_code('Turf Herbaceous', False)
    with rio.open(rasPath) as src_co:
        with rio.open(rasPath2) as src_co2:
            for id_field, geom in geoms:
                try: #smm added 6/18/21
                    out_image, out_transform = rio.mask.mask(src_co, [geom], crop=True)
                    out_image2, out_transform2 = rio.mask.mask(src_co2, [geom], crop=True)
                    out_image = np.where(np.isin(out_image, vals), 1, 0)
                    out_image = np.where(out_image2 == turf_val, 1, 0) # where T1 LC is LVB and T2 LU is TG
                    gdf = vectorizeRaster(out_image, out_transform)
                    for idx, row in gdf.iterrows():
                        geom_list.append(row['geometry'])
                        id_list.append(id_field)
                except:
                    continue
    gdf = gpd.GeoDataFrame(data={'PID':id_list}, geometry=geom_list, crs="EPSG:5070")
    return gdf

def maskRasByGeom_mp2(rasPath, geoms, vals):
    """
    Method: maskRasByGeom_mp2()
    Purpose: Build chunks of geometries and multiprocess maskRasByGeom().
    Params: rasPath - ras path to be masked
            geoms - list of geometries
            vals - list of raster values to limit rasPath2
    Returns: gdf - geodataframe of vectorized raster data
    """
    cpus_minus_1 = mp.cpu_count() - 2
    if cpus_minus_1 == 0:
        cpus_minus_1 = 1

    pool = mp.Pool(processes=cpus_minus_1)

    batch_size = len(geoms) / cpus_minus_1
    if batch_size % 1 != 0:
        batch_size = int((batch_size) + 1)
    else:
        batch_size = int(batch_size)

    chunk_iterator = []
    for i in range(cpus_minus_1): # was num_chunks
        mn, mx = i * batch_size, (i + 1) * batch_size
        data = geoms[mn:mx]
        gdf_args = rasPath, data, vals
        chunk_iterator.append(gdf_args)

    results = pool.map(maskRasByGeom2, chunk_iterator) #list of dictionaries
    pool.close()

    data = gpd.GeoDataFrame(pd.concat(results, ignore_index=True), crs="EPSG:5070")

    return data

def maskRasByGeom2(args):
    """
    Method: maskRasByGeom2()
    Purpose: For each geom in the list, mask raster by geometry. Vectorize
             the cmasked array where the array vals are in vals list.
    Params: rasPath - ras path to be masked
            geoms - list of geometries
            vals - list of raster values to vectorize
    Returns: gdf - geodataframe of vectorized raster data
    """
    rasPath, geoms, vals = args
    geom_list = []
    with rio.open(rasPath) as src_co:
        for geom in geoms:
            try: #smm added 6/18/21
                out_image, out_transform = rio.mask.mask(src_co, [geom], crop=True)
                out_image = np.where(np.isin(out_image, vals), 1, 0)
                gdf = vectorizeRaster(out_image, out_transform)
                for idx, row in gdf.iterrows():
                    geom_list.append(row['geometry'])
            except:
                continue
    gdf = gpd.GeoDataFrame(geometry=geom_list, crs="EPSG:5070")
    return gdf

def getWidth_mp(gdf, IDfield):
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
    cpus_minus_1 = mp.cpu_count() - 1
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
        if maxDist*2 >= 72: #only need to do second pass if first pass is under threshold
            mx_b = maxPt.bounds[0:2]
            bounds = (mx_b[0] - maxDist, mx_b[1] - maxDist, mx_b[0] + maxDist, mx_b[1] + maxDist)
            if bounds[0] < 0:
                bounds = (0.0, bounds[1], bounds[2], bounds[3])
            if bounds[1] < 0:
                bounds = (bounds[0], 0.0, bounds[2], bounds[3])
            maxDist, maxPt = findCenter(row['geometry'], bounds, False) # second pass

            if maxDist*2 >= 72:
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

    # if width == 0 or height == 0:
    #     print("\n\tError : ", bounds, "\n")
    #     if os.path.isfile(badGeoms) or os.path.isdir(badGeoms):
    #         bg = gpd.read_file(badGeoms, layer='bad_geoms')
    #         bg.loc[len(bg), 'geometry'] = poly
    #     else:
    #         bg = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:5070")
    #     bg.to_file(badGeoms, layer='bad_geoms', driver="GPKG")

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

    return maxDist, maxPt

def assignGroups(gdf, uid):
    """
    Method: assignGroups()
    Purpose: Assign unique group IDs of a geodataframe by looping through sjoin results.
    Params: gdf - geodataframe to group
            uid - unique ID field to relate the new GID field to
    Returns: groups - dataframe of uid and GID (unique group ids)
    """
    left_id, right_id = uid+'_left', uid+'_right'
    joined_table = sjoin_mp6(gdf, 15000, 'intersects', [left_id, right_id], gdf) 
    joined_table = joined_table[joined_table[left_id] != joined_table[right_id]]
    groups = pd.DataFrame()
    groups.loc[:, uid] = list(set(list(set(list(joined_table[left_id])+list(joined_table[right_id]))))) #all zones that are touching
    groups.loc[:, 'GID'] = 0 #set group id to 0
    gid = 1
    for zone in list(groups[uid]):
        if list(groups[groups[uid] == zone]['GID'])[0] == 0: #still needs group
            all_in_group = list(joined_table[joined_table[left_id] == zone][right_id])
            tmp = all_in_group.copy()
            while len(tmp) > 0:
                t = list(joined_table[joined_table[left_id] == tmp[0]][right_id])
                t = list( set(t) - set(all_in_group))
                tmp += t
                all_in_group += t
                del tmp[0]

            all_gids = list(set(list(groups[groups[uid].isin(all_in_group)]['GID']))) #should set of 0
            all_in_group += [zone]
            if len(all_gids) > 1 or all_gids[0] != 0: #should never happen
                groups.loc[groups[uid].isin(all_in_group), 'GID'] = all_gids[0]
            else:
                groups.loc[groups[uid].isin(all_in_group), 'GID'] = gid
                gid += 1
    return groups[[uid, 'GID']]

def indirectTC(tree_canopy_gdf, psegs, lu_change_gdf):
    """
    Method: indirectTC()
    Purpose: For remaining change whose T1 LC is tree canopy, class as Forest Forest, TCT or TOA based on
             patch size and adjacency to TG.
    Params: tree_canopy_gdf - lu_change_gdf where T1 is Tree canopy and does not have a T1 LU
            psegs - geodataframe of psegs
            lu_change_gdf - full change df that will be updated 
    Returns: lu_change_gdf - updated lu change 
    """
    if len(tree_canopy_gdf) > 0:
        try: # smm added 6/18/21 - put 1st line in try and added except lines
            forest_patches = tree_canopy_gdf[(tree_canopy_gdf['g_area'] >= 4047)&(~tree_canopy_gdf['GID'].isna())].dissolve(by='GID')
        except:
            forest_patches = tree_canopy_gdf[(tree_canopy_gdf['g_area'] >= 4047)&(~tree_canopy_gdf['GID'].isna())]
            forest_patches['geometry'] = forest_patches.geometry.buffer(0.0)
            forest_patches = forest_patches.dissolve(by='GID')
        forest_patches = forest_patches.reset_index().explode()[['GID', 'geometry']]
        tmp = tree_canopy_gdf[(tree_canopy_gdf['g_area'] >= 4047)&(tree_canopy_gdf['GID'].isna())].explode()[['zone', 'geometry']]
        max_gid = int(np.nanmax(lu_change_gdf['GID'])) + 1
        tmp.loc[:, 'GID'] = [int(x) for x in range(max_gid, len(tmp)+max_gid)]
        forest_patches.append(tmp[['GID', 'geometry']])
        tmp = tmp[['zone', 'GID']]
        forest_patches = getWidth_mp(forest_patches, 'GID') # get list of GIDs that are forest
        zones = list(tmp[tmp['GID'].isin(forest_patches)]['zone']) + list(tree_canopy_gdf[tree_canopy_gdf['GID'].isin(forest_patches)]['zone'])
        lu_change_gdf.loc[lu_change_gdf['zone'].isin(zones) & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Forest Forest', False)
        
        tree_canopy_gdf = tree_canopy_gdf[~tree_canopy_gdf['zone'].isin(zones)] # under an acre patches
        zones_to_drop = list(lu_change_gdf[(lu_change_gdf['LC_Change']=='Tree Canopy to Tree Canopy')&(lu_change_gdf['T1_LU_Code']==0)]['zone'])
        lu_change_gdf = lu_change_gdf[~lu_change_gdf['zone'].isin(zones_to_drop)] # remove T2 patches I added that weren't forest
        if len(tree_canopy_gdf) > 0: # < acre area - use surrounding tc to determine if TCT or TOA
            all_lus = list(set(list(psegs['lu'])))
            if None in all_lus:
                all_lus.remove(None)
            turf_classes = [x for x in all_lus if 'turf' in x or 'Turf' in x]
            joined_table = sjoin_mp6(psegs[psegs['lu'].isin(turf_classes)], 15000, 'intersects', ['zone'], tree_canopy_gdf)
            ns_zones = list(lu_change_gdf[lu_change_gdf['LC_Change']=='Tree Canopy to Tree Canopy NS']['zone'])
            tct = list(set(list(joined_table['zone'])) - set(ns_zones))
            lu_change_gdf.loc[(lu_change_gdf['zone'].isin(ns_zones)) & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Tree Canopy in Agriculture', False)
            lu_change_gdf.loc[lu_change_gdf['zone'].isin(tct) & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Tree Canopy over Turf', False)
            rem_zones = list(tree_canopy_gdf['zone'])
            lu_change_gdf.loc[(lu_change_gdf['zone'].isin(rem_zones)) & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Tree Canopy in Agriculture', False)
    return lu_change_gdf

def indirectLV(low_veg_gdf, lu_change_gdf):
    """
    Method: indirectLV()
    Purpose: For remaining LC change whose T1 LC is LV, class LU as Cropland, Pasture or Suspended Succession.
             TYPE field to reclass to crop, pasture or suspended.
    Params: low_veg_gdf - gdf with full attributes for change whose T1 LC is LV and is unclassed
            lu_change_gdf - lu change gdf to update
            lcmap_85_17_path - path to LCMAP patterns raster
    Returns: lu_change_gdf - updated lu change
    # """
    lu_change_gdf.loc[(lu_change_gdf['TYPE']=='crop') & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Cropland Herbaceous', False)
    lu_change_gdf.loc[(lu_change_gdf['TYPE']=='pasture') & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Pasture/Hay Herbaceous', False)
    lu_change_gdf.loc[(lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Suspended Succession Herbaceous', False) 

    return lu_change_gdf

def indirectBarr(barr_gdf, lu_change_gdf,):
    """
    Method: indirectBarr()
    Purpose: For remaining LC change whose T1 LC is barren, class T1 LU as either Crop, pasture or natural succession.
    Params: barr_gdf - gdf with full attributes for change whose T1 LC is barren and is unclassed
            lu_change_gdf - lu change gdf to update
    Returns: lu_change_gdf - updated lu change
    """
    lu_change_gdf.loc[(lu_change_gdf['TYPE']=='crop') & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Cropland Barren', False)
    lu_change_gdf.loc[(lu_change_gdf['TYPE']=='pasture') & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Pasture/Hay Barren', False)
    lu_change_gdf.loc[(lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Natural Succession Barren', False) 

    return lu_change_gdf

def indirectSS(scrub_gdf, lu_change_gdf):
    """
    Method: indirectSS()
    Purpose: For LC change with T1 LC scrub shrub who does not yet have a T1 LU, class T1 LU as either
                orchard, pasture, or natural succession using TYPE field.
    Params: scrub_gdf - gdf of T1 LC scrub shrub who does not yet have a T1 LU
            lu_change_gdf - lu change gdf to update
    Returns: lu_change_gdf
    """
    rem_zones = list(scrub_gdf['zone'])
    lu_change_gdf.loc[(lu_change_gdf['zone'].isin(rem_zones))&(lu_change_gdf['TYPE']=='crop') & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Orchard/Vineyard Scrub/Shrub', False)
    lu_change_gdf.loc[(lu_change_gdf['zone'].isin(rem_zones))&(lu_change_gdf['TYPE']=='pasture') & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Pasture/Hay Scrub/Shrub', False)
    lu_change_gdf.loc[(lu_change_gdf['zone'].isin(rem_zones))&(lu_change_gdf['TYPE']=='other') & (lu_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = get_lu_code('Natural Succession Scrub/Shrub', False)
 
    return lu_change_gdf
