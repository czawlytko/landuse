"""
Script: lu_change_vector_v1_callable.py
Purpose: Vectorize LC change from LC change raster and determine the T1 LU for each polygon. Once all T1 LU has been defined
         for all areas of change, the T1 LU is rasterized. The T1 LU and the T2 LU from the LU model are both rolled up to the
         13, Phase 6 classes and overlaid to create LU Change. The LU Change for the 13 classes is summarized as a table (in acres)
         and written as a CSV.
         Determining if a parcel is crop, pasture or non-ag is done by using NLCD 2011, CDL 2013, LCMAP Patterns data from 2010-2017, and
         the T2 LU. If NLCD, CDL or LCMAP detects any ag class in the parcel, then the majority T2 LU class in the parcel is calculated.
         If the majority is Cropland, it is crop; if it is Pasture, it is pasture; else it is other.
         There are 4 approaches to determine the T1 LU:
         1. Direct - The T1 LU is the same as the T1 LC in the change product and is a direct translation
         2. New Structure - The goal of this approach is to capture all LU change in newly developed parcels. Since the LC
                             may have only changed where the structure was built, this method will back cast all Turf Grass
                             as either Crop, Pasture or Suspended Succession. In addition, the buffers used to create TCT in the Trees
                             Over LU sub model are used to find and vectorize all TC that was TCT in T2 but should not be TCT in T1 - T1
                             will always roll up to forest.
        3. Context - The Context based approach uses a rule set for specified classes using only the LC change polygons, T2 LU and
                      the 'Other' column (defines if the parcel sharing most area with change is maj crop, pas or other). This approach
                      uses context based on these 3 sources of information plus human logic to determine what the T1 LU should be. For 
                      example, if the LC change is LV to TC and the T2 LU is TCT, then T1 is TG.
        4. Indirect - This approach captures the remaining classes of LC change that is difficult to determinea  ruleset given the 
                    numerous possibilities. This approach uses the psegs from T2 LC to find like LC in T2. The T2 LU area is determined for
                    each of the psegs that are touching change of like-LC class and summed by unique change segment to get total surrounding area.
                    If the surrounding area is at least 25% of the change area, the T1 LU of the change segment is the LU with the highest area.
                    Wetlands are removed as an option for this approach. 
                    Not all change segments are surrounded by segments of the same LC class. These segments default to a generic ruleset based
                    on their LC. For TC, all T1 TC and T2 TCT patches based on size (tct layer from trees over gpkg) are sjoined and given 
                    group IDs to determine patch size in T1. If the total patch size is over an acre, the TCT polygons are added as a potential
                    LU change, since they may have been Forest in T1. The TC rules will class all TC as either:
                            Forest - Patch area > acre and patch width >= 72 meters
                            Trees in ag - Patch area < acre and not touching TG in T2 (psegs); All TCT polygons from New Structure method
                                            are classed here as well by default if < acre in area.
                            TCT - Patch area is < acre and touching TG in T2 (psegs)
Author: Sarah McDonald, Geographer, U.S. Geological Survey
Contact: smcdonald@chesapeakebay.net
"""

from osgeo import gdal
import fiona
import numpy as np
import rasterio as rio
import rasterio.mask
from rasterio.windows import from_bounds
from rasterio.features import rasterize
import pandas as pd
import geopandas as gpd
import time
import sys
import os

sys.path.insert(0,'..')

from lu_change import lu_change_helpers_v1 as lch
from lu_change import readXML as xml 
from lu_change import LU_Change_P6Rollup as p6_change
from helpers import etime 
import helpers
import luconfig

def runDirect(lc_change_gdf):
    """
    Method: runDirect()
    Purpose: Run direct approach using the direct_dict in helpers. The T1 LU is the same as its T1 LC.
    Params: lc_change_gdf - geodataframe of LC Change
    Returns: lc_change_gdf - original gdf with updated T1_LU_Code field
    """
    direct_dict = lch.getDirectChange('ALL')
    for lc in direct_dict:
        lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T1_LU_Code']==0), 'Method'] = 'Direct'
        lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = direct_dict[lc]
    return lc_change_gdf

def runNewStructure(lc_change_gdf, parcels_gdf, lc_change_ras_path, lc_change_dict, tab_area_df, lu_2017_ras_path, tct_gdf, raster_parcels_path):
    """
    Method: runNewStructure()
    Purpose: Run new structure approach by back-casting all turf grass in newly developed parcels as crop, pasture or
             suspended succession. Vectorize all T1 TC that is not LC change within TCT buffers from trees over sub module
             to be back-casted from TCT in the indirect method.
    Params: lc_change_gdf - geodataframe of LC Change
            parcels_gdf - geodataframe of parcels
            lc_change_ras_path - path to lc change raster
            lc_change_dict - dictionary relating LC Change raster values and their descriptions
            tab_area_df - dataframe containing parcel scale info - 'TYPE' column
            lu_2017_ras_path - path to T2 LU raster
            tct_gdf - geodataframe of TCT buffers from layer tct_bufs in trees_over.gpkg
    Returns: lc_change_gdf - original gdf with updated T1_LU_Code field
    """
    lc_change_classes = ['Low Vegetation to Structures']
    optional_classes = ['Low Vegetation to Tree Canopy',
                        'Low Vegetation to Barren',
                        'Low Vegetation to Other Impervious Surfaces',
                        'Low Vegetation to Roads',
                        'Low Vegetation to Scrub\\Shrub',
                        'Barren to Tree Canopy',
                        'Barren to Low Vegetation',
                        'Barren to Other Impervious Surfaces',
                        'Barren to Roads',
                        'Barren to Scrub\\Shrub',
    ]

    # vals to use for lvb to ... classes
    crop_val = lch.get_lu_code('Cropland Herbaceous', False)
    pas_val = lch.get_lu_code('Pasture/Hay Herbaceous', False)
    sussuc_val = lch.get_lu_code('Suspended Succession Herbaceous', False)
    
    # Get all new struct classes  
    new_struct_classes = ['Barren to Structures', 
                        'Emergent Wetlands to Structures',
                        'Scrub\Shrub to Structures', 
                        'Tree Canopy to Structures'] + lc_change_classes

    # get total area of structures and other imp in T1 from lc change for each parcel
    struct_threshold = 55 #square meters of area; >= to this is developed
    parcel_dict = {row['PID']:row['geometry'] for idx, row in parcels_gdf.iterrows()}
    dev_values = [  lc_change_dict['Structures'],
                    lc_change_dict['Other Impervious Surfaces'],
                    lc_change_dict['Tree Canopy Over Structures'],
                    lc_change_dict['Tree Canopy Over Other Impervious Surfaces'],
                    lc_change_dict['Low Vegetation'],
                    lc_change_dict['Barren'],
                ]
    for nsc in new_struct_classes:
        dev_values.append(lc_change_dict[nsc])
    stuct_df = lch.zonal_stats_mp(parcel_dict, '', lc_change_ras_path, dev_values, ['PID', 'Structure', 'OtherImp', 'TC_Struct', 'TC_Imp', 'LV', 'Barr', 'BNS', 'EWS', 'SSNS', 'TCNS', 'LVNS'], False, False)
    del parcel_dict
    stuct_df['New_Struct'] = stuct_df[['BNS', 'EWS', 'SSNS', 'TCNS', 'LVNS']].sum(axis=1)
    stuct_df = stuct_df[stuct_df['New_Struct'] >= struct_threshold] # parcels containing new structure
    stuct_df['Tot_Struct'] = stuct_df[['Structure', 'TC_Struct']].sum(axis=1)
    stuct_df['Tot_Imp'] = stuct_df[['OtherImp', 'TC_Imp']].sum(axis=1)
    stuct_df = stuct_df[(stuct_df['Tot_Struct'] < struct_threshold)] #new struct parcels only
    stuct_df = stuct_df[(stuct_df['LV'] > 0)|(stuct_df['Barr'] > 0)] # new struct and have LV or Bar to reclass
    new_parcels_list = list(stuct_df['PID'])
    parcels_gdf = parcels_gdf[parcels_gdf['PID'].isin(new_parcels_list)]
    parcels_gdf = parcels_gdf.merge(tab_area_df, on='PID', how='left')
    del stuct_df

    # add LVB no change in new structure parcels
    # use geoms in parcels_gdf to mask change ras - vectorize LV and Barren
    parcels = [(row['PID'], row['geometry']) for idx, row in parcels_gdf.iterrows()]
    lvb = [  lc_change_dict['Low Vegetation'],
            lc_change_dict['Barren']
        ]
    # vectorized LVB in parcels that are newly developed - with pid
    lvb_gdf = lch.maskRasByGeom_mp(lc_change_ras_path, lu_2017_ras_path, parcels, lvb) # vector of LVB in newly dev parcels who are TG in T2
    if len(lvb_gdf) > 0:
        lvb_gdf = lvb_gdf[['PID', 'geometry']]
        lvb_gdf = lvb_gdf.merge(tab_area_df[['PID','TYPE', 'Type_log']], on='PID', how='left')
        #order lc_change_gdf and lvb_gdf columns the same
        max_zone = np.amax(lc_change_gdf['zone']+1)
        lvb_gdf.loc[:, 'zone'] = [int(x) for x in range(max_zone, len(lvb_gdf)+max_zone)]
        lvb_gdf = lvb_gdf[['zone', 'PID', 'TYPE', 'geometry']]
        lvb_gdf.loc[:, 'Method'] = 'New Structure - Parcel'
        lvb_gdf.loc[:, 'T2_LU_Val'] = lch.get_lu_code('Turf Herbaceous', False)
        lvb_gdf.loc[:, 'T1_LU_Code'] = 0
        #add new vectors to table and give it T1_LU_Code
        lc_change_gdf = lc_change_gdf.append(lvb_gdf)
        
    del lvb_gdf

    # Group newly developed parcels to find new neighborhoods that should be back-casted the same
    parcel_groups = lch.assignGroups(parcels_gdf[['PID', 'geometry']], 'PID')
    ## SMM 6/18/21 - REPLACED GID WITH PAR_GID
    parcel_groups = parcel_groups.rename(columns={'GID':'PAR_GID'})
    lc_change_gdf = lc_change_gdf.merge(parcel_groups[['PID', 'PAR_GID']], how='left')

    # Get majority TYPE for each unique GID
    gids = list(set(list(parcel_groups['PAR_GID'])))
    for gid in gids:
        # if some are equal counts, use mode to get list to give precendence to ag classes
        types = list(lc_change_gdf[lc_change_gdf['PAR_GID'] == gid][['TYPE']].mode()['TYPE']) 
        if 'crop' in types:
            parcel_groups.loc[parcel_groups['PAR_GID'] == gid, 'GRP_TYPE'] = 'crop'
        elif 'pasture' in types:
            parcel_groups.loc[parcel_groups['PAR_GID'] == gid, 'GRP_TYPE'] = 'pasture'
        else:
            parcel_groups.loc[parcel_groups['PAR_GID'] == gid, 'GRP_TYPE'] = 'other'
    
    lc_change_gdf = lc_change_gdf.merge(parcel_groups[['PID', 'GRP_TYPE']],on='PID', how='left')

    # find Barren to LV polys whose area is at least 65% shared with newly dev parcels
    barr_lv_dict = {row['zone']:row['geometry'] for idx, row in lc_change_gdf[lc_change_gdf['LC_Change']=='Barren to Low Vegetation'].iterrows()}
    barr_lv_df = lch.zonal_stats_mp(barr_lv_dict, '', raster_parcels_path, [int(x) for x in new_parcels_list], ['zone']+new_parcels_list, False, False)
    barr_lv_df.loc[:, 'ns_area'] = barr_lv_df[new_parcels_list].sum(axis=1)
    barr_lv_gdf = lc_change_gdf[lc_change_gdf['LC_Change']=='Barren to Low Vegetation'][['zone', 'geometry']]
    barr_lv_gdf.loc[:, 'zone_area'] = barr_lv_gdf.geometry.area
    barr_lv_df = barr_lv_df.merge(barr_lv_gdf[['zone', 'zone_area']], on='zone')
    del barr_lv_gdf
    barr_lv_df.loc[:, 'pctNS'] = barr_lv_df['ns_area'] / barr_lv_df['zone_area']
    barr_lv_df = barr_lv_df[barr_lv_df['pctNS'] > 0.5]
    barr_lv_zones = list(barr_lv_df['zone'])
    del barr_lv_df


    # Assign T1 LU using GRP_TYPE where available, otherwise use TYPE
    reclass_classes = optional_classes + lc_change_classes
    g_zones = list(lc_change_gdf[(lc_change_gdf['PID'].isin(new_parcels_list))&(lc_change_gdf['LC_Change'].isin(reclass_classes))&(~lc_change_gdf['GRP_TYPE'].isna())]['zone'])
    zones = list(lc_change_gdf[(lc_change_gdf['PID'].isin(new_parcels_list))&(lc_change_gdf['LC_Change'].isin(reclass_classes))&(lc_change_gdf['GRP_TYPE'].isna())]['zone'])
    zones += barr_lv_zones
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(g_zones))&(lc_change_gdf['T1_LU_Code']==0), 'Method'] = 'New Structure'
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(g_zones))&(lc_change_gdf['GRP_TYPE']=='crop')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  crop_val
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(g_zones))&(lc_change_gdf['GRP_TYPE']=='pasture')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  pas_val
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(g_zones))&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  sussuc_val
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(zones))&(lc_change_gdf['T1_LU_Code']==0), 'Method'] = 'New Structure'
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(zones))&(lc_change_gdf['TYPE']=='crop')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  crop_val
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(zones))&(lc_change_gdf['TYPE']=='pasture')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  pas_val
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(zones))&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  sussuc_val 
    # Assign T1 LU to LV to Struct class specifically using GRP_TYPE if available
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['T1_LU_Code']==0), 'Method'] = 'New Structure'
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['GRP_TYPE']=='crop')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  crop_val
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['GRP_TYPE']=='pasture')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  pas_val
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['GRP_TYPE']=='other')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  sussuc_val
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['T1_LU_Code']==0), 'Method'] = 'New Structure'
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['TYPE']=='crop')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  crop_val
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['TYPE']=='pasture')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  pas_val
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(lc_change_classes))&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] =  sussuc_val 


    # back out tct bufs from newly developed parcels
    # add tc within bufs containing new struct to lc change gdf
    tct_gdf.loc[:, 'ID'] = [int(x) for x in range(1, len(tct_gdf)+1)]
    ns_tmp = lc_change_gdf[lc_change_gdf['LC_Change'].isin(new_struct_classes)][['zone', 'geometry']]
    ns_tmp.loc[:, 'area'] = ns_tmp.geometry.area
    ns_tmp = ns_tmp[ns_tmp['area'] >= struct_threshold]
    joined_table = lch.sjoin_mp6(tct_gdf[['ID', 'geometry']], 15000, 'contains', ['ID'], ns_tmp)
    tct_gdf = tct_gdf[tct_gdf['ID'].isin(list(joined_table['ID']))]
    # ZONAL SUM OF STRUCT CLASSES IN T1 TO REMOVE ALREADY DEV BUFFERS
    dev_values = [  lc_change_dict['Structures'],
                lc_change_dict['Tree Canopy Over Structures'],
            ]
    buffers = {row['ID']:row['geometry'] for idx, row in tct_gdf.iterrows()}
    old_struct = lch.zonal_stats_mp(buffers, '', lc_change_ras_path, dev_values, ['ID', 'Structure', 'TC_Struct'], False, False)
    old_struct['tot_struct'] = old_struct[['Structure', 'TC_Struct']].sum(axis=1)
    old_struct = list(old_struct[old_struct['tot_struct'] < struct_threshold]['ID']) # was not dev before
    tct_gdf = tct_gdf[tct_gdf['ID'].isin(old_struct)]
    del joined_table
    if len(tct_gdf) > 0: # SMM added 6/18/21
        tct_gdf = lch.maskRasByGeom_mp2(lc_change_ras_path, list(tct_gdf['geometry']), [3]) #vectorize tc within buffers of newly dev parcels
        tct_gdf = tct_gdf.reset_index()[['geometry']]
        tct_gdf.loc[:, 'LC_Change'] = 'Tree Canopy to Tree Canopy NS'
        st_zone = np.amax(lc_change_gdf['zone'])
        tct_gdf.loc[:, 'zone'] = [int(x) for x in range(st_zone, st_zone+len(tct_gdf))]
        tct_gdf.loc[:, 'T2_LU_Val'] = lch.get_lu_code('Tree Canopy over Turf', False)
        tct_gdf.loc[:, 'T1_LU_Code'] = 0
        tct_gdf.loc[:, 'Method'] = 'Indirect Rules'
        cols = ['zone', 'LC_Change', 'T2_LU_Val', 'T1_LU_Code', 'Method', 'geometry']
        lc_change_gdf = lc_change_gdf.append(tct_gdf[cols])
    else:
        print("No TC in buffers for new dev parcels")

    return lc_change_gdf

def runContextBase(lc_change_gdf):
    """
    Method: runContextBase()
    Purpose: Run context based approach using the context_dict in helpers. All rules used in the dict
             and this approach are:
             1. AG - crop / AG - pas: means use TYPE field to check ag type and assign T1 LU code
             2. else: means any lC change of this class that does not have a T1 LU gets this code
             3. Name of T2 LU class: if T2 lu == this class, assign T1 LU specified value
    Params: lc_change_gdf - geodataframe of LC Change
    Returns: lc_change_gdf - original gdf with updated T1_LU_Code field
    """
    context_dict = lch.getContextChange('ALL') # lc change classes with dict of rules
    all_change = list(set(list(lc_change_gdf['LC_Change'])))
    for lc in context_dict:
        if lc in all_change: # this LC change class exists in the county
            lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T1_LU_Code']==0), 'Method'] = 'Context'
            for rule in context_dict[lc]: #lc is lc change name key to dict
                if rule == 'AG - crop':
                    lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['TYPE']=='crop')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = context_dict[lc][rule]
                elif rule == 'AG - pas':
                    lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['TYPE']=='pasture')&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = context_dict[lc][rule]
                elif rule == 'else':
                     lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = context_dict[lc][rule]
                elif rule == 'Wetland - Tidal':
                    lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T2_LU_Val'].isin(lch.getWetlandTypes('Tidal')))&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = context_dict[lc][rule]
                elif rule == 'Wetland - Floodplain':
                    lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T2_LU_Val'].isin(lch.getWetlandTypes('Floodplain')))&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = context_dict[lc][rule]
                elif rule == 'Wetland - Other':
                    lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T2_LU_Val'].isin(lch.getWetlandTypes('Other')))&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = context_dict[lc][rule]
                elif lch.get_lu_code(rule, False) != -1:
                    t2lu = lch.get_lu_code(rule, False)
                    lc_change_gdf.loc[(lc_change_gdf['LC_Change']==lc)&(lc_change_gdf['T2_LU_Val']==t2lu)&(lc_change_gdf['T1_LU_Code']==0), 'T1_LU_Code'] = context_dict[lc][rule]
                else:
                    print("ERROR: ", rule, " is not a viable option for context method - please use 'AG - crop', 'AG - pas', 'else' or LU name in lu_code_dict")
    return lc_change_gdf

def runIndirect(lc_change_gdf, psegs, lu_2017_ras_path, t1_tc_gdf):
    """
    Method: runIndirect()
    Purpose: Determine T1 LU for remaining LC Change classes (all T1 natural class). Use surrounding T2 info
             to try to gain context and determine T1 LU, otherwise default to set of rules. Find all TCT patches
             in T2 that were connected to TC that existed in T1 but not T2 and determine if the TCT should be
             back-casted to forest.
    Params: lc_change_gdf - geodataframe of LC change
            psegs - geodataframe of T2 LC segments unioned with parcels
            lu_2017_ras_path - path to T2 LU raster
            t1_tc_gdf - geodataframe of TCT patches by < acre and width rules; tct layer from trees_over.gpkg
    Returns: lc_change_gdf - original gdf with updated T1_LU_Code field
    """
    indirect_list = lch.getIndirectChange()
    lc_class_names = ['Barren', 'Low Vegetation', 'Scrub\\Shrub', 'Tree Canopy']
    t = lc_change_gdf['LC_Change'].str.split(' to ', n = 1, expand = True)
    lc_change_gdf.loc[:, 'T1LC'] = t[0]
    # reclass emergent wetlands to LV
    lc_change_gdf.loc[lc_change_gdf['T1LC'] == 'Emergent Wetlands', 'T1LC'] = 'Low Vegetation'
    del t
    lc_change_gdf.loc[:, 'chg_area'] = lc_change_gdf.geometry.area
    joined_table = lch.sjoin_mp6(psegs[psegs['Class_name'].isin(lc_class_names)], 15000, 'intersects', ['SID', 'zone', 'Class_name', 'T1LC', 'chg_area'], lc_change_gdf) 

    # tab area for all natural PSID LCs touching any change left
    use_T2_LU_zones = list(set(list(joined_table['zone']))) 
    shapes = {row['SID']:row['geometry'] for idx, row in psegs[psegs['SID'].isin(list(joined_table['SID']))].iterrows()}
    
    lu_list_vals = list(set(list(lch.get_lu_code('ALL', False).values())))

    lu_list_vals_str = [str(x) for x in lu_list_vals]
    lu2017df = lch.zonal_stats_mp(shapes, '', lu_2017_ras_path, lu_list_vals, ['SID']+lu_list_vals_str, True, False)
    lu2017df = lu2017df.merge(joined_table, on='SID', how='left')
    # Remove all wetlands classes
    wetlands = [str(x) for x in lch.getWetlandTypes('Tidal')]
    wetlands += [str(x) for x in lch.getWetlandTypes('Floodplain')]
    wetlands += [str(x) for x in lch.getWetlandTypes('Other')]
    lu2017df.loc[:, 'wetlands'] = lu2017df[wetlands].sum(axis=1)
    lu2017df[str(lch.get_lu_code('Forest Forest', False))] = lu2017df[str(lch.get_lu_code('Forest Forest', False))] + lu2017df['wetlands']#add wetlands total to forest
    wetlands.append('wetlands')
    lu2017df.drop(wetlands, axis=1, inplace=True)
    lu2017df = lu2017df[(lu2017df['Class_name'] == lu2017df['T1LC'])] # remove records where LC types don't match
    cols = [x for x in list(lu2017df) if x not in ['SID', 'Class_name']]
    lu2017df = lu2017df[cols].groupby(['zone', 'T1LC', 'chg_area']).agg('sum').reset_index() #sum all non-wetland LUs by zone
    cols.remove('zone')
    cols.remove('T1LC')
    cols.remove('chg_area')
    print(cols)
    # REMOVE COLUMNS WHERE MAX IS < 25% OF SEG AREA
    lu2017df.loc[:, 'pct_lu'] = lu2017df[cols].max(axis=1) / lu2017df['chg_area']
    # Don't remove TC if touching Forest class
    lu2017df.loc[(lu2017df['T1LC']=='Tree Canopy')&(lu2017df[str(lch.get_lu_code('Forest Forest', False))]>0), 'pct_lu'] = 1
    lu2017df.loc[(lu2017df['T1LC']=='Tree Canopy')&(lu2017df[str(lch.get_lu_code('Forest', False))]>0), 'pct_lu'] = 1
    lu2017df = lu2017df[lu2017df['pct_lu'] >= 0.25]
    lu2017df.loc[:, 'SID_LU'] = lu2017df[cols].idxmax(axis="columns") # set SID to column name of max tab area for zone
    # If TC is touching Forest - it is forest
    lu2017df.loc[(lu2017df['T1LC']=='Tree Canopy')&(lu2017df[str(lch.get_lu_code('Forest Forest', False))]>0), 'SID_LU'] = str(lch.get_lu_code('Forest Forest', False))
    lu2017df = lu2017df[['zone', 'SID_LU', 'T1LC']]
    lu2017df.loc[:, 'SID_LU'] = lu2017df.SID_LU.astype(int)

    foundZones = list(lu2017df[lu2017df['SID_LU'] != 0]['zone'])
    lc_change_gdf = lc_change_gdf.merge(lu2017df[['zone', 'SID_LU']], on='zone', how='left')
    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(foundZones)) & (lc_change_gdf['T1_LU_Code'] == 0), 'T1_LU_Code'] = lc_change_gdf['SID_LU']
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(indirect_list))&(lc_change_gdf['T1_LU_Code']!=0), 'Method'] = 'Indirect Adjacency'
    lc_change_gdf.loc[(lc_change_gdf['LC_Change'].isin(indirect_list))&(lc_change_gdf['T1_LU_Code']==0), 'Method'] = 'Indirect Rules'
    lc_change_gdf.drop(['SID_LU'], axis=1, inplace=True)

    # All other rules failed - use ancillary for remaining zones
    rem_zones = list(set(list(lc_change_gdf[(lc_change_gdf['LC_Change'].isin(indirect_list + ['Tree Canopy to Tree Canopy']))&(lc_change_gdf['T1_LU_Code']==0)]['zone'])))

    # Run rules for TC
    # get groups of TC in T1
    st_zone = np.amax(lc_change_gdf['zone']) + 1
    t1_tc_gdf.loc[:, 'zone'] = [int(x) for x in range(st_zone, st_zone+len(t1_tc_gdf))]
    all_forest = lc_change_gdf[lc_change_gdf['T1LC'] == 'Tree Canopy'][['zone', 'geometry']].append(t1_tc_gdf)
    groups = lch.assignGroups(all_forest, 'zone')
    groups_with_change = list(groups[groups['zone']<st_zone]['GID']) #groups containing lc change
    groups = groups[groups['GID'].isin(groups_with_change)] #remove groups that contain no change
    all_forest.loc[:, 'g_area'] = all_forest.geometry.area
    groups = groups.merge(all_forest[['zone', 'g_area']], on='zone', how='left')
    group_area = groups[['GID', 'g_area']].groupby(['GID']).agg('sum').reset_index()
    groups = groups[['zone', 'GID']].merge(group_area, on='GID')
    lc_change_gdf = lc_change_gdf.merge(groups, on='zone', how='left') # add group area

    all_forest = all_forest.merge(groups[['zone', 'GID', 'g_area']], on='zone', how='right')
    all_forest = all_forest[~all_forest['zone'].isin(list(lc_change_gdf['zone']))] #get t1_tc patches touching tc change
    if len(all_forest) > 0:
        all_forest.loc[~all_forest['g_area_y'].isna(), 'g_area_x'] = all_forest['g_area_y'] # where there is group area - use it - otherwise keep seg area
        all_forest = all_forest[['zone', 'GID', 'g_area_x', 'geometry']]
        all_forest = all_forest.rename(columns={'g_area_x':'g_area'})
        all_forest = all_forest[all_forest['g_area'] >= 4047]
        
        if len(all_forest)>0:
            all_forest.loc[:, 'Method'] = 'Indirect - T1 TC'
            all_forest.loc[:, 'LC_Change'] = 'Tree Canopy to Tree Canopy'
            all_forest.loc[:, 'T1_LU_Code'] = 0
            all_forest.loc[:, 'T1LC'] = 'Tree Canopy'
            lc_change_gdf = lc_change_gdf.append(all_forest[['zone', 'g_area', 'GID', 'Method', 'LC_Change', 'T1_LU_Code', 'T1LC', 'geometry']]) # add T1 TC touching tc change
            print(lc_change_gdf.head())
            cols = list(lc_change_gdf)
            lc_change_gdf = lc_change_gdf.reset_index()[cols]
    # lc_change_gdf = lc_change_gdf[[cols]].reset_index() # labeebs suggestion to try, should do the same thing as above; passing list of list of list? smm thinks this will break, and this adds index column that I don't want

    lc_change_gdf.loc[(lc_change_gdf['zone'].isin(rem_zones))&(lc_change_gdf['T1LC'] == 'Tree Canopy')&(lc_change_gdf['g_area'].isna()), 'g_area'] = lc_change_gdf.geometry.area
    lc_change_gdf = lch.indirectTC(lc_change_gdf[(lc_change_gdf['T1LC'] == 'Tree Canopy')&(lc_change_gdf['T1_LU_Code']==0)], psegs, lc_change_gdf)
    # Run rules for Barren
    lc_change_gdf = lch.indirectBarr(lc_change_gdf[(lc_change_gdf['zone'].isin(rem_zones)) & (lc_change_gdf['T1LC'] == 'Barren')], lc_change_gdf)
    # Run rules for LV
    lc_change_gdf = lch.indirectLV(lc_change_gdf[(lc_change_gdf['zone'].isin(rem_zones)) & (lc_change_gdf['T1LC'] == 'Low Vegetation')], lc_change_gdf)
    # Run rules for SS
    lc_change_gdf = lch.indirectSS(lc_change_gdf[(lc_change_gdf['zone'].isin(rem_zones)) & (lc_change_gdf['T1LC'] == 'Scrub\\Shrub')], lc_change_gdf)

    return lc_change_gdf

def run_lu_change(cf, lu_type):
    import luconfig
    st_time = time.time()
    print('############################')
    print(f'####### {cf} - lu change #########')
    print('############################')

    print("\t", luconfig.crosswalk_csv)
    print("\t", luconfig.lu_change_csv)

    # try: # if a county fails don't disrupt other counties
    folder = luconfig.folder

    anci_folder = luconfig.anci_folder

    main_path = os.path.abspath(f"{folder}/{cf}")
    input_path = os.path.join(main_path, 'input')
    output_path = os.path.join(main_path, 'output')
    temp_path = os.path.join(main_path, 'temp')

    lc_change_ras_path = helpers.rasFinder(input_path, f"{cf}_landcoverchange_????_????_*.tif")
    lu_2017_ras_path = os.path.join(main_path, 'output', f"{cf}_lu_2017_2018.tif") #updated
    psegs_gpkg_path = os.path.join(main_path, 'output', 'data.gpkg')
    trees_over_gpkg_path = os.path.join(main_path, 'output', 'trees_over.gpkg')
    parcels_path = os.path.join(main_path, 'temp', 'temp_dataprep.gdb') 

    # raster_parcels_path = os.path.join(main_path, 'temp', 'parcels_rasterized.tif') # is this what this will be called?
    raster_parcels_path = helpers.rasFinder(temp_path, "*parcels*.tif")

    nlcd11_path = os.path.join(anci_folder, 'NLCD', 'nlcd_2011_10m.tif')
    lcmap_11_19_path = os.path.join(anci_folder, 'lcmap', '10m_PreDev','Primary_2y_2011_2019_projected.tif')

    cdl13_path = os.path.join(anci_folder, 'CDL', '2013_10m_cdls_arc.tif')

    # get county boundary poly
    cnty_boundary = lch.getCntyBoundary(anci_folder, cf)
    etime(cf, 'Opened county boundary polygon', st_time)
    st = time.time()

    # read in rasters - clipped to county boundary
    lc_change_ary, lc_change_meta = lch.clipRasByCounty(cnty_boundary, lc_change_ras_path)
    etime(cf, 'Opened LC Change Raster', st)
    st = time.time()

    # make no data 0 for change raster
    lc_change_ary = np.where(lc_change_ary == 255, 0, lc_change_ary)
    lc_change_ary = np.where(lc_change_ary <= 12, 0, lc_change_ary)
    etime(cf, 'Reclassed LC Change to Change Only', st)
    st = time.time()

    # Vectorize change raster
    lc_change_gdf = lch.vectorizeRaster(lc_change_ary, lc_change_meta['transform'])
    etime(cf, 'Vectorized Change', st)
    
    # Read in vector parcels
    parcels_gdf = gpd.read_file(parcels_path, layer='parcels_vectorized')
    parcels_gdf = parcels_gdf[['PID', 'geometry']]
    parcels_gdf['PID'] = parcels_gdf.PID.astype(int)
    etime(cf, 'Read in parcels', st)
    st = time.time()

    # select parcels with change in them
    chg_seg_dict = {row['zone']:row['geometry'] for idx, row in lc_change_gdf.iterrows()}
    pars_change_table = lch.zonal_stats_mp(chg_seg_dict, 'MAJ', raster_parcels_path, [], ['zone', 'PID'], False, False)
    pars_change_table['PID'] = pars_change_table.PID.astype(int)
    etime(cf, 'Selected parcels intersecting change', st)
    st = time.time()

    # Create dict of PIDs and geometries
    parcel_dict = {row['PID']:row['geometry'] for idx, row in parcels_gdf.iterrows()}

    # Run zonal stats
    # tabulate area for ag classes by parcel for NLCD 11
    nlcd_ag_classes = [lch.getNLCD(81), lch.getNLCD(82)] # 81 pasture, 82 crop
    nlcd11df = lch.zonal_stats_mp(parcel_dict, '', nlcd11_path, [81,82], ['PID', 'NLCD11_pas', 'NLCD11_crop'], False, False)
    etime(cf, 'Ran zonal stats majority on NLCD11', st)
    st = time.time()
    lcmap10df = lch.zonal_stats_mp(parcel_dict, 'MAJ', lcmap_11_19_path, list(lch.getLCMAP('ALL')), ['PID', 'LCMAPmaj'], False, False)
    etime(cf, 'Ran zonal stats majority on LCMAP', st)
    st = time.time()
    cdl_dict = lch.getCDL('ALL')
    cdl_cols = [cdl_dict[c] for c in cdl_dict if c != 0]
    cdldf = lch.zonal_stats_mp(parcel_dict, '', cdl13_path, list(cdl_dict.keys()), ['PID']+cdl_cols, False, False)
    etime(cf, 'Ran tabulate area on CDL13', st)
    st = time.time()
    cdldf = cdldf[['PID'] + cdl_cols[0:4]]
    lu2017df = lch.zonal_stats_mp(parcel_dict, 'MAJ', lu_2017_ras_path, [], ['PID', 'ParT2LUmaj'], True, False)
    etime(cf, 'Ran zonal stats majority on T2 LU', st)
    st = time.time()

    # Merge zonal stats dfs on PID to create tab_area_df
    tab_area_df = nlcd11df.copy()
    tab_area_df = tab_area_df.merge(lcmap10df, on=['PID'])
    tab_area_df = tab_area_df.merge(cdldf, on=['PID'])
    tab_area_df = tab_area_df.merge(lu2017df, on=['PID'])
    parcels_gdf.loc[:, 'pid_area'] = parcels_gdf.geometry.area
    tab_area_df = tab_area_df.merge(parcels_gdf[['PID', 'pid_area']], on='PID')

    # delete individual zonal stats df
    del nlcd11df
    # del nlcd16df
    del lcmap10df
    del cdldf
    del lu2017df

    # Create table with PID and TYPE columns, where TYPE is turf, crop or pasture
    tab_area_df = lch.getAgAndTurf(tab_area_df)
    etime(cf, 'Summarized anci to find crop, pas and turf parcels', st)
    st = time.time()

    # Merge tab area data with vectorized change segments
    lc_change_gdf = lc_change_gdf.merge(pars_change_table, on='zone', how='left') # ADD PID column to change segments
    lc_change_gdf = lc_change_gdf.merge(tab_area_df, on='PID', how='left') # add TYPE field to change segments
    del pars_change_table

    # Read change raster xml - convert to dict
    change_df = xml.buildChangeData(lc_change_ras_path+'.xml')
    change_df = xml.defineReclassValues(change_df) #Value is LC Change and NewVal is T1 LC
    lc_change_dict = {}
    try:
        for idx, row in change_df.iterrows():
            desc = row['Description']
            if 'Impervious Structures' in desc:
                desc = desc.replace("Impervious Structures", "Structures")
                print("Updating Impervious Structures: ", desc)
            elif 'Other Impervious' in desc and 'Surfaces' not in desc:
                if 'Surface' in desc:
                    desc = desc.replace("Other Impervious Surface", "Other Impervious Surface")
                    print("Updating Other Impervious Surfaces: ", desc)
                else:
                    desc = desc.replace("Other Impervious", "Other Impervious Surfaces") 
                    print("Updating Other Impervious: ", desc)

            lc_change_dict[desc] = row['Value']
    except:
        print(lc_change_dict)
        sys.exit()
    # Tag change raster with majority LC change class and majority T2 LU class
    chg_tab = lch.zonal_stats_mp(chg_seg_dict, 'MAJ', lc_change_ras_path, list(lc_change_dict.values()), ['zone', 'LC_Chg_Val'], False, True)
    for i in list(set(list(chg_tab['LC_Chg_Val']))):
        if i > 0:
            chg_tab.loc[chg_tab['LC_Chg_Val'] == i, 'LC Change'] = list(change_df[change_df['Value'] == i]['Description'])[0]
    etime(cf, 'Ran zonal stats majority on LC change - change segs', st)
    st = time.time()
    
    lu_tab = lch.zonal_stats_mp(chg_seg_dict, 'MAJ', lu_2017_ras_path, list(lch.get_lu_code('ALL', False).values()), ['zone', 'T2_LU_Val'], True, False)
    for i in list(set(list(lu_tab['T2_LU_Val']))):
        lu_tab.loc[lu_tab['T2_LU_Val'] == i, 'T2 LU'] = lch.get_lu_code(i, True)
    del chg_seg_dict
    etime(cf, 'Ran zonal stats majority on T2 LU - change segs', st)
    st = time.time()

    # Add LC change class and T2 LU class to segments gdf
    lc_change_gdf = lc_change_gdf.merge(chg_tab, on='zone')
    lc_change_gdf = lc_change_gdf.merge(lu_tab, on='zone')
    change_df = change_df.rename(columns={'Description':'LC_Change', 'Value':'LC_Chg_Val'})
    change_df = change_df[['LC_Change', 'LC_Chg_Val']]
    lc_change_gdf = lc_change_gdf.merge(change_df, on='LC_Chg_Val') #add LC change description

    all_t2 = set(list(lc_change_gdf['T2_LU_Val']))
    if -1 in all_t2:
        print('T2_LU_Val contained -1 - other error')

    etime(cf, 'Total Data Prep', st_time)
    st = time.time()

    # Add T1 LU code field - intialized at 0
    lc_change_gdf.loc[:, 'T1_LU_Code'] = 0

    # Run direct method
    lc_change_gdf = runDirect(lc_change_gdf)
    etime(cf, 'Ran direct approach', st)
    st = time.time()
    
    # Run new structure method
    tct_gdf = gpd.read_file(trees_over_gpkg_path, layer='tct_bufs')
    lc_change_gdf = runNewStructure(lc_change_gdf, parcels_gdf, lc_change_ras_path, lc_change_dict, tab_area_df, lu_2017_ras_path, tct_gdf, raster_parcels_path)
    etime(cf, 'Ran new structure approach', st)
    st = time.time()

    # Run context based method
    lc_change_gdf = runContextBase(lc_change_gdf)
    etime(cf, 'Ran context based approach', st)
    st = time.time()

    # Read in psegs for indirect method
    psegs = gpd.read_file(psegs_gpkg_path, layer='psegs_lu')
    psegs = psegs[['SID', 'PSID', 'Class_name', 'lu','geometry']]
    etime(cf, 'Read psegs', st)
    st = time.time()

    # Run indirect method
    tct_gdf = gpd.read_file(trees_over_gpkg_path, layer='tct')
    lc_change_gdf = runIndirect(lc_change_gdf, psegs, lu_2017_ras_path, tct_gdf)
    del tct_gdf
    etime(cf, 'Ran Indirect', st)
    st = time.time()

    # Create LU Change code 
    try:
        lc_change_gdf['T1_LU_Code'] = lc_change_gdf.T1_LU_Code.astype(int)
    except:
        print("Could not convert T1_LU_Code to int ")
        t1_codes = list(set(list(lc_change_gdf['T1_LU_Code'])))
        bad_codes = []
        for code in t1_codes:
            if lch.get_lu_code(code, True) == 'Null':
                bad_codes.append(code)
        # print(bad_codes)
        lc_change_gdf[(lc_change_gdf['T1_LU_Code'].isin(bad_codes))|(lc_change_gdf['T1_LU_Code'].isna())].to_csv(os.path.join(output_path, f"{cf}_bad_t1_code_{lu_type}.csv"), index=False)

    for i in list(set(list(lc_change_gdf['T1_LU_Code']))):
        lc_change_gdf.loc[lc_change_gdf['T1_LU_Code'] == i, 'T1_LU'] = lch.get_lu_code(i, True)

    lc_change_gdf['Acres'] = lc_change_gdf.geometry.area 
    lc_change_gdf['Acres'] = lc_change_gdf['Acres'] / 4047 #m2 to acres
    lc_change_gdf = lc_change_gdf[['zone', 'T1_LU_Code', 'T1_LU', 'T2 LU', 'LC_Change', 'Method', 'TYPE', 'Type_log', 'GRP_TYPE', 'LC_Chg_Val', 'PID', 'Acres', 'geometry']]
    lc_change_gdf.to_file(os.path.join(temp_path, f"{cf}_LU_change_{lu_type}.shp"))

    if 0 in list(lc_change_gdf['T1_LU_Code']) or -1 in list(lc_change_gdf['T1_LU_Code']):
        print("\n***********************************************************************************")
        print("Missing T1 LU for ", len(lc_change_gdf[lc_change_gdf['T1_LU_Code'].isin([0, -1])]), " segments")
        print("Check ", os.path.join(output_path, f"{cf}_missed_segs_{lu_type}.csv"))
        print("***********************************************************************************\n")
        lc_change_gdf[lc_change_gdf['T1_LU_Code'].isin([0, -1])].to_csv(os.path.join(output_path, f"{cf}_missed_segs_{lu_type}.csv"), index=False)

    etime(cf, 'Write vector LU change', st)
    st = time.time()

    # Rasterize T1 LU and mask by LC change
    classes_not_in_change = ['Tree Canopy to Tree Canopy', 'Tree Canopy to Tree Canopy NS']
    sh = (lc_change_ary.shape[1], lc_change_ary.shape[2])
    shapes = [ (row['geometry'], row['T1_LU_Code']) for idx, row in lc_change_gdf[(lc_change_gdf['Method'] == 'New Structure - Parcel')|(lc_change_gdf['LC_Change'].isin(classes_not_in_change))].iterrows()]
    if len(shapes) > 0:
        t1lu_ary = rasterize(shapes, out_shape=sh, fill=0,  transform=lc_change_meta['transform'], all_touched=False)
    else:
        etime(cf, 'No TC to TC or NS Parcel to rasterize', st)
    shapes = [ (row['geometry'], row['T1_LU_Code']) for idx, row in lc_change_gdf[lc_change_gdf['Method'] != 'New Structure - Parcel'].iterrows()]
    if len(shapes) > 0:
        ns_p_ary = rasterize(shapes, out_shape=sh, fill=0,  transform=lc_change_meta['transform'], all_touched=False)
        try:
            t1lu_ary = np.where(lc_change_ary > 0, ns_p_ary, t1lu_ary)
            etime(cf, 'Adding other change to TC to TC and NS Parcel - raster', st)
        except:
            t1lu_ary = ns_p_ary.copy()
            etime(cf, 'No TC to TC or NS Parcel - skipping where', st)
        del ns_p_ary
    del lc_change_ary
    lc_change_meta.update({'nodata':0,
                            'dtype':'uint16'})
    with rio.open(os.path.join(temp_path, f"{cf}_T1_LU_{lu_type}.tif"), 'w', **lc_change_meta, compress="LZW") as dataset:
        try:
            dataset.write(t1lu_ary)
        except:
            dataset.write(t1lu_ary, 1)

    etime(cf, 'Rasterize T1 LU and wrote to TIFF', st)
    st = time.time()
    # Create P6 LU Change
    p6_change.run_p6_rollup_change(cf, lu_type) 
    etime(cf, 'Create Raster LU Change for P6 Classes and Pivot Table', st)

    etime(cf, 'Total LU Change Time', st_time)


    return 0

    # except Exception as e:
    #     etime(cf, f"main exception \n{e}", st_time)
    #     print("********* ", cf, " FAILED **********\n\n")
    #     return -1

