anci_folder = f'/home/azureuser/azData/ancillary-v1/'
folder = f'/home/azureuser/azData/version1/'
batch_size = 10000 # used in landuse_rev2 and TC_LU_Submodule_v1
batch_log_Path = f"{folder}/batch_log.txt"
crosswalk_csv = r'C:/scripts/landuse/lu_change/P6RollUpLUCrosswalk.csv' 
lu_change_csv = r'C:/scripts/landuse/lu_change/p6lu_change_all_classes.csv'
fart = r'C:/scripts/landuse/lu_change/P6RollUpLUCrosswalk.csv'
TC_Tile_Min = 150000 # thresholds for minimum number of psegs required to generate tiles
TC_Tile_Max = 500000
TC_CPUS = 15 # number of cores to be used to help divide total core count and balance processes.

dest  = f""
test = True

anci_dict = {
    'landfillPath' : r"20172018_Landfills/CBW_digitized_landfills_2017_2018_20210517.shp", # updated 4/26/21
    'solarPath' : r"solarfields/CBW_solarAI_20172018.shp", # updated 4/26/21
    'minePath' : r'mines/CBW_digitized_extractive_2017_2018_20210517.shp', # updated 4/27/21
    'PAtimberPath' : r"timber_harvest/PA/20190312_TimbersaleRemovals_projected.shp",
    'MDtimberPath' : r"timber_harvest/MD/timberharvestdata/cf_stands_harvest_2013_projected.shp",
    'transPath' : r'transmission_lines/transmission_lines_15mbuff.shp',
    'herePath' : r'HERE/HERE_turf_aeac.shp',
    'UACPath' : r"census/tl_2018_uac10_baycounties20m_AEAC.shp", # updated 4/27/21
    'timHarRasPath' : r'lcmap/10m_timbHarv/Primary_2y_10m.tif', # lcmap primary patterns with timber harvest class
    'sucAgeRasPath' : r'lcmap/10m_timbHarv/Sucess_Age_10m.tif', # lcmp succession age
}


dp_file_list = [
        f"psegs.gpkg",
        f"segments.gpkg",
        f"TEMP_Parcels.tif",  # used for re-vectorizing
        f"temp_dataprep.gpkg", # (parcels)
        f"",# All of the tabulate area .csvs (named by their value for each SID/PID key)
        f"ps_segs.tif",
        f"ps_parcels.tif"]

LUZ_values = ['AG_GEN', 'BAR', 'CAFO', 'CATT', 'CENT', 'CONS', 'CROP', 'DEC', 'EVE', 'EXT', 'FALL', 'NAT', 'OV', 'PAS', 'POUL', 'SUS', 'TG', 'TIM', 'WAT', 'WET', 'WET_NT', 'WET_T', 'no_luz']

lu_code_dict = {
    'Roads' : 2110,
    'Buildings' : 2120, 
    'Emergent Wetlands' : 5000,
    'Tree Canopy' : 3100, 
    'Other Impervious' : 2130,
    'Other Impervious Surface' : 2130, # "Surfaces" vs "Surface"
    'Other Impervious Surfaces' : 2130,
    'Water':1000,
    'Estuary (tidal)' : 1110,
    'Lake/Pond' : 1120, # 
    'Open Channel' : 1211,
    'Tree Canopy over Channel' : 1212,
    'Culverted/Buried Channel' : 1213,
    'Open Ditch' : 1221,
    'Tree Canopy over Ditch' : 1222,
    'Culverted/Buried Ditch' : 1223,
    'Tree Canopy over Roads' : 2141,
    'Tree Canopy over Structures' : 2142,
    'Tree Canopy over Other Impervious' : 2143,
    'Turf Herbaceous' : 2210, #rev1
    'Bare Developed' : 2220,
    'Suspended Succession Barren':2231,
    'Suspended Succession Herbaceous':2232,
    'Suspended Succession Scrub/Shrub':2233, # rev2
    'Tree Canopy over Turf' : 2240,
    'Forest' : 3000,
    'Forest Forest' : 3100,
    'Tree Canopy in Agriculture' : 3200,
    'Harvested Forest Barren' : 3310,
    'Harvested Forest Herbaceous' : 3320,
    'Natural Succession Barren' : 3410,
    'Natural Succession Herbaceous' : 3420,
    'Natural Succession Scrub/Shrub' :3430,
    'Cropland Barren' : 4111,
    'Cropland Herbaceous' : 4112,
    'Pasture/Hay Barren (Former Fallow)' : 4121,
    'Pasture/Hay Herbaceous (Former Fallow)' : 4122,
    'Pasture/Hay Scrub/Shrub' : 4123,
    'Orchard/Vineyard Barren' : 4131,
    'Orchard/Vineyard Herbaceous' : 4132,
    'Orchard/Vineyard Scrub/Shrub' : 4133,
    'Pasture/Hay Barren' : 4141,
    'Pasture/Hay Herbaceous' : 4142,
    'Idle/Fallow Scrub/Shrub' : 4143, # Class does not exist in v1
    'Solar Field Impervious' : 4210,
    'Solar Field Barren' : 4221,
    'Solar Field Herbaceous' : 4222,
    'Solar Field Scrub/Shrub' : 4223,
    'Extractive Barren' : 4310,
    'Extractive Other Impervious' : 4320,
    'Wetland':5000,
    'Tidal Wetlands Barren' : 5101, # rev2
    'Tidal Wetlands Herbaceous' : 5102, # rev2
    'Tidal Wetlands Scrub/Shrub' : 5103, # rev2
    'Tidal Wetlands Tree Canopy' : 5104, # rev2
    'Tidal Wetlands Forest' : 5105, # rev2
    'Riverine (Non-Tidal) Wetlands Barren':5201,
    'Riverine (Non-Tidal) Wetlands Herbaceous':5202,
    'Riverine (Non-Tidal) Wetlands Scrub/Shrub':5203,
    'Riverine (Non-Tidal) Wetlands Tree Canopy':5204,
    'Riverine (Non-Tidal) Wetlands Forest':5205,
    'Headwater Barren' : 5211,
    'Headwater Herbaceous' : 5212,
    'Headwater Scrub/Shrub' : 5213,
    'Headwater Tree Canopy' : 5214,
    'Headwater Forest' : 5215,
    'Floodplain Barren' : 5221,
    'Floodplain Herbaceous' : 5222,
    'Floodplain Scrub/Shrub' : 5223,
    'Floodplain Tree Canopy' : 5224,
    'Floodplain Forest' : 5225,
    'Terrene/Isolated Wetlands Barren' : 5301,
    'Terrene/Isolated Wetlands Herbaceous' : 5302,
    'Terrene/Isolated Wetlands Scrub/Shrub' : 5303,
    'Terrene/Isolated Wetlands Tree Canopy' : 5304,
    'Terrene/Isolated Wetlands Forest' : 5305,
    'Bare Shore' : 5400,
    }


# dictionary for fixing name orders and punctuations

name_dict = {
    "Low Vegetation" : "Herbaceous",
    "Structures" : "Buildings",
    "Developed Barren" : "Bare Developed",
    r"Scrub\\Shrub" : "Scrub/Shrub",
    "Other Impervious Surfaces" : "Other Impervious Surface",
    "Timber Harvest" : "Harvested Forest",
    "Harvested Forest Scrub/Shrub" :"Natural Succession Scrub/Shrub",
    r"Harvested Forest Scrub\\Shrub" : "Natural Succession Scrub/Shrub",
    "Shore Barren" : "Bare Shore",
    "Pasture" : "Pasture/Hay",
    "Orchard Vineyard" : "Orchard/Vineyard",
    'Solar Other Impervious' : 'Solar Field Impervious',
    'Solar Barren' : 'Solar Field Barren',
    'Solar Pervious Herbaceous' : 'Solar Field Herbaceous',
    'Solar Herbaceous' : 'Solar Field Herbaceous',
    'Solar Scrub/Shrub' : 'Solar Field Scrub/Shrub',
    }

st_dict = {
        '10': 'DE',
        '11': 'DC',
        '24': 'MD',
        '36': 'NY',
        '42': 'PA',
        '51': 'VA',
        '54': 'WV'
        }

forgivemeforhavingfun = """
    ,--,                                                                                               
    ,---.'|                             ,--.                                                              
    |   | :       ,---,               ,--.'|     ,---,                              .--.--.        ,---,. 
    :   : |      '  .' \          ,--,:  : |   .'  .' `\                    ,--,   /  /    '.    ,'  .' | 
    |   ' :     /  ;    '.     ,`--.'`|  ' : ,---.'     \                 ,'_ /|  |  :  /`. /  ,---.'   | 
    ;   ; '    :  :       \    |   :  :  | | |   |  .`\  |           .--. |  | :  ;  |  |--`   |   |   .' 
    '   | |__  :  |   /\   \   :   |   \ | : :   : |  '  |         ,'_ /| :  . |  |  :  ;_     :   :  |-, 
    |   | :.'| |  :  ' ;.   :  |   : '  '; | |   ' '  ;  :         |  ' | |  . .   \  \    `.  :   |  ;/| 
    '   :    ; |  |  ;/  \   \ '   ' ;.    ; '   | ;  .  |         |  | ' |  | |    `----.   \ |   :   .' 
    |   |  ./  '  :  | \  \ ,' |   | | \   | |   | :  |  '         :  | | :  ' ;    __ \  \  | |   |  |-, 
    ;   : ;    |  |  '  '--'   '   : |  ; .' '   : | /  ;          |  ; ' |  | '   /  /`--'  / '   :  ;/| 
    |   ,/     |  :  :         |   | '`--'   |   | '` ,/           :  | : ;  ; |  '--'.     /  |   |    \ 
    '---'      |  | ,'         '   : |       ;   :  .'             '  :  `--'   \   `--'---'   |   :   .' 
                `--''           ;   |.'       |   ,.'               :  ,      .-./              |   | ,'   
                                '---'         '---'                  `--`----'                  `----'     """



