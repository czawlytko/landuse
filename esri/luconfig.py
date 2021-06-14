anci_folder = r"B:/ancillary"
folder = f'B:/landuse/version1'
# folder = f'B:/landuse/rev1/batch_test'
batch_size = 10000
batch_log_Path = f"{folder}/batch_log.txt"


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
        f"TEMP_Parcels.tif",  #used for re-vectorizing
        f"temp_dataprep.gpkg", # (parcels)
        f"",# All of the tabulate area .csvs (named by their value for each SID/PID key)
        f"ps_segs.tif",
        f"ps_parcels.tif"]
