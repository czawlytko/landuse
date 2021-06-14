import os
from pathlib import Path
import time
import argparse
import multiprocessing as mp
import subprocess
import sys

import luconfig

def etime(cf, psegs, note, starttime):
    folder = luconfig.folder

    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    log_dir = Path(f"{folder}/{cf}") # define as pathlib path object, allows pathlib's exists()

    if log_dir.exists() == False:
        Path.mkdir(log_dir)
        print(f'making dir... {log_dir}')
    etime_file = Path(log_dir, "etlog.txt")

    if 'lu' in psegs.columns:  # etime() is used before lu exists
        print(f"----{round((len(psegs[(psegs.lu.notna())]))/len(psegs)*100, 2)}% lu classified")

    f = open(etime_file, "a")
    if elapsed > 60:
        f.write(f'--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n')
        print(f'--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n')
    else:
        f.write(f'--{note} runtime - {round(elapsed, 2)} sec\n')
        print(f'--{note} runtime - {round(elapsed, 2)} sec\n')
    f.close()

def tformat(elapsed_t):
    if elapsed_t > 60:
        print(f'{time.strftime("%H:%M:%S", time.gmtime(elapsed_t))}')
    else:
        return f"{round(elapsed_t, 2)} seconds"

def checkFile(fPath): # used for all anci data
    if not os.path.isfile(fPath):
        print(f"--Input File Path failed checkFile()\n\t{fPath}")
        sys.exit()
    else:
        pass

def county_check(cf,):
    
    for k,v in luconfig.anci_dict.items():
        checkFile(Path(luconfig.anci_folder, v))
    print('Ancillary data check: COMPLETE')

    if cf[5:7] in luconfig.st_dict:
        stabv = luconfig.st_dict[cf[5:7]]
            
    else:
        print(f'ERROR!\n\t{cf} is not an approved state fips code')
        print(st_dict)
        pass

def files_exist(file_list):
    file_exist_flag = True
    for item in file_list:
        if os.path.isfile(item):
            print(f"{item} found")
        else:
            file_exist_flag == False
            exit()
    return file_exist_flag


def transfer_files(source, destination):
    global pid_list
    # check if files exist:
    source, destination = Path(source), Path(destination)

    if destination.exists() == False:
        Path.mkdir(destination)

    # Copy-Item powershell command
    copy_cmdlet = ['powershell', 'Copy-Item', f'"{source}"', '-Destination', f'"{destination}"']
    # execute
    run_process = subprocess.Popen(copy_cmdlet, shell=False, stdin=None, stdout=None, stderr=None)
    print('Process ID (PID): ', run_process.pid)
    pid_list.append(run_process.pid)

def make_ta_dict(cf, anci_folder, folder):

    files = os.listdir(f'{folder}/{cf}/input')
    pattern = f"{cf}_landcover_????_*.tif"
    for entry in files:
        if fnmatch.fnmatch(entry, pattern):
            print(f"Found landcover file: {entry}")
            lc_path = f'{folder}/{cf}/input/{entry}'
            break
    if not arcpy.Exists(lc_path):
        print(f"Failed to open LC: {lc_path}" )
        exit()
    LUZ_values = luconfig.LUZ_values
    
    lc_pid = {
        'name' : "landcover",
        'path': lc_path,
        'zone': 'PID',
        'vals' : [1,2,3,4,5,6,7,8,9,10,11,12]
        }

    c1719_sid = {
        'name' : "CDL 2017-2019",
        'path': f'{anci_folder}/CDL/CDL_2017_2019_4class_maj_1m.tif',
        'zone': 'SID',
        'vals' : [0,1,2,3,4]
        }

    c18_sid = {
        'name' : "CDL 2018",
        'path': f'{anci_folder}/CDL/cdl_2018_4class_maj_1m.tif',
        'zone': 'SID',
        'vals' : [0,1,2,3,4]
        }

    c18_pid = {
        'name' : "CDL 2018",
        'path': f'{anci_folder}/CDL/cdl_2018_4class_maj_1m.tif',
        'zone': 'PID',
        'vals' : [0,1,2,3,4]
        }

    n16_sid = {
        'name' : "NLCD",
        'path': f'{anci_folder}/NLCD/NLCD_2016_pashay_maj_1m.tif',
        'zone': 'SID',
        'vals' : [0,1]
        }

    n16_pid = {
        'name' : "NLCD",
        'path': f'{anci_folder}/NLCD/NLCD_2016_pashay_maj_1m.tif',
        'zone': 'PID',
        'vals' : [0,1]
        }

    luz_sid = {
        'name' : "Local Use or Zoning",
        'path': f'{folder}/{cf}/input/cbp_lu_mask.tif',
        'zone': 'SID',
        'vals' : LUZ_values
        }

    luz_pid = {
        'name' : "Local Use or Zoning",
        'path': f'{folder}/{cf}/input/cbp_lu_mask.tif',
        'zone': 'PID',
        'vals' : LUZ_values
        }

    # list of rasters and the zone units
    dict_dict = {'lc_pid': lc_pid,
                'c1719_sid': c1719_sid,
                'c18_sid': c18_sid,
                'c18_pid': c18_pid,
                'n16_sid': n16_sid,
                'n16_sid': n16_pid,
                'luz_sid': luz_sid,
                'luz_pid': luz_pid
                }

    return dict_dict

def joinData(psegs, remove_columns):

    jd_st = time.time()

    folder = luconfig.folder
    LUZ_values = luconfig.LUZ_values
    if remove_columns:
        # subset to only required columns
        psegs = psegs[['PSID', 'PID', 'SID', 'Class_name', 'geometry']]

    dict_dict = generate_ta_list(cf, folder)

    new_cols = []

    for dname, tadict in dict_dict.items():
        merge_st = time.time()
        tabPath = tadict['path']
        zoneAbv = tadict['zone'][0].lower()
        data_source = dname.split('_')[0]
        zoneID = tadict['zone']

        if os.path.isfile(tabPath):
            tabledf = gpd.read_file(tabPath)

            # rename Value to zone ID
            tabledf = tabledf.rename(columns={'VALUE': zoneID})

            if 'geometry' in tabledf.columns:
                tabledf = tabledf.drop(['geometry'], axis=1)

            # handle LUZ
            if dname in ('luz_pid', 'luz_sid'):
                print(dname, " - group 1 (LUZ only)")

                # Esri has named the no-data column for LUZ as A__ or __A from arcpy.TabulateArea(), unclear why. Shoot me.
                for none_col in ('A__', '__A'):
                    if none_col in tabledf.columns:
                        tabledf = tabledf.rename(columns={none_col: 'no_luz'})

                vcols = []  # to build list of included and accepted value columns
                for col in tabledf.columns:
                    if col in LUZ_values:
                        vcols.append(col)

                # Get max LUZ value
                print(f"kept value columns: {vcols}")
                new_col_name = f'{zoneID[0].lower()}_{dname.split("_")[0]}' # make p_luz or s_luz
                tabledf[new_col_name] = tabledf[vcols].idxmax(axis=1)

                print(tabledf.columns)
                for drop_col in vcols:
                    tabledf = tabledf.drop(drop_col, axis=1)

                print(f'merging {dname} to psegs by {zoneID}')
                psegs = psegs.merge(tabledf, on=zoneID, how='left')
                etime(folder, cf, f"merged {dname}", merge_st)

            if dname in ('s_area', 'p_area'):
                print(dname, " - group 2 (areas)")

                tabledf = tabledf.rename(columns={'Value': zoneID, 'Count': dname})

                print(f'merging {dname} to psegs by {zoneID}')
                print(f'columns: {tabledf.columns}')
                psegs = psegs.merge(tabledf, on=zoneID, how='left')
                etime(folder, cf, f"merged {dname}", merge_st)

            if dname not in ('luz_pid', 'luz_sid', 's_area', 'p_area'):
                print(dname, " - group 3 (everything else)")
                tabledf = tabledf.rename(columns={'VALUE': zoneID})

                for col in tabledf.columns:
                    new_name = col.replace("VALUE_", f'{zoneAbv}_{data_source}_')
                    tabledf = tabledf.rename(columns={col: new_name})

                print(f'merging {dname} to psegs by {zoneID}')
                psegs = psegs.merge(tabledf, on=zoneID, how='left')
                etime(folder, cf, f"merged {dname}", merge_st)

        else:
            print(f"bad file path {tabPath}")
            # pass
            sys.exit()

    print('final columns')
    for fincol in psegs.columns:
        if fincol not in ('Class_name','p_luz','s_luz','geometry'):
            psegs[fincol] = psegs[fincol].fillna(0)
            psegs[fincol] = psegs[fincol].astype('int32')
        print(fincol, psegs[fincol].dtype)

    etime(cf, psegs, "joinData()", jd_st)

    return psegs





