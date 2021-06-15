import os
from pathlib import Path
import time
import argparse
import multiprocessing as mp
import subprocess
import sys
import fnmatch
import geopandas as gpd

import luconfig

def etime(cf, psegs, note, starttime):
    folder = luconfig.folder

    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    log_dir = Path(f"{folder}/{cf}") # define as pathlib path object, allows pathlib's exists()
    etime_file = Path(log_dir, "etlog.txt")

    # if 'lu' in psegs.columns:  # etime() is used before lu exists
    #     print(f"----{round((len(psegs[(psegs.lu.notna())]))/len(psegs)*100, 2)}% lu classified")

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
    print('--Ancillary data check: COMPLETE')

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

def joinData(cf, psegs, remove_columns):

    jd_st = time.time()

    folder = luconfig.folder
    LUZ_values = luconfig.LUZ_values
    if remove_columns:
        print('removing columns')
        # subset to only required columns
        psegs = psegs[['PSID', 'PID', 'SID', 'Class_name', 'geometry']]

    lc_pid = {
        'path': f"{folder}/{cf}/temp/lc_pid_ta.dbf",
        'zone': 'PID'
    }

    c1719_sid = {
        'path': f"{folder}/{cf}/temp/c1719_sid_ta.dbf",
        'zone': 'SID'
    }

    c18_sid = {
        'path': f"{folder}/{cf}/temp/c18_sid_ta.dbf",
        'zone': 'SID'
    }

    c18_pid = {
        'path': f"{folder}/{cf}/temp/c18_pid_ta.dbf",
        'zone': 'PID'
    }

    n16_sid = {
        'path': f"{folder}/{cf}/temp/n16_sid_ta.dbf",
        'zone': 'SID'
    }
    
    n16_pid = {
    'path': f"{folder}/{cf}/temp/n16_pid_ta.dbf",
    'zone': 'PID'
    }

    luz_sid = {
        'path': f"{folder}/{cf}/temp/luz_sid_ta.dbf",
        'zone': 'SID'
    }

    luz_pid = {
        'path': f"{folder}/{cf}/temp/luz_pid_ta.dbf",
        'zone': 'PID'
    }

    p_area = {
        'path': f"{folder}/{cf}/temp/parcelstable.dbf",
        'zone': 'PID'
    }

    s_area = {
        'path': f"{folder}/{cf}/temp/segtable.dbf",
        'zone': 'SID'
    }

    # list of rasters and the zone units
    dict_dict = {
        'p_area': p_area,
        's_area': s_area,
        'luz_pid': luz_pid,
        'luz_sid': luz_sid,
        'lc_pid': lc_pid,
        'c1719_sid': c1719_sid,
        'c18_sid': c18_sid,
        'c18_pid': c18_pid,
        'n16_sid': n16_sid,
        # 'n16_pid': n16_pid
    }

    new_cols = []
    missing_TA = []
    for dname, tadict in dict_dict.items():
        tabPath = tadict['path']
        if not os.path.isfile(tabPath):
            missing_TA.append(tabPath)
            print(f"TA MISSING FILE: {tabPath}")
    if len(missing_TA) > 0:
        print(f'ERROR! Missing TA files! Exiting...\n {missing_TA}')

        sys.exit()

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

                # Esri has named the no-data column for LUZ as A__ or __A from esri's TabulateArea(), unclear why. Shoot me.
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

            if dname in ('s_area', 'p_area'):
                print(dname, " - group 2 (areas)")

                tabledf = tabledf.rename(columns={'Value': zoneID, 'Count': dname})

                print(f'merging {dname} to psegs by {zoneID}')
                print(f'columns: {tabledf.columns}')
                psegs = psegs.merge(tabledf, on=zoneID, how='left')


            if dname not in ('luz_pid', 'luz_sid', 's_area', 'p_area'):
                print(dname, " - group 3 (everything else)")
                tabledf = tabledf.rename(columns={'VALUE': zoneID})

                for col in tabledf.columns:
                    new_name = col.replace("VALUE_", f'{zoneAbv}_{data_source}_')
                    tabledf = tabledf.rename(columns={col: new_name})

                print(f'merging {dname} to psegs by {zoneID}')
                psegs = psegs.merge(tabledf, on=zoneID, how='left')


        else:
            print(f"bad file path {tabPath}")
            # pass
            sys.exit()

    print('final columns')
    try:
        for fincol in psegs.columns:
            if fincol not in ('Class_name','p_luz','s_luz','geometry'):
                psegs[fincol] = psegs[fincol].fillna(0)
                psegs[fincol] = psegs[fincol].astype('int32')
            print(fincol, psegs[fincol].dtype)
    except:
        print(psegs.dtypes)

    etime(cf, psegs, "joinData()", jd_st)

    return psegs

def rasFinder(dir, pattern):
    files = os.listdir(dir)
    for entry in files:
        if fnmatch.fnmatch(entry, pattern):
            print(f"Found raster: {entry}")
            foundRas = f'{dir}/{entry}'
            break

    if not os.path.exists(foundRas):
        print(f"ERROR! Failed to open foundRas: {foundRas}\n--dir: {dir}\n--pattern: {pattern}" )
        exit()

    return foundRas



