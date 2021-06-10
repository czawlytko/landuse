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


def join_data():
    """
join_data.py
Authors: Jacob Czawlytko and Patrick MacCabe
Purpose: Join required tables to psegs within a GPKG and swap

"""

import geopandas as gpd
import time
import os
# from lu_utilities import transfer
import sys
import luconfig


def etime(folder, cf, note, starttime):
    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time() - starttime
    f = open(f"{folder}/{cf}/etime_log.txt", "a")
    if elapsed > 60:
        f.write(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n\n')
        print(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}')
    else:
        f.write(f'{cf}--{note} runtime - {round(elapsed, 2)} sec\n\n')
        print(f'{cf}--{note} runtime - {round(elapsed, 2)} sec')
    f.close()


def generate_ta_list(cf, folder):

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
    'path': f"{folder}/{cf}/temp/luz_pid_ta.dbf",
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
        'n16_pid': n16_pid
    }

    return dict_dict

