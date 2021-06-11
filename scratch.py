import os
from pathlib import Path
import time
import argparse
import multiprocessing as mp
import subprocess
import sys

import luconfig

def etime(cf, note, starttime):
    folder = luconfig.folder

    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    log_dir = Path(f"{folder}/{cf}") # define as pathlib path object, allows pathlib's exists()

    if log_dir.exists() == False:
        Path.mkdir(log_dir)
        print(f'making dir... {log_dir}')
    etime_file = Path(log_dir, "etlog.txt")

    f = open(etime_file, "a")
    if elapsed > 60:
        f.write(f'--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n')
        print(f'--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n')
    else:
        f.write(f'--{note} runtime - {round(elapsed, 2)} sec\n')
        print(f'--{note} runtime - {round(elapsed, 2)} sec\n')
    f.close()

def lu_etime(cf, psegs, note, starttime):
    # print text and write to log file 
    # includes percent of psegs classified and time in HMS or Seconds if time < 60 sec

    folder = luconfig.folder
    elapsed = time.time()-starttime
    log_dir = Path(f"{folder}/{cf}") # define as pathlib path object, allows pathlib's exists()

    if not log_dir.exists():
        Path.mkdir(log_dir)
        print(f'making dir... {log_dir}')

    etime_file = Path(log_dir, "etlog.txt")
    pct = round((len(psegs[(psegs.lu.notna())]))/len(psegs)*100, 2)
    if 'lu' in psegs.columns:  # etime() is used before lu exists
        print(f"----{pct}% lu classified")

    f = open(etime_file, "a")
    if elapsed > 60:
        logText = f'--{note} {pct} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n'
    else:
        logText = f'--{note} {pct} runtime - {round(elapsed, 2)} sec\n'

    f.write()
    print(f'--{note} {pct} runtime - {round(elapsed, 2)} sec\n')

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

def joinData(psegs, clean_columns=True):

    jd_st = time.time()


    folder = luconfig.folder
    LUZ_values = luconfig.LUZ_values
    if clean_columns:
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


name_dict = {
    "Low Vegetation" : "Herbaceous",
    "Developed Barren" : "Bare Developed",
    r"Scrub\\Shrub" : "Scrub/Shrub",
    "Other Impervious Surfaces" : "Other Impervious Surface",
    "Timber Harvest" : "Harvested Forest",
    "Shore Barren" : "Bare Shore",
    "Pasture" : "Pasture/Hay",
    "Orchard Vineyard" : "Orchard/Vineyard",
    'Solar Other Impervious' : 'Solar Field Impervious',
    'Solar Barren' : 'Solar Field Barren',
    'Solar Herbaceous' : 'Solar Field Herbaceous',
    'Solar Scrub/Shrub' : 'Solar Field Scrub/Shrub',
    }