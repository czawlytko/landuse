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

def etime(batch, note, starttime):
    # batch arg can either be "batch" as a string or cf
    folder = luconfig.folder
    elapsed = time.time()-starttime
    scriptName = os.path.basename(sys.argv[0])
    if scriptName.lower() == "main.py":
        etime_file = Path(folder, "batch_log.txt")
    else:
        # batch = cf 
        etime_file = Path(folder, batch, "log.txt")

    f = open(etime_file, "a")
    f.write(f'--{note} runtime: {tformat(elapsed)}\n')
    print(f'--{note} runtime: {tformat(elapsed)}\n----{time.asctime()}')
    f.close()


def lu_etime(cf, psegs, note, starttime):
    folder = luconfig.folder
    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    log_dir = Path(f"{folder}/{cf}") # define as pathlib path object, allows pathlib's exists()
    etime_file = Path(log_dir, "et_log.txt")

    if 'lu' in psegs.columns:  # etime() is used before lu exists
        note = note + f"\n----{round((len(psegs[(psegs.lu.notna())]))/len(psegs)*100, 2)}% lu classified"
    f = open(etime_file, "a")
    f.write(f'--{note} runtime - {tformat(elapsed)}\n')
    print(f'--{note} runtime - {tformat(elapsed)}\n')
    f.close()
  

def tformat(elapsed_t):
    if elapsed_t > 60:
        return(f'{time.strftime("%H:%M:%S", time.gmtime(elapsed_t))}')
    else:
        return f"{round(elapsed_t, 2)} seconds"

def checkFile(fPath): # used for all anci data
    if not os.path.isfile(fPath):
        print(f"--Input File Path failed checkFile()\n\t{fPath}")
        raise TypeError(f"--Input File Path failed checkFile()\n\t{fPath}")
        # sys.exit()
    else:
        pass

def checkCounty(cf):
    outfolder = f"{luconfig.folder}/{cf}/output"
    if not os.path.isdir(outfolder):
        try:
            print("making output folder at {outfolder}")
            os.mkdir(outfolder)
        except:
            print('failed to make output folder... exiting')
            sys.exit()
    else:
        print(f"--outfolder exists: {outfolder}")

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

def bash_command(cmd):
    # run a bash command and wait for process to finish
    p1 = subprocess.Popen(cmd, shell=True, executable='/bin/bash')
    p1.wait()


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
    TA_dict = generate_TA_dict(cf)

    s_area = {
        'name' : "segment area",
        'tabPath' : f'{folder}/{cf}/temp/segtable.dbf',
        'zone' : 'SID',
    }

    p_area = {
        'name' : "parcel area",
        'tabPath' : f'{folder}/{cf}/temp/parcelstable.dbf',
        'zone' : 'PID',
    }

    TA_dict["s_area"] = s_area
    TA_dict["p_area"] = p_area

    new_cols = []
    missing_TA = []


    missingLUZ_p, missingLUZ_s = False, False

    for dname, tadict in TA_dict.items():
        tabPath = tadict['tabPath']
        if not os.path.isfile(tabPath):
            print(f"TA MISSING FILE: {tabPath}")
            if dname != 'luz_pid' and dname != 'luz_sid':
                missing_TA.append(tabPath)
            elif dname == 'luz_pid':
                missingLUZ_p = True
            else:
                missingLUZ_s = True

    if len(missing_TA) > 0:
        print(f'ERROR! Missing TA files! Exiting...\n {missing_TA}')
        raise TypeError(f'ERROR! Missing TA files! Exiting...\n {missing_TA}')
        # sys.exit()

    for dname, tadict in TA_dict.items():
        merge_st = time.time()
        tabPath = tadict['tabPath']
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
                    if col in luconfig.LUZ_values:
                        vcols.append(col)

                if len(vcols) == 0:
                    checkCBP = []
                    for col in tabledf.columns:
                        if 'VALUE' in col:
                            checkCBP.append(col)
                    if len(checkCBP) > 0: # the LUZ mask used VALUE_#, check CBP mask RAT for # to LUZ name relationship
                        cbp_dbf_paths = [f'{folder}/{cf}/input/cbp_lu_mask.tif.vat.dbf',
                        f'{folder}/{cf}/input/cbp_lu_mask.tif.dbf',
                        f'{folder}/{cf}/input/cbp_lu_mask.vat.dbf',
                        f'{folder}/{cf}/input/cbp_lu_mask.dbf'
                        ]
                        cbp_dbf_path=r''
                        for p in cbp_dbf_paths:
                            if os.path.isfile(p):
                                cbp_dbf_path = p
                        print('mask file path:', cbp_dbf_path)

                        if os.path.isfile(cbp_dbf_path):
                            cbp_rat = gpd.read_file(cbp_dbf_path)
                            if 'CBP_mask' not in cbp_rat.columns:
                                for col in cbp_rat.columns:
                                    if 'mask' in col.lower():
                                        cbp_rat = cbp_rat.rename(columns={col:'CBP_mask'})
                            if 'CBP_mask' not in cbp_rat.columns:
                                t_cols = cbp_rat.columns
                                rem_cols = ['Value', 'Count', 'geometry']
                                t_cols = [x for x in t_cols if x not in rem_cols]
                                if len(t_cols) == 1:
                                    cbp_rat = cbp_rat.rename(columns={t_cols[0]:'CBP_mask'})
                            if 'CBP_mask' not in cbp_rat.columns:
                                print("LUZ table has VALUE_# cols and CBP mask does not have mask column in vat")
                                raise TypeError("LUZ table has VALUE_# cols and CBP mask does not have mask column in vat")
                                # sys.exit(0)
                            for old_name in checkCBP:
                                val = int(old_name.split('_')[-1]) # get the #
                                if val in list(cbp_rat['Value']):
                                    lu_name = list(cbp_rat[cbp_rat['Value']==val]['CBP_mask'])[0]
                                    if lu_name == None:
                                        lu_name = 'no_luz'
                                    else:
                                        lu_name = lu_name.upper()
                                    if lu_name in luconfig.LUZ_values:
                                        if lu_name in vcols: # name already exists - check RAT?
                                            raise TypeError(f'LUZ table error: CBP mask RAT has duplicate name {lu_name}')
                                        vcols.append(lu_name)
                                        tabledf = tabledf.rename(columns={old_name:lu_name})
                                        print(f"renamed {old_name} to {lu_name} using CBP mask dbf")

                # Get max LUZ value
                print(f"kept value columns: {vcols}")
                new_col_name = f'{zoneID[0].lower()}_{dname.split("_")[0]}' # make p_luz or s_luz
                tabledf[new_col_name] = tabledf[vcols].idxmax(axis=1)
                for drop_col in vcols:
                    tabledf = tabledf.drop(drop_col, axis=1)

                print(f'merging {dname} to psegs by {zoneID}')
                psegs = psegs.merge(tabledf, on=zoneID, how='left')

            if dname in ('s_area', 'p_area'):
                print(dname, " - group 2 (areas)")
                if 'PID' in list(tabledf) and zoneID == 'PID':
                    tabledf = tabledf[['Value', 'Count']]
                elif 'SID' in list(tabledf) and zoneID == 'SID':
                    tabledf = tabledf[['Value', 'Count']]

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
            if dname == 'luz_sid' and missingLUZ_s:
                psegs.loc[:, 's_luz'] = 'no_luz'
            elif dname == 'luz_pid' and missingLUZ_p:
                psegs.loc[:, 'p_luz'] = 'no_luz'
            else:
                print(f"bad file path {tabPath}")
                raise TypeError(f"bad file path {tabPath}")
                # pass
                # sys.exit()

    print('final columns')
    try:
        for fincol in psegs.columns:
            if fincol not in ('Class_name','p_luz','s_luz','geometry'):
                psegs[fincol] = psegs[fincol].fillna(0)
                psegs[fincol] = psegs[fincol].astype('int32')
            print(fincol, psegs[fincol].dtype)
    except:
        print(psegs.dtypes)

    etime(cf, "joinData()", jd_st)

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

def memCheck():
    print(psutil.virtual_memory())

def generate_TA_dict(cf):

    anci_folder = luconfig.anci_folder
    folder = luconfig.folder
        
    lc_path = rasFinder(f"{folder}/{cf}/input", f"{cf}_landcover_*.tif")
    lc_pid = {
        'name' : "landcover",
        'colname' : 'p_lc_',
        'path': lc_path,
        'tabPath' : f'{folder}/{cf}/temp/lc_pid_ta.dbf',
        'zone' : 'PID',
        'vals' : [1,2,3,4,5,6,7,8,9,10,11,12]
        }

    c1719_sid = {
        'name' : "CDL 2017-2019",
        'colname' : 's_c18_',
        'path': f'{anci_folder}/CDL/CDL_2017_2019_4class_maj_1m.tif',
        'tabPath' : f'{folder}/{cf}/temp/c1719_sid_ta.dbf',
        'zone': 'SID',
        'vals' : [0,1,2,3,4]
        }
    c18_sid = {
        'name' : "CDL 2018",
        'colname' : 's_c18_',
        'path': f'{anci_folder}/CDL/cdl_2018_4class_maj_1m.tif',
        'tabPath' : f'{folder}/{cf}/temp/c18_sid_ta.dbf',
        'zone': 'SID',
        'vals' : [0,1,2,3,4]
        }
    c18_pid = {
        'name' : "CDL 2018",
        'colname' : 'p_c18_',
        'path': f'{anci_folder}/CDL/cdl_2018_4class_maj_1m.tif',
        'tabPath' : f'{folder}/{cf}/temp/c18_pid_ta.dbf',
        'zone': 'PID',
        'vals' : [0,1,2,3,4]
        }
    n16_sid = {
        'name' : "NLCD",
        'colname' : 's_n16_',
        'path': f'{anci_folder}/NLCD/NLCD_2016_pashay_maj_1m.tif',
        'tabPath' : f'{folder}/{cf}/temp/n16_sid_ta.dbf',
        'zone': 'SID',
        'vals' : [0,1]
        }
    n16_pid = {
        'name' : "NLCD",
        'colname' : 'p_n16_',
        'path': f'{anci_folder}/NLCD/NLCD_2016_pashay_maj_1m.tif',
        'tabPath' : f'{folder}/{cf}/temp/n16_pid_ta.dbf',
        'zone': 'PID',
        'vals' : [0,1]
        }
    luz_sid = {
        'name' : "Local Use or Zoning",
        'colname' : 's_luz_',
        'path': f'{folder}/{cf}/input/cbp_lu_mask.tif',
        'tabPath' : f'{folder}/{cf}/temp/luz_sid_ta.dbf',
        'zone': 'SID',
        'vals' : luconfig.LUZ_values
        }
    luz_pid = {
        'name' : "Local Use or Zoning",
        'colname' : 'p_luz_',
        'path': f'{folder}/{cf}/input/cbp_lu_mask.tif',
        'tabPath' : f'{folder}/{cf}/temp/luz_pid_ta.dbf',
        'zone': 'PID',
        'vals' : luconfig.LUZ_values
        }

    # list of rasters and the zone units
    TA_dict = {
        # 'p_area': p_area,
        # 's_area': s_area,
        'luz_pid': luz_pid,
        'luz_sid': luz_sid,
        'lc_pid': lc_pid,
        'c1719_sid': c1719_sid,
        'c18_sid': c18_sid,
        'c18_pid': c18_pid,
        'n16_sid': n16_sid,
        # 'n16_pid': n16_pid
    }

    return TA_dict
