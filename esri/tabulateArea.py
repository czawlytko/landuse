import arcpy
from arcpy import env
from arcpy.sa import *
import time
import os
import fnmatch
import argparse

import luconfig

def etime(cf, note, starttime, folder):
    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    f = open(f"{folder}/{cf}/ta_log.txt", "a")
    if elapsed > 60:
        f.write(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n\n')
        print(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}')
    else:
        f.write(f'{cf}--{note} runtime - {round(elapsed,2)} sec\n\n')
        print(f'{cf}--{note} runtime - {round(elapsed,2)} sec')
    f.close()

#TABULATE BY RASTER
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


    lc_pid = {
        'path': lc_path,
        'zone': 'PID'
        }

    c1719_sid = {
        'path': f'{anci_folder}/CDL/CDL_2017_2019_4class_maj_1m.tif',
        'zone': 'SID'
        }

    c18_sid = {
        'path': f'{anci_folder}/CDL/cdl_2018_4class_maj_1m.tif',
        'zone': 'SID'
        }

    c18_pid = {
        'path': f'{anci_folder}/CDL/cdl_2018_4class_maj_1m.tif',
        'zone': 'PID'
        }

    n16_sid = {
        'path': f'{anci_folder}/NLCD/NLCD_2016_pashay_maj_1m.tif',
        'zone': 'SID'
        }

    n16_pid = {
        'path': f'{anci_folder}/NLCD/NLCD_2016_pashay_maj_1m.tif',
        'zone': 'PID'
        }

    luz_sid = {
        'path': f'{folder}/{cf}/input/cbp_lu_mask.tif',
        'zone': 'SID'
        }

    luz_pid = {
        'path': f'{folder}/{cf}/input/cbp_lu_mask.tif',
        'zone': 'PID'
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
       
def convertRATs(folder, cf):
    print(f'Starting RAT conversion - {cf}')
    st_time = time.time()
    prast = f"{folder}/{cf}/temp/temp_dataprep.gdb/parcels_rasterized"
    srast = f"{folder}/{cf}/input/dataprep.gdb/segs_rasterized"
    outpath = f"{folder}/{cf}/temp"
    poutname = "parcelstable.dbf"
    soutname = "segtable.dbf"
    if not arcpy.Exists(f"{folder}/{cf}/temp/segtable.dbf"):
        segt_time = time.time()
        arcpy.TableToTable_conversion(srast,outpath,soutname)
        etime(cf,"Segment Table Saved",segt_time,folder)
    else:
        print('segtable.dbf already exists')
    if not arcpy.Exists(f"{folder}/{cf}/temp/parcelstable.dbf"):
        parct_time = time.time()
        arcpy.TableToTable_conversion(prast,outpath,poutname)
        etime(cf,"Parcel Table Saved",parct_time,folder)
    else:
        print('parcelstable.dbf already exists')
    etime(cf,"RAT to dbf",st_time,folder)

############################################################################

def runTA(cf, folder, anci_folder):

    # folder = luconfig.folder # r"M:/projects/landuse/V1"
    # anci_folder = luconfig.anci_folder #r"B:/ancillary"
    cf_st = time.time()
    dict_dict = make_ta_dict(cf, anci_folder, folder)
    for dname, rdict in dict_dict.items():
        ta_st = time.time()
        print(f'Starting {cf} - {dname} - {time.asctime()}')
        valRas = rdict['path']
        zoneID = rdict['zone']

        if zoneID == 'SID':
            zoneRas = f"{folder}/{cf}/input/dataprep.gdb/segs_rasterized"

        if zoneID == 'PID':
            zoneRas = f"{folder}/{cf}/temp/temp_dataprep.gdb/parcels_rasterized"
        
        outTab = f"{folder}/{cf}/temp/{dname}_ta.dbf"
        if not arcpy.Exists(outTab):
            # if tabulating LUZ, use the "CBP_mask" field instead of Value (We want the string values)
            # check if the "CBP_mask field exists, there are inconsistent RATs
            if "cbp_lu_mask.tif" in valRas:
                luz_fields = arcpy.ListFields(valRas)
                if "CBP_mask" in luz_fields:
                    rasField = 'CBP_mask'
                    print('good')
                else:
                    for field in luz_fields:
                        if 'mask' in field.name.lower():
                            rasField = field.name
                            print("swapping luz field name", field.name)
            else:
                rasField = "Value"

            print("\t", valRas)
            print("\t", rasField)
            print("\t", zoneRas)
            print("\t", zoneID)
            print("\t", outTab)

            try:
                print('tabulating')
                TabulateArea(zoneRas, "Value", valRas, rasField, outTab)
            except Exception:
                e = sys.exc_info()[1]
                print(e.args[0])

            etime(cf, f"{dname} tabulated", ta_st, folder)
        else:
           print(f'{outTab} already exists') 

    convertRATs(folder, cf)

    etime(cf, f"{cf} tabulations complete", cf_st, folder)



parser = argparse.ArgumentParser(description='Integrated Land Use production arg parser')
parser.add_argument('-cfs', nargs='+', help='list co_fips (cf) ', required=True)
args = parser.parse_args()
cflist = list(args.cfs)

folder = luconfig.folder # r"M:/projects/landuse/V1"
anci_folder = luconfig.anci_folder #r"B:/ancillary"

for cf in cflist:

    runTA(cf, folder, anci_folder)

    # try:
    #     runTA(cf, folder, anci_folder)
    # except:
    #     print(f'{cf} failed')