import argparse
import sys
import arcpy
from arcpy.sa import *
import multiprocessing as mp
import time
import os
import fnmatch
arcpy.CheckOutExtension("Spatial")

def etime(cf, note, starttime):
    # print text and elapsed time in HMS or Seconds if time < 60 sec
    elapsed = time.time()-starttime
    f = open(f"{project_folder}/{cf}/etime_log.txt", "a")
    if elapsed > 60:
        f.write(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}\n\n')
        print(f'{cf}--{note} runtime - {time.strftime("%H:%M:%S", time.gmtime(elapsed))}')
    else:
        f.write(f'{cf}--{note} runtime - {round(elapsed,2)} sec\n\n')
        print(f'{cf}--{note} runtime - {round(elapsed,2)} sec')
    f.close()

def calculate_max(file, field, query_is_not):
    query = f"{field} {query_is_not}"
    return arcpy.da.SearchCursor(file, [field], query, sql_clause=(None, "ORDER BY {} DESC".format(field))).next()[0]

def list_fields(featureclass):
    return [f for f in arcpy.ListFields(featureclass)]

def create_seg_ids(segs):
    fields = [f.name for f in arcpy.ListFields(segs)]
    if not 'SID' in fields:
        if 'Id' in fields:
            # rename uid to SID
            arcpy.AlterField_management(segs, 'Id', 'SID', 'SID') #Id was uid, 
            # calculate new SID values
            count = 0
            with arcpy.da.UpdateCursor(segs, ['SID']) as cursor:
                for row in cursor:
                    count += 1
                    row[0] = count
                    cursor.updateRow(row)
        else:
            print("ERROR! No 'Id' field in fields to conver to 'SID'")
    else:
        print('SID already exists, skip creat_seg_ids')
        return
def create_parcel_ids(parcels):
    fields = [f.name for f in list_fields(parcels)]
    # delete unnecessary fields
    for f in fields:
        if f != 'PID':
            try:
                arcpy.DeleteField_management(parcels, f)
            except arcpy.ExecuteError:
                print('cannot delete: ', f)
    if not 'PID' in fields:
        # add PID
        arcpy.AddField_management(parcels, 'PID', "LONG")
        # calculate new PID values
        count = 0
        with arcpy.da.UpdateCursor(parcels, ['PID']) as cursor:
            for row in cursor:
                count += 1
                row[0] = count
                cursor.updateRow(row)
    else:
        return
def multi_2_single(in_feat, out_feat):
    # in_feat, out_feat = args
    if not arcpy.Exists(out_feat):
        arcpy.MultipartToSinglepart_management(in_feat, out_feat)
def clean_and_align_segs_and_parcels(ptype, polygon, field, input_path, snap_raster):
#     ptype, polygon, field, input_path, snap_raster = args
    clean_file = f'{input_path}/{ptype}_vectorized'
    if not arcpy.Exists(clean_file):
        # rasterize
        raster = f'{input_path}/{ptype}_rasterized'
        if not arcpy.Exists(raster):
            arcpy.env.snapRaster = snap_raster
            arcpy.conversion.PolygonToRaster(polygon, field, raster, "CELL_CENTER", cellsize=1)
            print(f'{ptype} rasterized')
        if ptype == 'segs':
            multipart = "MULTIPLE_OUTER_PART"
            vector = f'{input_path}/{ptype}_vectorized'
            new_id = 'SID'
        else:
            multipart = "SINGLE_OUTER_PART"
            vector = f'{input_path}/{ptype}_vectorized_temp'
            new_id = 'PID'
        # vectorize
        arcpy.conversion.RasterToPolygon(raster, vector, simplify="NO_SIMPLIFY", create_multipart_features=multipart)
        arcpy.management.AlterField(vector, "gridcode", new_id, new_id)
        print(f'{ptype} raster vectorized')
        if ptype == 'parcels':
            dissolved = f'{input_path}/{ptype}_vectorized'
            arcpy.management.Dissolve(vector, dissolved, new_id, None, "MULTI_PART", "DISSOLVE_LINES")
            print('vectorized parcels dissolved')
        arcpy.management.Delete(f'{input_path}/{ptype}_vectorized_temp')
        # arcpy.management.Delete(f'memory/{ptype}_vectorized')
    else:
        return
def lc_zonalstats_by_segs(cf, segs, zone_field, lc_raster, gdb):

    arcpy.env.parallelProcessingFactor = "90%"
    lc_zs_table = f'{gdb}/lc_zs_table'
    if not arcpy.Exists(lc_zs_table):
        print('start zs')
        zs_start = time.time()
        arcpy.sa.ZonalStatisticsAsTable(segs, zone_field, lc_raster, lc_zs_table, ignore_nodata="DATA",
                                                statistics_type="MAJORITY")
        etime(cf, 'lc zonal stats',  zs_start)

    segs_x_lc = f'{gdb}/segs_w_lc_stats' # output
    # join lc zonal stats to segs_vectorized
    if not arcpy.Exists(segs_x_lc):
        # Execute Join Features
        start_join = time.time()
        arcpy.JoinField_management(segs, zone_field, lc_zs_table, zone_field, ["MAJORITY"])

        arcpy.FeatureClassToFeatureClass_conversion(segs, gdb, "segs_w_lc_stats")
        etime(cf, "joinField_management and FC2FC", start_join)

def create_psegs(cf, segs, parcels, psegs):
    if not arcpy.Exists(psegs):
        union_start = time.time()
        arcpy.analysis.Union([parcels, segs], psegs, "ALL", None, "NO_GAPS")
        etime(cf, "union to create psegs", union_start)

def update_fields(psegs, lc_path):
    # update fields
    fields = [f.name for f in list_fields(psegs)]
    # add new fields
    add_fields = ['PSID', 'Class_name']
    for f in add_fields:
        if f not in fields:
            ftype = 'LONG' if f == 'PSID' else 'TEXT'
            arcpy.management.AddField(psegs, f, ftype)

    # COMMENTED FOR V1
    # k = 0  # class count
    # class_dict = {0: 'NA', }
    # fields = ["Class_name", "Value"]

    # with arcpy.da.SearchCursor(lc_path, fields) as cursor:
    #     for row in cursor:
    #         class_dict[row[1]] = row[0]
    #         k = k + 1
    # print(f'there are {k} raster classes \n class_dict: {class_dict}')

    counter_psid = 0
    max_pid = calculate_max(psegs, 'PID', '<> 0')
    max_sid = calculate_max(psegs, 'SID', '<> 0')
    fields_to_update = ['PSID', 'PID', 'SID'] # removed 'MAJORITY', 'Class_name'
    print(counter_psid, max_pid, max_sid)
    with arcpy.da.UpdateCursor(psegs, fields_to_update) as cursor:
        for row in cursor:
            # counters
            counter_psid += 1
            max_pid += 1
            max_sid += 1
            # create psid
            row[0] = counter_psid
            # update PID
            if row[1] == 0:
                row[1] = max_pid
            # update SID
            if row[2] == 0:
                row[2] = max_sid
            # set Class_name to Majority
            # row[4] = class_dict[row[3]] #

            cursor.updateRow(row)
    fields_to_keep = ['OBJECTID', 'Shape', 'PSID', 'PID', 'SID', 'Class_name']
    for f in fields:
        if f not in fields_to_keep:
            try:
                arcpy.DeleteField_management(psegs, f)
            except arcpy.ExecuteError:
                print('cannot delete: ', f)

def run_prep(args):
    cf, project_folder = args
    print(f'\n{cf} {time.asctime()}')
    # paths
    input_path = f'{project_folder}/{cf}/input'

    gdb = f'{input_path}/dataprep.gdb'
    if not arcpy.Exists(gdb):
        print('dataprep.gdb does not exist, creating now')
        arcpy.management.CreateFileGDB(input_path, "dataprep.gdb")

    parcels_path = f'{input_path}/parcels.shp'
    # lc_path = f'{project_folder}/{cf}/temp/lc_fake_raster.tif'
    # lc_path = f'{project_folder}/{cf}/input/{cf}_landcover_2017_June2021.tif'
    files = os.listdir(f'{project_folder}/{cf}/input')
    pattern = f"{cf}_landcover_????_*.tif"
    for entry in files:
        if fnmatch.fnmatch(entry, pattern):
            print(f"Found landcover file: {entry}")
            lc_path = f'{project_folder}/{cf}/input/{entry}'
            break
    if not arcpy.Exists(lc_path):
        print(f"Failed to open LC: {lc_path}" )
        exit()
    snap_raster = lc_path
    seg_path = f"{project_folder}/{cf}/input/segments.gdb/lc_segs"
    # seg_path = f'{gdb}/segs_dissolved'

    # temp gdb
    temp_folder= f'{project_folder}/{cf}/temp'
    temp_gdb = f'{project_folder}/{cf}/temp/temp_dataprep.gdb'
    if not arcpy.Exists(temp_gdb):
        print('temp_dataprep.gdb does not exist, creating now')
        arcpy.management.CreateFileGDB(f"{project_folder}/{cf}/temp", "temp_dataprep.gdb")
    arcpy.env.snapRaster = lc_path 
    print('using input LC as snap raster, confirm outputs')
    arcpy.env.cellSize = 1
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(102039)
    arcpy.env.overwriteOutput = True
    st = time.time()
    step_1 = time.time()
    # 1 - fix & calculate Id (was uid) to sid for segments
    create_seg_ids(seg_path)
    etime(cf, f'SID in segs updated', step_1)
    # 2 - multipart to singlepart for segs and parcels
    step_2 = time.time()
    singpart_parcels = f'{temp_gdb}/singlepart_parcels'
    multi_2_single(parcels_path, singpart_parcels)
    create_parcel_ids(singpart_parcels)
    etime(cf,"parcels multi_2_single + create_parcel_ids", step_2)

    step_3s = time.time()
    clean_and_align_segs_and_parcels('segs', seg_path, "SID", gdb, snap_raster)
    etime(cf,f"clean_and_align_segs_and_parcels - segs", step_3s)
    step_3p = time.time()
    clean_and_align_segs_and_parcels('parcels', singpart_parcels, "PID", temp_folder, snap_raster)
    etime(cf,f"clean_and_align_segs_and_parcels - parcels", step_3p)

    # 4 - run zonal stats on lc using lc_segs as zones (was segs_vectorized) zones
    segs = f'{project_folder}/{cf}/input/segments.gdb/lc_segs'

    # 5 - union: segs x parcels
    step_5 = time.time()

    parcels = f'{temp_gdb}/parcels_vectorized'
    psegs = f'{gdb}/psegs'
    create_psegs(cf, segs, parcels, psegs)
    update_fields(psegs, lc_path)
    etime(cf, f'Union and update fields. (Almost done, boo!)', step_5)

    ###### 
    mp2sp_st = time.time()
    psegs_mtos_gdb= f'{gdb}/psegs_mp_to_sp'
    arcpy.MultipartToSinglepart_management(psegs, psegs_mtos_gdb)
    etime(cf, f"Multipart to singlepart completed", mp2sp_st)

    psegs_gpkg=f"{project_folder}\{cf}\input\data.gpkg"
    #make a clean geopackage
    makegpkg_st = time.time()
    arcpy.gp.CreateSQLiteDatabase (psegs_gpkg, 'GEOPACKAGE')
    etime(cf, "created GPKG", makegpkg_st)

    #copy psegs to geopackage
    copy_st = time.time()
    out_fc= f"{psegs_gpkg}/psegs"
    arcpy.management.CopyFeatures(psegs_mtos_gdb, out_fc)
    print(cf, f"Copied to geopackage", copy_st)
    ######

    etime(cf, f'Total run time: ', st)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='dataprep.py'
    )
    parser.add_argument('-folder', type=str, help='workspace')
    parser.add_argument('-cfs', nargs='+', help='list co_fips (cf) ', required=True)
    args = parser.parse_args()

    project_folder = rf"{args.folder}"
    cofips_list = list(args.cfs)

    f = open(f"{project_folder}/batch_log.txt", "a")
    f.write(f'\n\nSTART\n--{time.asctime()}\n--{os.path.basename(__file__)}\n--Batch List: {cofips_list}\n---\n\n')
    f.close()

    for cf in cofips_list:
        cfargs = (cf, project_folder)
        run_prep(cfargs)


    print('all processes finished')
