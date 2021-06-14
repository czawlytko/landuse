import os
import fnmatch

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


