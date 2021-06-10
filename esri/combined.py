import argparse
import sys
import arcpy
from arcpy.sa import *
import multiprocessing as mp
import time
import os
import fnmatch
arcpy.CheckOutExtension("Spatial")

from dataPrep import run_prep
from tabulateArea import run_ta
import luconfig


parser = argparse.ArgumentParser(description='Integrated Land Use production arg parser')
parser.add_argument('-cfs', nargs='+', help='list co_fips (cf) ', required=True)
args = parser.parse_args()
cflist = list(args.cfs)

folder = luconfig.folder # r"M:/projects/landuse/V1"
print(folder)
anci_folder = luconfig.anci_folder #r"B:/ancillary"

for cf in cflist:
    run_prep(cf, folder)
    runTA(cf, folder, anci_folder)