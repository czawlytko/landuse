#! /usr/bin/python

import geopandas as gpd
from pathlib import Path
import argparse
import time
import inspect
import azconfig

print("gpd ver: ", gpd.__version__)

from azure.storage.blob import BlobServiceClient

def blob2gdf(infile):
    STORAGEACCOUNTURL= f"https://{azconfig._STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    STORAGEACCOUNTKEY= azconfig._STORAGE_ACCOUNT_KEY
    LOCALFILENAME= r"TempPath/t_data.zip"
    CONTAINERNAME= "batching"
    BLOBNAME= infile

    #download from blob
    t1=time.time()
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    # blob_client_instance = blob_service_client.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    # typo in azure example... wtf
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

    print("primary_endpoint ", blob_client_instance.primary_endpoint, '\n')


    with open(LOCALFILENAME, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)
    t2=time.time()
    print(f"It takes {t2 - t1} seconds to download {BLOBNAME}")


    if ".zip" in infile:
        gdf = gpd.read_file(f"zip://{LOCALFILENAME}!parcels.shp", crs="EPSG:5070")

    else:
        print("NOT A ZIP, DO SOMETHING ELSE")
        exit()
    return gdf


if __name__ == "__main__":
    print('start main')
    # myzippath = "C:\scripts\landuse_batching\src\data\parcel_data.zip" # contains parcels.shp

    parser = argparse.ArgumentParser(
        description='Land Use Batch production argument parcer'
    )
    parser.add_argument('-cfs', nargs='+', help='list co_fips (cf) ', required=True)
    parser.add_argument('-outdir', type=str, help='folder or directory for outputs')

    args = parser.parse_args()
    cflist = list(args.cfs)
    outdir = str(args.outdir)

    for cf in cflist:
        print("\n",cf.upper(),"\n")
        # some_gdf = blob2gdf(fr"rev1_testing/{cf}/input.zip!parcels.shp")
        some_gdf = blob2gdf(fr"rev1_testing/{cf}/input.zip")
        print(f"{cf} parcel records length: {len(some_gdf)}\n")

        if 'lu' not in some_gdf.columns:
            print("----Adding 'lu' col")
            some_gdf['lu'] = "probably grass"

        # save copy locally

        # # some_gdf.to_file(outPath, layer=outLayer, driver='GPKG')
        # some_gdf.to_file(filename=f'C:/data/{cf}/input.zip', driver='ESRI Shapefile')

        # # Create a blob client using the local file name as the name for the blob
        # blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        # print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        # # Upload the created file
        # with open(upload_file_path, "rb") as data:
        #     blob_client.upload_blob(data)


