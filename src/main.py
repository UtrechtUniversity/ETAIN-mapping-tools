import importlib
import pandas as pd
import helper_functions as hf
import csv
import subprocess
import glob
import os
import gc
from datetime import date, datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from osgeo import gdal
import re
import time
os.environ['PROJ_LIB'] = r'data'


def process_country(country, ssi, today, output_folder, cell_size_output):
    print(f'Processing {country} {ssi}')
    output_name = f'LTE_{ssi}_{country}_{today}'
    csv_output_path = f"{output_folder}/{output_name}.csv"
    tif_output_path = f"{output_folder}/{output_name}.tif"

    df = hf.fetch_country_data(country, ssi)
    if df.empty:
        return (country, 0, None)

    df = hf.add_frequency_colums(df)
    df = hf.normalize_ssi(df, ssi)
    df_mw = hf.convert_dBw_to_mW(df, ssi, copy_columns=True, save_csv=False)
    split_dfs = hf.split_dataframes(df_mw)

    exposure_array, count_array, transform = hf.create_exposure_array(df_mw, split_dfs, cell_size_output)
    calibrated_array = hf.map_calibration(exposure_array, calibration_method=f'LTE_{ssi}')
    hf.save_raster(tif_output_path, calibrated_array, transform, source_crs='EPSG:3035', target_crs='EPSG:3035')

    return (country, len(df), tif_output_path)

def submit_with_retry(executor, func, *args, **kwargs):
    """Keep retrying until submission and execution succeeds."""
    while True:
        try:
            future = executor.submit(func, *args, **kwargs)
            return future
        except MemoryError:
            print("MemoryError on submission. Waiting before retrying...")
            time.sleep(10)
        except Exception as e:
            print(f"Error on submission: {e}. Retrying...")
            time.sleep(5)

if __name__ == '__main__':
    starttime = datetime.now()
    today = date.today().strftime("%d%m%Y")
    ssi_values = ['rssi','rsrp']
    

    cell_size_output = 25

    countries = ['AT','AD','AL','BA',
                 'BE','BG','CH','CY',       
                 'CZ','DE','DK','EE','EL',  
                 'ES','FI','FR','GG',      
                 'HR','HU','IE','IM',       
                 'IS','IT','JE','LI','LT',
                 'LU','LV','MC','MD','ME',
                 'MK','MT','NL','NO','PL',
                 'PT','RO','RS','SE',
                 'SI','SK','SM','UK','UA',
                 'VA','XK']

    meas_per_country = {}
    saved_rasters = []

    for ssi in ssi_values:
        ##PUT BREAK HERE
        output_folder = f"C:/scripts/ETAIN_mapping_tools/data/private/output/{today}/{ssi}"
        os.makedirs(output_folder, exist_ok=True)
        
        with ProcessPoolExecutor(max_workers=1) as executor:
            futures = {
                submit_with_retry(
                    executor,
                    process_country,
                    country, ssi, today, output_folder, cell_size_output
                ): country
                for country in countries
            }

            for future in as_completed(futures):
                country = futures[future]
                while True:  # Retry loop around result fetching
                    try:
                        country_code, meas_count, raster_path = future.result()
                        if meas_count > 0:
                            meas_per_country[country_code] = meas_count
                            saved_rasters.append(raster_path)
                            print(f'{country_code} processed: {meas_count} measurements')
                        else:
                            print(f'{country_code} skipped: no measurements found')
                        break  # success, break retry loop
                    except MemoryError:
                        del futures[future]
                        del future
                        print(f'MemoryError while processing {country}. Retrying after delay...')
                        time.sleep(10)
                        future = submit_with_retry(
                            executor,
                            process_country,
                            country, ssi, today, output_folder, cell_size_output
                        )
                    except Exception as e:
                        print(f'Error processing {country}: {e}. Retrying...')
                        time.sleep(5)
                        future = submit_with_retry(
                            executor,
                            process_country,
                            country, ssi, today, output_folder, cell_size_output
                        )

        

        print(f"\nprocessing time {ssi}: {(datetime.now() - starttime).total_seconds():.2f} seconds.")


    #MERGE RSSI AND RSRP FILES
    folder_path = f"C:/scripts/ETAIN_mapping_tools/data/private/output/{today}"
    rssi_folder = f"{folder_path}/rssi"
    rsrp_folder = f"{folder_path}/rsrp"
    output_folder = f"{folder_path}/merged"
    os.makedirs(output_folder, exist_ok=True)

    def get_country_code(filename):
        match = re.search(r'_([A-Z]{2})_', filename)
        return match.group(1) if match else None

    #list TIFFs in each folder
    rssi_files = {get_country_code(f): os.path.join(rssi_folder, f)
                for f in os.listdir(rssi_folder) if f.endswith('.tif')}
    rsrp_files = {get_country_code(f): os.path.join(rsrp_folder, f)
                for f in os.listdir(rsrp_folder) if f.endswith('.tif')}
    common_countries = rssi_files.keys() & rsrp_files.keys()

    #merge pairs
    for country in common_countries:
        print(f"merging {country}")
        rssi_path = rssi_files[country]
        rsrp_path = rsrp_files[country]
        output_path = os.path.join(output_folder, f"LTE_merged_{country}_{today}.tif")

        gdal.Warp(
            destNameOrDestDS=output_path,
            srcDSOrSrcDSTab=[rsrp_path, rssi_path],
            dstSRS="EPSG:3035",
            format="GTiff",
            multithread=True,
            warpOptions=["COMPRESS=DEFLATE"]
        )
        #remove rssi/rsrp input (not needed after merge)
        os.remove(rssi_path)
        os.remove(rsrp_path)






    #create mosaic shp with gdal subprocess
    tif_files = glob.glob(f"{output_folder}/*.tif")
    os.environ['GTIFF_SRS_SOURCE'] = 'EPSG'
    command = ["gdaltindex","-nodata","nan", f"{output_folder}/mosaic.shp"] + tif_files
    subprocess.run(command)
    result = subprocess.run(command, capture_output=True, text=True)


    hf.fetch_metadata(folder_path,today)
