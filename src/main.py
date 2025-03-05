import pandas as pd
import helper_functions as hf
import csv
import subprocess
import glob
import os
from datetime import date
from datetime import datetime

starttime = datetime.now()
today = date.today().strftime("%d%m%Y")
#define inputs

countries = ['AD','AL','AT','BA',       ###SKIPPING FOR NOW: 
             'BE','BG','CH','CY',       
             'CZ','DE','DK','EE','EL',  ###'RU','GE','AZ','TR','BY','GI': NOT EU
             'ES','FI','FR','GG',            
             'HR','HU','IE','IM',       
             'IS','IT','JE','LI','LT',
             'LU','LV','MC','MD','ME',
             'MK','MT','NL','NO','PL',
             'PT','RO','RS','SE',
             'SI','SK','SM','UK','UA',
             'VA','XK']


output_folder = f"C:/scripts/ETAIN_mapping_tools/data/private/output/rsrp/{today}" 
os.makedirs(output_folder, exist_ok=True)
cell_size_output = 25
saved_rasters = []
meas_per_country = {}
count = 0

for country in countries:
    count+=1
    print(f'processing {country}, {count} / {len(countries)}')
    output_name = f'LTE_RSRP_{country}_{today}'
    csv_output_path = f"{output_folder}/{output_name}.csv"
    tif_output_path = f"{output_folder}/{output_name}.tif"


    #reading input from database
    df = hf.fetch_country_data(country)
    print(f'Amount of measurements fetched: {len(df)}')
    if len(df) == 0:
        print(f'Skipped {country}, no measurements found')
        continue
    else:
        meas_per_country[country] = len(df)

    df = hf.add_frequency_colums(df)   #add frequency column calculated from earfcn
    df = hf.normalize_rsrp(df) #calculate normalized rsrp based on frequency columns
    df_mw = hf.convert_dBw_to_mW(df, copy_columns=True, save_csv=False) #convert dBm to mW

    #splitting dataframes by networkprovider
    split_dfs = hf.split_dataframes(df_mw)

    #create exposure array (exposure values) and count array (amount of network providers in a given cell)
    exposure_array,count_array,transform = hf.create_exposure_array(df_mw, split_dfs, cell_size_output)

    #use map calibration formula
    calibrated_array = hf.map_calibration(exposure_array,calibration_method='LTE_rsrp')

    hf.save_raster(tif_output_path,calibrated_array,transform,source_crs='EPSG:3035', target_crs='EPSG:3035')
    saved_rasters.append(tif_output_path)

    #hf.save_raster(f'{tif_output_path[:-4]}_count.tif',count_array,transform) ##FOR RASTER COUNTER

#write amount of measurements per country csv
with open(f'{output_folder}/measurements_percountry.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Writing header
    writer.writerow(['Key', 'Value'])
    # Writing data
    for key, value in meas_per_country.items():
        writer.writerow([key, value])

#create mosaic shp with gdal subprocess
tif_files = glob.glob(f"{output_folder}/*.tif")
command = ["gdaltindex", f"{output_folder}/mosaic.shp"] + tif_files
result = subprocess.run(command, capture_output=True, text=True)

 


