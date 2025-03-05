import pandas as pd
import helper_functions as hf
from datetime import date
from datetime import datetime
import os


starttime = datetime.now()
today = date.today().strftime("%d%m%Y")
#define inputs

countries = ['AD','AL','AT','BA',       ###SKIPPING FOR NOW: 
             'BE','BG','CH','CY',       
             'CZ','DE','DK','EE','EL',  ###'FI','NO','SE','UK': BORDER TOO COMPLEX ADD THESE BACK AFTER GEOM FIX
             'ES','FR','GG',            
             'HR','HU','IE','IM',       ###'RU','GE','AZ','TR','BY','GI': NOT EU
             'IS','IT','JE','LI','LT',
             'LU','LV','MC','MD','ME',
             'MK','MT','NL','PL',
             'PT','RO','RS',
             'SI','SK','SM','UA',
             'VA','XK']
len(countries)
cell_size_output = 25
saved_rasters = []
count = 0
for country in countries:
    count+=1
    print(f'processing {country}, {count} / {len(countries)}')
    output_name = f'LTE_RSSI_{country}_{today}'
    csv_output_path = f"C:/scripts/ETAIN_mapping_tools/data/private/output/{output_name}.csv"
    tif_output_path = f"C:/scripts/ETAIN_mapping_tools/data/private/output/{output_name}.tif"


    #reading input from database
    df = hf.fetch_country_data(country)
    print(f'Amount of measurements fetched: {len(df)}')
    if len(df) == 0:
        print(f'Skipped {country}, no measurements found')
        continue
    #filtering and converting
    df_mw = hf.convert_dBw_to_mW(df, copy_columns=True, save_csv=csv_output_path) #convert dBm to mW

    #splitting dataframes by networkprovider
    split_dfs = hf.split_dataframes(df_mw)

    #create exposure array (exposure values) and count array (amount of network providers in a given cell)
    exposure_array,count_array,transform = hf.create_exposure_array(df_mw, split_dfs, cell_size_output)

    #use map calibration formula
    calibrated_array = hf.map_calibration(exposure_array,calibration_method='rssi_temp')

    hf.save_raster(tif_output_path,calibrated_array,transform,source_crs='EPSG:3035', target_crs='EPSG:3035')
    saved_rasters.append(tif_output_path)

    #hf.save_raster(f'{tif_output_path[:-4]}_count.tif',count_array,transform) ##FOR RASTER COUNTER

import importlib
importlib.reload(hf)
starttime = datetime.now()
today = date.today().strftime("%d%m%Y")
if True:
    saved_rasters = []
    folder = "C:/scripts/ETAIN_mapping_tools/data/private/output"
    for file_name in os.listdir(folder):
        if file_name.endswith('.tif'):
            print(file_name)
            file_path = os.path.join(folder,file_name)
            saved_rasters.append(file_path)

    
merged_output = f"C:/scripts/ETAIN_mapping_tools/data/private/output/LTE_RSSI_EU_{today}.tif"
hf.merge_rasters(saved_rasters,merged_output)

print(f'Done, time taken:{datetime.now()-starttime}')