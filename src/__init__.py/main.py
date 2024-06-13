import pandas as pd
import importlib
import helper_functions as hf
importlib.reload(hf)

#define inputs
input_csv_path = r'data\private\app_test_data_dump3\measurementData.csv'
boundary_path = r'data\private\GIS\europe\europe_4326_square.gpkg'
csv_output_path = r'data\private\outputs\all_measurements\output_raster.csv'
tif_output_path = r'data\private\outputs\all_measurements\output_raster2.tif'

#reading input csv
df = pd.read_csv(input_csv_path,sep='\t',dtype=object)

#filtering and converting
df_filtered = hf.LTE_filter_table(df) #select only LTE columns
df_filtered_geo = hf.geofilter(df_filtered,boundary_path) #select only records inside boundary_path
df_mw = hf.convert_dBw_to_mW(df_filtered_geo, copy_columns=True, save_path=csv_output_path) #convert dBm to mW

#splitting dataframes by networkprovider
split_dfs = hf.split_dataframes(df_mw)

#create exposure array
exposure_array,transform = hf.create_exposure_array(df_mw, split_dfs, 25)

#use map calibration formula
calibrated_array = hf.map_calibration(exposure_array,calibration_method='swiss')

hf.save_raster(tif_output_path,calibrated_array,transform)