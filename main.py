import pandas as pd
import numpy as np
import rasterio as rio
from rasterio.transform import from_origin
from scipy.stats import binned_statistic_2d
from pyproj import Transformer
import importlib

import helper_functions as hf
importlib.reload(hf)

df = pd.read_csv(r'data\app_test_data_dump2\measurementData.csv',sep='\t',dtype=object)
df_filtered = hf.LTE_filter_table(df)

boundary_path = r'data\GIS\basel_boundary_4326.gpkg'
df_filtered_basel = hf.geofilter(df_filtered,boundary_path)
df_filtered_basel.to_csv(r'data\outputs\basel_points_testedata.csv',sep=';',index=False)

df_mw = hf.convert_dBw_to_mW(df_filtered_basel)

### THIS IS NOT CLEAN OR SUSTAINABLE #TODO FIX
#drop columns with nonsense network provider data
column_drops = [0,'x','y']
df_mw = df_mw.drop(df_mw[df_mw['DIRECT_sim_mcc_mnc'].isin(column_drops)].index)
df_mw = df_mw.reset_index(drop=True)

### 
#transform coordinates to EPSG:3035
transformer = Transformer.from_crs("EPSG:4326","EPSG:3035")
df_mw['y'], df_mw['x'] = (
    transformer.transform(df_mw['LOC_latitude'].values, df_mw['LOC_longitude'].values)
)
df_mw['y'] = round(df_mw['y'],3)
df_mw['x'] = round(df_mw['x'],3)

# df_mw = df_mw[['ts','appId','x','y','DIRECT_connection_mcc_mnc','LTE_mW_total']]
df_mw['LTE_dBw_total'] = 10 * np.log10(df_mw['LTE_mW_total']) - 30
df_mw.to_csv('data\outputs\output_raster.csv',sep=';',index=False)

if False: ####ACTIVATE THIS LATER, SPLIT DATAFRAMES BY NETWORK PROVIDER
    unique_values = df_mw['DIRECT_sim_mcc_mnc'].unique()
    print('Unique network providers: ')
    for uv in unique_values:
        print(uv)
    split_dfs = [df_mw[df_mw['DIRECT_sim_mcc_mnc'] == value] for value in unique_values]
    print('Amount of network provider specific dataframes: ' + str(len(split_dfs)))

# Define data bounds and grid size
xmin, ymin = df_mw['x'].min(), df_mw['y'].min()
xmax, ymax = df_mw['x'].max(), df_mw['y'].max()
cell_size = 25  # meters

# Calculate the number of cells in x and y directions
ncols = int(np.ceil((xmax - xmin) / cell_size))
nrows = int(np.ceil((ymax - ymin) / cell_size))

# Adjust bounds to ensure they are multiples of cell_size
xmin_adj = np.floor(xmin / cell_size) * cell_size
xmax_adj = xmin_adj + ncols * cell_size
ymin_adj = np.floor(ymin / cell_size) * cell_size
ymax_adj = ymin_adj + nrows * cell_size

# Create the raster grid
x_edges = np.linspace(xmin_adj, xmax_adj, ncols + 1)
y_edges = np.linspace(ymin_adj, ymax_adj, nrows + 1)

# Use binned_statistic_2d to calculate the median value in each cell
statistic, x_edge, y_edge, binnumber = binned_statistic_2d(
    df_mw['x'], df_mw['y'], df_mw['LTE_mW_total'], statistic='median', bins=[x_edges, y_edges]
)
###
##TODO: calculate sum of medians of network providers
###

#convert statistic back to dBm
statistic = 10 * np.log10(statistic) - 30

#use calibration formula on statistic?
statistic = 183.25 + (1.10 * statistic)
statistic = np.rot90(statistic, k=1) # Rotate array by 90 degrees

statistic =  10 ** ((statistic- 120) / 20)

# Save the raster to a TIFF file
transform = from_origin(xmin_adj, ymax_adj, cell_size, cell_size)
with rio.open(
    'data\outputs\output_raster.tif', 'w',
    driver='GTiff',
    height=statistic.shape[0],
    width=statistic.shape[1],
    count=1,
    dtype='float32',
    crs='EPSG:3035',
    transform=transform,
    nodata=99999
) as dst:
    dst.write(statistic, 1)

print("Raster file saved")