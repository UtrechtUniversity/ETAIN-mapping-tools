import geopandas as gpd
from pyproj import Transformer
import pandas as pd
import numpy as np
import scipy.stats
import rasterio as rio
from rasterio.merge import merge
from rasterio.enums import Resampling
from rasterio.warp import calculate_default_transform, reproject
from urllib.parse import quote
from sqlalchemy import create_engine
from datetime import datetime
from osgeo import gdal

import db_secrets
import sql_queries

def _postgres_connect():
    """
    Create a connection to the PostgreSQL database using SQLAlchemy.

    Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy engine instance 
        connected to the specified PostgreSQL database.
    """
    db_creds = db_secrets.EtainDB()

    user = quote(db_creds.db_user)
    password = quote(db_creds.db_pass)
    host = db_creds.db_address
    port = db_creds.db_port
    db_name = db_creds.db_name

    db_uri = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(db_uri)
    return engine

def fetch_country_data(country_code):
    """
    Fetch measurement data for a specified country from the database.
    Constructs a SQL query based on the country code in the database.

    Args:
        country_code (str): The country code to filter the measurement 
        data.

    Returns:
        pandas.DataFrame: A DataFrame containing the measurement data 
        for the specified country, with columns for appId, timestamps,
        and various LTE rssi values. Geometry is already constructed
        in the "geom" column
    """
    fetch_start = datetime.now()
    print(f'Fetch start: {fetch_start}')
    query = sql_queries.country_data(country_code)
    df = pd.read_sql(query, _postgres_connect())
    print(f'Fetch duration: {datetime.now()-fetch_start}')
    return df

def convert_dBw_to_mW(df,column_list=None,copy_columns=False,save_csv=False):
    """
    Function to convert dBw to mW and sum all LTE cells together
    """
    if column_list == None:
        column_list=['LTE_0_rssi','LTE_1_rssi','LTE_2_rssi','LTE_3_rssi','LTE_4_rssi',
                    'LTE_5_rssi','LTE_6_rssi','LTE_7_rssi','LTE_8_rssi','LTE_9_rssi']
    if copy_columns == True:
        for col in column_list:
            df[f"{col}_dBw"] = df[col].copy()

    for column in column_list:
        df[column] =  10** (df[column].astype(float)/10) * 1000
    
    df=df.fillna(0)
    
    df['LTE_mW_total'] = df[column_list].sum(axis=1)
    
    #remove data points where the total == 0
    df = df[df['LTE_mW_total'] != 0]

    ### THIS IS NOT CLEAN OR SUSTAINABLE #TODO FIX
    #drop rows with nonsense network provider data
    row_drops = [0,'x','y']
    df = df.drop(df[df['DIRECT_connection_mcc_mnc'].isin(row_drops)].index)
    df = df.reset_index(drop=True)

    if save_csv != False:
        df.to_csv(save_csv,sep=';',index=False)
    return df


def split_dataframes(df):
    """
    Split input dataframes by network provider

    Returns list of split dataframes
    """
    print("splitting dataframes...")
    unique_values = df['DIRECT_connection_mcc_mnc'].unique()
    # print('Unique network providers: ')
    # for uv in unique_values:
    #     print(uv)
    split_dfs = [df[df['DIRECT_connection_mcc_mnc'] == value] for value in unique_values]
    print('Amount of network provider specific dataframes: ' + str(len(split_dfs)))

    return split_dfs

def create_exposure_array(df, split_dfs,cell_size):
    """
    Creates an exposure array representing the median LTE mW total values for a specified grid size,
    and sums these values across different network providers.

    Parameters:
    df (DataFrame): The original DataFrame containing 'x', 'y', and 'LTE_mW_total' columns.
    split_dfs (list of DataFrames): A list of DataFrames, each representing different network providers
                                     with 'x', 'y', and 'LTE_mW_total' columns.
    cell_size: int in georeferences units based on CRS (meters in the case of EPSG:3035)

    Returns:
    numpy.ndarray: A 2D array with the log-transformed sum of median LTE mW total values for each cell.
    rasterio.transform.Affine: The affine transform for the generated raster grid.

    Notes:
    - The function ensures that zero values are converted to NaN for the final output.
    """

    # Define data bounds and grid size
    xmin, ymin = df['x'].min(), df['y'].min()
    xmax, ymax = df['x'].max(), df['y'].max()

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



    sum_array = np.zeros((ncols, nrows)) #initialize sum array for adding array values from different network providers
    count_array = np.zeros((ncols, nrows)) #initialize count array for counting the amount of network providers

    #calculate median mW for each cell and add them to the sum array
    for df in split_dfs:
        ############if out of bounds error, probably a geometry issue. Spain had this issue until i removed the far away islands from the geometry
        statistic, x_edge, y_edge, binnumber = scipy.stats.binned_statistic_2d(
            df['x'], df['y'], df['LTE_mW_total'], statistic='median', bins=[x_edges, y_edges]
        )
        statistic = np.nan_to_num(statistic, nan=0.0) #convert nans to 0 for calculating purposes
        sum_array += statistic

        non_zero_mask = statistic != 0  #check which cells have valid values
        count_array += non_zero_mask #add valid values to count array to count network providers

    #############
    np.where(sum_array == 0, np.nan,sum_array) #convert 0 back to nan
    sum_array = 10 * np.log10(sum_array) - 30 #convert mW to dBw

    sum_array = np.rot90(sum_array, k=1) 
    count_array = np.rot90(count_array, k=1)

    transform = rio.transform.from_origin(xmin_adj, ymax_adj, cell_size, cell_size)

    return sum_array, count_array,transform

def map_calibration(exposure_array,calibration_method):
    if calibration_method == 'rssi_temp':
        calibrated_array = 183.25 + (1.10 * exposure_array) #apply map calibration
        calibrated_array = 10 ** ((calibrated_array- 120) / 20) #convert dBv/m to V/m
    elif calibration_method == 'rsrp_temp':
        calibrated_array = 179.31 + (0.89 * exposure_array) #apply map calibration
        calibrated_array = 10 ** ((calibrated_array- 120) / 20) #convert dBv/m to V/m
    return calibrated_array

def save_raster(output_raster_path, array, transform, source_crs='EPSG:3035',target_crs='EPSG:3035'):
    
    #calculate the bounds from the transform
    height, width = array.shape
    left = transform.c
    right = transform.c + transform.a * width
    top = transform.f
    bottom = transform.f + transform.e * height
    
    #calculate the transform and dimensions for the new CRS
    dst_transform, dst_width, dst_height = calculate_default_transform(
        source_crs, target_crs, width, height, left, bottom, right, top
    )
    
    reprojected_array = np.empty((dst_height, dst_width), dtype='float32')
    reproject(
        source=array,
        destination=reprojected_array,
        src_transform=transform,
        src_crs=source_crs,
        dst_transform=dst_transform,
        dst_crs=target_crs,
        resampling=Resampling.nearest
    )
    
    #valid data check
    if np.all(reprojected_array == 0):
        print("Warning: Reprojected array contains only zeros. Check source CRS and transformation.")
    
    #write reprojected data
    with rio.open(
        output_raster_path, 'w',
        driver='GTiff',
        height=dst_height,
        width=dst_width,
        count=1,
        dtype='float32',
        crs=target_crs,
        transform=dst_transform,
        nodata=0
    ) as dst:
        dst.write(reprojected_array, 1)
        dst.update_tags(1, 
                        STATISTICS_MAXIMUM=reprojected_array.max(), 
                        STATISTICS_MINIMUM=reprojected_array.min(), 
                        STATISTICS_MEAN=reprojected_array.mean(), 
                        STATISTICS_STDDEV=reprojected_array.std())
    
    print(f"Raster file saved in {target_crs}")

import os
import rasterio as rio
from rasterio.merge import merge

def merge_rasters(raster_list, output_file):
    gdal.Warp(output_file, raster_list)


