import geopandas as gpd
from pyproj import Transformer
import pandas as pd
import numpy as np
import scipy.stats
import rasterio.transform
import rasterio as rio

def LTE_filter_table(df):
    """
    Filter measurement data dump dataframe, returns dataframe that only contains
    relevant columns for LTE analysis.
    """
    var_list = [
        'ts', 'appId','LOC_latitude','LOC_longitude', 'DIRECT_connection_mcc_mnc',
        'LTE_0_rssi','LTE_1_rssi','LTE_2_rssi','LTE_3_rssi','LTE_4_rssi',
        'LTE_5_rssi','LTE_6_rssi','LTE_7_rssi','LTE_8_rssi','LTE_9_rssi',
    ]
    return df[var_list]

def geofilter(df,boundary_path,crs='4326'):
    """
    Returns filtered measurement data dataframe, containing only values within
    a geographic boundary. Also removes rows that contain max int32 values and
    transforms coordinates from EPSG:4326 to EPSG:3035
    """
    print('converting points to geodataframe..')
    gdf_points = gpd.GeoDataFrame(df,geometry=gpd.points_from_xy(df.LOC_longitude,df.LOC_latitude))
    gdf_boundary = gpd.read_file(boundary_path,crs=crs)
    print('geofiltering...')
    gdf_geofiltered = gdf_points[gdf_points.geometry.within(gdf_boundary.geometry.iloc[0])]
    gdf_geofiltered = gdf_geofiltered.drop('geometry',axis = 'columns')

    #create mask for value 2147483647 and remove those rows
    print('drop rows with values 2147483647...')
    mask = gdf_geofiltered.map(lambda x: x == '2147483647')  
    rows_to_drop = mask.any(axis=1)  
    gdf_geofiltered= gdf_geofiltered[~rows_to_drop]

    print('transform to 3035')
    #transform coordinates to EPSG:3035
    transformer = Transformer.from_crs("EPSG:4326","EPSG:3035")
    gdf_geofiltered['y'], gdf_geofiltered['x'] = (
        transformer.transform(gdf_geofiltered['LOC_latitude'].values, gdf_geofiltered['LOC_longitude'].values)
    )
    gdf_geofiltered['y'] = round(gdf_geofiltered['y'],3)
    gdf_geofiltered['x'] = round(gdf_geofiltered['x'],3)
    return gdf_geofiltered

def convert_dBw_to_mW(df,column_list=None,copy_columns=False,save_path=False):
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

    if save_path != False:
        df.to_csv(save_path,sep=';',index=False)
    return df


def split_dataframes(df):
    """
    Split input dataframes by network provider

    Returns list of split dataframes
    """
    print("splitting dataframes...")
    unique_values = df['DIRECT_connection_mcc_mnc'].unique()
    print('Unique network providers: ')
    for uv in unique_values:
        print(uv)
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


    #calculate median mW for each cell and add them to the sum array
    for df in split_dfs:
        statistic, x_edge, y_edge, binnumber = scipy.stats.binned_statistic_2d(
            df['x'], df['y'], df['LTE_mW_total'], statistic='median', bins=[x_edges, y_edges]
        )
        statistic = np.nan_to_num(statistic, nan=0.0) #convert nans to 0 for calculating purposes
        sum_array += statistic

    #############
    np.where(sum_array == 0, np.nan,sum_array) #convert 0 back to nan
    sum_array = 10 * np.log10(sum_array) - 30
    sum_array = np.rot90(sum_array, k=1) # Rotate array by 90 degrees
    transform = rasterio.transform.from_origin(xmin_adj, ymax_adj, cell_size, cell_size)

    return sum_array,transform

def map_calibration(exposure_array,calibration_method):
    if calibration_method == 'swiss':
        calibrated_array = 183.25 + (1.10 * exposure_array) #apply map calibration
        calibrated_array = 10 ** ((calibrated_array- 120) / 20) #convert dBv/m to V/m
    return calibrated_array

def save_raster(output_raster_path, array, transform):
    with rio.open(
        output_raster_path, 'w',
        driver='GTiff',
        height=array.shape[0],
        width=array.shape[1],
        count=1,
        dtype='float32',
        crs='EPSG:3035',
        transform=transform,
        nodata=0
    ) as dst:
        dst.write(array, 1)

    print("Raster file saved")