import pandas as pd
import geopandas as gpd
import numpy as np

def LTE_filter_table(df):
    """
    Filter measurement data dump dataframe, returns dataframe that only contains
    relevant columns for LTE analysis.
    """
    var_list = [
        'ts', 'appId','LOC_latitude','LOC_longitude', 'DIRECT_connection_mcc_mnc','DIRECT_sim_mcc_mnc',
        'LTE_0_rssi','LTE_1_rssi','LTE_2_rssi','LTE_3_rssi','LTE_4_rssi',
        'LTE_5_rssi','LTE_6_rssi','LTE_7_rssi','LTE_8_rssi','LTE_9_rssi',
    ]
    return df[var_list]

def geofilter(df,boundary_path,crs='4326'):
    """
    Returns filtered measurement data dataframe, containing only values within
    a geographic boundary
    """
    gdf_points = gpd.GeoDataFrame(df,geometry=gpd.points_from_xy(df.LOC_longitude,df.LOC_latitude))
    gdf_boundary = gpd.read_file(boundary_path,crs=crs)
    gdf_geofiltered = gdf_points[gdf_points.geometry.within(gdf_boundary.geometry.iloc[0])]
    gdf_geofiltered = gdf_geofiltered.drop('geometry',axis = 'columns')
    return gdf_geofiltered

def convert_dBw_to_mW(df,column_list=None):
    """
    Function to convert dBw to mW and sum all LTE cells together
    """
    if column_list == None:
        column_list=['LTE_0_rssi','LTE_1_rssi','LTE_2_rssi','LTE_3_rssi','LTE_4_rssi',
                    'LTE_5_rssi','LTE_6_rssi','LTE_7_rssi','LTE_8_rssi','LTE_9_rssi']
    df['LTE_0_rssi_dBw'] = df['LTE_0_rssi']
    for column in column_list:
        df[column] =  10** (df[column].astype(float)/10) * 1000
    
    df=df.fillna(0)
    
    df['LTE_mW_total'] = df[column_list].sum(axis=1)
    
    #remove data points where the total == 0
    df = df[df['LTE_mW_total'] != 0]

    return df



