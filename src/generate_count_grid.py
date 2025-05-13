import psycopg2
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime,date,timedelta
import os

import db_secrets

today = date.today().strftime("%d%m%Y")
output_folder = f'data/private/output/{today}'

###TODO: MULTITHREADING BOTH GENERATION AND MERGING
#db connection
db_creds = db_secrets.EtainDB()
db_params = {
    'dbname': db_creds.db_name,
    'user': db_creds.db_user,
    'password': db_creds.db_pass,
    'host': db_creds.db_address,
    'port': db_creds.db_port
}

countries = ['AT','AD','AL','BA',       ###SKIPPING FOR NOW: 
             'BE','BG','CH','CY',       
             'CZ','DE','DK','EE','EL',  ###'RU','GE','AZ','TR','BY','GI': NOT EU
             'ES','FI','FR','GG',       ### 'US'          
             'HR','HU','IE','IM',       
             'IS','IT','JE','LI','LT',
             'LU','LV','MC','MD','ME',
             'MK','MT','NL','NO','PL',
             'PT','RO','RS','SE',
             'SI','SK','SM','UK',
             'UA','VA','XK']

conn = psycopg2.connect(**db_params)

# Fetch measurement data 
print('Fetching all measurements')
measurement_query = '''
    SELECT "LOC_longitude", "LOC_latitude" FROM appdata.measurementdata
    WHERE "LOC_longitude" IS NOT NULL AND ("LTE_0_rssi" IS NOT NULL OR "LTE_0_rsrp" IS NOT NULL)
'''

with conn.cursor() as cur:
    cur.execute(measurement_query)
    measurement_data = cur.fetchall()

measurement_df = pd.DataFrame(measurement_data, columns=['LOC_longitude', 'LOC_latitude'])

if measurement_df.empty:
    print('No measurements found, exiting.')
    exit()

print('Creating geometries for measurements')
measurement_df['geom'] = measurement_df.apply(lambda row: Point(row['LOC_longitude'], row['LOC_latitude']), axis=1)
measurement_gdf = gpd.GeoDataFrame(measurement_df, geometry='geom', crs="EPSG:4326")
print(f'Matching CRS')
measurement_gdf_transformed = measurement_gdf.to_crs("EPSG:3857")



for country in countries:
    print(f'Fetching grid for {country}')
    grid_query = f'SELECT fid, geom FROM hexagon_grids."{country}_hexgrid_500m"'
    grid_gdf = gpd.GeoDataFrame.from_postgis(grid_query, conn, geom_col='geom')
    

    
    print(f'Counting measurements in grid cells for {country}')
    result_gdf = gpd.sjoin(grid_gdf, measurement_gdf_transformed, how='left', predicate='contains')
    result_gdf['point_count'] = result_gdf.index_right.notna().astype(int)

    result_counts = result_gdf.groupby('fid').agg(
        point_count=('index_right', lambda x: x.notna().sum()),
        geom=('geom', 'first')
    ).reset_index()

    result_counts = gpd.GeoDataFrame(result_counts, geometry='geom', crs=grid_gdf.crs)
    result_counts['point_count'] = result_counts['point_count'].fillna(0).astype(int)
    result_counts['country'] = country 


    if result_counts.empty:
        print(f'No measurement data to save for {country}, skipping.')
        continue

    output_path = f'{output_folder}/counts{country}.gpkg'
    result_counts.to_file(output_path, driver='GPKG')

    print(f'Saved results for {country}')
        
    del grid_gdf, result_gdf, result_counts

###TODO: MULTITHREADING BOTH GENERATION AND MERGING
#write to gpkg
output_gpkg = f"{output_folder}/count_merged{today}.gpkg"
layer_name = "count"

gdfs = []

for file in os.listdir(output_folder):
    if file.endswith(".gpkg"):
        print(file)
        file_path = os.path.join(output_folder, file)
        
        gdf = gpd.read_file(file_path)
        
        gdfs.append(gdf)

if gdfs:
    print('merging gdf')
    merged_gdf = pd.concat(gdfs, ignore_index=True)

    print('saving to merged gpkg')
    merged_gdf.to_file(output_gpkg, layer=layer_name, driver="GPKG")
    print(f"Successfully merged {len(merged_gdf)} features into {output_gpkg}")

else:
    print("No valid data found to merge.")
print('Script completed successfully.')

