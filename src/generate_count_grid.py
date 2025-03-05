import psycopg2
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime

import db_secrets
import db_secrets

#db connection
db_creds = db_secrets.EtainDB()
db_params = {
    'dbname': db_creds.db_name,
    'user': db_creds.db_user,
    'password': db_creds.db_pass,
    'host': db_creds.db_address,
    'port': db_creds.db_port
}

conn = psycopg2.connect(**db_params)

#GRID FETCH
print('fetching grid from database')
grid_query = "SELECT fid, geom FROM spatial_help.nlch_hexgrid_500m"
grid_gdf = gpd.GeoDataFrame.from_postgis(grid_query, conn, geom_col='geom') 

#CLIP FETCH
print('fetching clip mask from database')
clip_query = "SELECT nuts,geom FROM spatial_help.european_borders WHERE nuts = 'CH' OR nuts = 'NL'"
clip_gdf = gpd.GeoDataFrame.from_postgis(clip_query, conn, geom_col='geom') 

print('starting clip...')
start_time = datetime.now()
grid_gdf = gpd.overlay(grid_gdf, clip_gdf, how='intersection')
print(f' Clipping time: {datetime.now()-start_time}')
grid_gdf['geom'] = grid_gdf['geometry']
grid_gdf.drop(labels = 'geometry', axis='columns')
grid_gdf

#MEASUREMENT FETCH
print('fetching measurements from database')
measurement_query = 'SELECT "LOC_longitude", "LOC_latitude" FROM appdata.measurementdata WHERE "LOC_longitude" IS NOT NULL AND "LTE_0_rssi" IS NOT NULL'
with conn.cursor() as cur:
    cur.execute(measurement_query)
    measurement_data = cur.fetchall()
measurement_df = gpd.GeoDataFrame(
    measurement_data,
    columns=['LOC_longitude', 'LOC_latitude']
)

print('create geometries for measurements')
#create geometries for measurements based on gps columns
measurement_df['geom'] = measurement_df.apply(
    lambda row: Point(row['LOC_longitude'], row['LOC_latitude']), axis=1
)
measurement_df
measurement_gdf = gpd.GeoDataFrame(measurement_df, geometry='geom', crs="EPSG:4326")

#match crs's
grid_crs = grid_gdf.crs 
measurement_gdf = measurement_gdf.to_crs(grid_crs)

#left join to keep all grid cells
result_gdf = gpd.sjoin(grid_gdf, measurement_gdf, how='left', predicate='contains')
#fill nan values to make sure unmatched rows are handled correctly
result_gdf['point_count'] = result_gdf.index_right.notna().astype(int)
#aggregate counts
result_counts = result_gdf.groupby('fid').agg(
    point_count=('index_right', lambda x: x.notna().sum()),
    geom=('geom', 'first')
).reset_index()

#make geodataframe from results
result_counts = gpd.GeoDataFrame(result_counts, geometry='geom', crs=grid_crs)
result_counts['point_count'] = result_counts['point_count'].fillna(0).astype(int)

#create connection for uploading to database (different one from before, other one didn't seem to work not sure why)
encoded_password = urllib.parse.quote(db_params['password'])
connection_string = f"postgresql+psycopg2://{db_params['user']}:{encoded_password}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
engine = create_engine(connection_string)

#upload to postgis database
result_counts.to_postgis('nlch_hexgrid_500m_with_counts', engine, schema='maps', if_exists='replace')
conn.close()

print("Script completed successfully.")

