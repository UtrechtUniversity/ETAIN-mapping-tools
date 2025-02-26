import subprocess
import os
from db_secrets import EtainDB

def raster_to_db(tif_path, table_name, etaindb):
    creds = etaindb
    database = creds.db_name
    user = creds.db_user
    password = creds.db_pass
    host = creds.db_address
    port = creds.db_port


    env = os.environ.copy()
    env["PGPASSWORD"] = password

    raster2pgsql_cmd = [
        "raster2pgsql", "-s", "4326", "-I", "-C", "-M", "-k", "-t", "1000x1000",  
        tif_path, table_name
    ]
    psql_cmd = [
        "psql", "-d", database, "-U", user, "-h", host, "-p", str(port)
    ]

    raster2pgsql_process = subprocess.Popen(raster2pgsql_cmd, stdout=subprocess.PIPE, env=env)
    subprocess.run(psql_cmd, stdin=raster2pgsql_process.stdout, env=env)

    raster2pgsql_process.stdout.close() 
    raster2pgsql_process.wait()

tif_path = r"C:\scripts\ETAIN_mapping_tools\data\private\test_output_db\output_db_test_CH3.tif"
table_name = "geoserver.rastertest2"
etaindb = EtainDB()

raster_to_db(tif_path, table_name, etaindb)

