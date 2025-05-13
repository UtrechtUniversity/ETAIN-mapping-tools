def country_data(country_code,ssi):
    query = f"""
WITH geom_points AS (
    SELECT 
        md."appId",
        md.ts,
        md."DIRECT_connection_mcc_mnc",
        md."LTE_0_{ssi}",
        md."LTE_1_{ssi}",
        md."LTE_2_{ssi}",
        md."LTE_3_{ssi}",
        md."LTE_4_{ssi}",
        md."LTE_5_{ssi}",
        md."LTE_6_{ssi}",
        md."LTE_7_{ssi}",
        md."LTE_8_{ssi}",
        md."LTE_9_{ssi}",
        md."LTE_0_earfcn",
        md."LTE_1_earfcn",
        md."LTE_2_earfcn",
        md."LTE_3_earfcn",
        md."LTE_4_earfcn",
        md."LTE_5_earfcn",
        md."LTE_6_earfcn",
        md."LTE_7_earfcn",
        md."LTE_8_earfcn",
        md."LTE_9_earfcn",
        ST_SetSRID(ST_MakePoint(md."LOC_longitude", md."LOC_latitude"), 4326) AS geom
    FROM 
        appdata.measurementdata md
    WHERE 
        md."LTE_0_{ssi}" IS NOT NULL
)

SELECT 
    gp."appId",
    gp.ts,
    ST_X(ST_Transform(gp.geom, 3035)) AS x,
    ST_Y(ST_Transform(gp.geom, 3035)) AS y,
    gp."DIRECT_connection_mcc_mnc",
    gp."LTE_0_{ssi}",
    gp."LTE_1_{ssi}",
    gp."LTE_2_{ssi}",
    gp."LTE_3_{ssi}",
    gp."LTE_4_{ssi}",
    gp."LTE_5_{ssi}",
    gp."LTE_6_{ssi}",
    gp."LTE_7_{ssi}",
    gp."LTE_8_{ssi}",
    gp."LTE_9_{ssi}",
    gp."LTE_0_earfcn",
    gp."LTE_1_earfcn",
    gp."LTE_2_earfcn",
    gp."LTE_3_earfcn",
    gp."LTE_4_earfcn",
    gp."LTE_5_earfcn",
    gp."LTE_6_earfcn",
    gp."LTE_7_earfcn",
    gp."LTE_8_earfcn",
    gp."LTE_9_earfcn"
FROM 
    geom_points gp
JOIN 
    spatial_help.european_borders_simple eb 
ON 
    ST_Within(
        ST_Transform(gp.geom, 3857), 
        ST_Transform(eb.geom, 3857)
    )
WHERE 
    eb.nuts = '{country_code}'::text;
    """
    return(query)

def fetch_metadata():
    query = '''
    SELECT 
    COUNT(*) AS total_rows, 
    COUNT(DISTINCT "appId") AS unique_appIds
    FROM appdata.measurementdata
    '''
    return query
