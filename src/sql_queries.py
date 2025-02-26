def country_data(country_code):
    query = f"""
WITH geom_points AS (
    SELECT 
        md."appId",
        md.ts,
        md."DIRECT_connection_mcc_mnc",
        md."LTE_0_rssi",
        md."LTE_1_rssi",
        md."LTE_2_rssi",
        md."LTE_3_rssi",
        md."LTE_4_rssi",
        md."LTE_5_rssi",
        md."LTE_6_rssi",
        md."LTE_7_rssi",
        md."LTE_8_rssi",
        md."LTE_9_rssi",
        ST_SetSRID(ST_MakePoint(md."LOC_longitude", md."LOC_latitude"), 4326) AS geom
    FROM 
        appdata.measurementdata md
    WHERE 
        md."LTE_0_rssi" IS NOT NULL
)

SELECT 
    gp."appId",
    gp.ts,
    ST_X(ST_Transform(gp.geom, 3035)) AS x,
    ST_Y(ST_Transform(gp.geom, 3035)) AS y,
    gp."DIRECT_connection_mcc_mnc",
    gp."LTE_0_rssi",
    gp."LTE_1_rssi",
    gp."LTE_2_rssi",
    gp."LTE_3_rssi",
    gp."LTE_4_rssi",
    gp."LTE_5_rssi",
    gp."LTE_6_rssi",
    gp."LTE_7_rssi",
    gp."LTE_8_rssi",
    gp."LTE_9_rssi"
FROM 
    geom_points gp
JOIN 
    spatial_help.european_borders2 eb 
ON 
    ST_Within(
        ST_Transform(gp.geom, 3857), 
        ST_Transform(eb.geom, 3857)
    )
WHERE 
    eb.nuts = '{country_code}'::text;
    """
    return(query)