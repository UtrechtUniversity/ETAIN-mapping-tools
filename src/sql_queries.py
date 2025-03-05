def country_data(country_code):
    query = f"""
WITH geom_points AS (
    SELECT 
        md."appId",
        md.ts,
        md."DIRECT_connection_mcc_mnc",
        md."LTE_0_rsrp",
        md."LTE_1_rsrp",
        md."LTE_2_rsrp",
        md."LTE_3_rsrp",
        md."LTE_4_rsrp",
        md."LTE_5_rsrp",
        md."LTE_6_rsrp",
        md."LTE_7_rsrp",
        md."LTE_8_rsrp",
        md."LTE_9_rsrp",
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
        md."LTE_0_rsrp" IS NOT NULL
)

SELECT 
    gp."appId",
    gp.ts,
    ST_X(ST_Transform(gp.geom, 3035)) AS x,
    ST_Y(ST_Transform(gp.geom, 3035)) AS y,
    gp."DIRECT_connection_mcc_mnc",
    gp."LTE_0_rsrp",
    gp."LTE_1_rsrp",
    gp."LTE_2_rsrp",
    gp."LTE_3_rsrp",
    gp."LTE_4_rsrp",
    gp."LTE_5_rsrp",
    gp."LTE_6_rsrp",
    gp."LTE_7_rsrp",
    gp."LTE_8_rsrp",
    gp."LTE_9_rsrp",
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