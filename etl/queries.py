work_orders = """
SELECT w.WONUM,
       w.DIM_ASSET_SK,
       w.DIM_WORK_LOC_SK,
       w.PARENT,
       w.HASCHILDREN,
       w.STATUS,
       w.WORKTYPE,
       w.DESCRIPTION,
       w.ASSETNUM,
       w.LOCATION,
       w.REPORTDATE,
       w.STATUSDATE,
       w.FAILDATE,
       w.CHANGEDATE,
       w.TARGCOMPDATE,
       w.ACTSTART,
       w.ACTFINISH,
       w.SCHEDSTART,
       w.SCHEDFINISH,
       w.COA_CREATEDATE,
       w.SNE_CONSTRAINT,
       w.RESPONDBY,
       w.ESTDUR,
       w.ESTLABHRS,
       w.ESTMATCOST,
       w.ESTLABCOST,
       w.ESTTOOLCOST,
       w.ACTLABHRS,
       w.ACTMATCOST,
       w.ACTLABCOST,
       w.ACTTOOLCOST,
       w.OUTLABCOST,
       w.OUTMATCOST,
       w.OUTTOOLCOST,
       w.WOPRIORITY,
       lkp.DESCRIPTION AS PROBLEMCODE,
       lkf.DESCRIPTION AS FAILURECODE,
       w.HASFOLLOWUPWORK,
       w.SENDERSYSID,
       w.ORGID,
       w.SITEID,
       w.WOCLASS,
       w.ONBEHALFOF,
       w.ORIGRECORDCLASS,
       w.ORIGRECORDID,
       w.CLASSSTRUCTUREID,
       w.WORKORDERID,
       w.PERSONGROUP,
       w.CREWID,
       w.ACTINTLABCOST,
       w.ACTINTLABHRS,
       w.ASSIGNEDOWNERGROUP,
       w.CSR311NUMBER,
       w.ROW_PERMIT,
       w.ROW_EXPDATE,
       l.NUMVALUE      AS CTM_SEGMENT_ID,
       w.DIM_OWNER_PERSON_SK, 
       w.SUPERINTENDENT,
       w.REPORTEDBY,
       w.DIM_LEAD_PERSON_SK,
       w.HOLDREASON,
       cc.DESCRIPTION  AS CAUSE,
       rc.DESCRIPTION  AS REMEDY,
       w.ALTREF,
       '{base_url}' || w.WONUM as WO_LINK
FROM MAXIMO_DM.FCT_WORKORDER w
         LEFT OUTER JOIN (SELECT *
                          FROM MAXIMO_DM.DIM_LOCATIONSPEC
                          WHERE ASSETATTRID = 'GIS_SEGMENT_ID') l ON w.LOCATION = l.LOCATION
         LEFT JOIN (SELECT fr.WONUM,
                           fr.FAILURECODE,
                           lkp.DESCRIPTION
                    FROM MAXIMO_DM.LKP_FAILUREREPORT fr
                             LEFT JOIN MAXIMO_DM.LKP_FAILURECODE lkp ON lkp.FAILURECODE = fr.FAILURECODE
                    WHERE TYPE = 'CAUSE') cc ON cc.WONUM = w.WONUM
         LEFT JOIN (SELECT fr.WONUM,
                           fr.FAILURECODE,
                           lkp.DESCRIPTION
                    FROM MAXIMO_DM.LKP_FAILUREREPORT fr
                             LEFT JOIN MAXIMO_DM.LKP_FAILURECODE lkp ON lkp.FAILURECODE = fr.FAILURECODE
                    WHERE TYPE = 'REMEDY') rc ON rc.WONUM = w.WONUM
         LEFT JOIN MAXIMO_DM.LKP_FAILURECODE lkp ON lkp.FAILURECODE = w.PROBLEMCODE
         LEFT JOIN MAXIMO_DM.LKP_FAILURECODE lkf ON lkf.FAILURECODE = w.FAILURECODE
WHERE w.SITEID = 'SBO'
  AND w.CHANGEDATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY')
    AND TO_DATE('{end}', 'MM/DD/YYYY')
	"""

service_requests = """
SELECT DIM_TICKET_ID,
       TICKETID,
       STATUS,
       STATUSDATE,
       REPORTDATE,
       ACTUALFINISH
from MAXIMO_DM.FCT_TICKET
WHERE CLASS = 'SR'
  AND SITEID = 'SBO'
  AND CHANGEDATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY')
    AND TO_DATE('{end}', 'MM/DD/YYYY')
"""

work_order_status_history = """
SELECT
    WOSTATUSID,
    WONUM,
    STATUS,
    CHANGEDATE,
    CHANGEBY
FROM MAXIMO_DM.LKP_WOSTATUS
WHERE
    SITEID='SBO' AND
    CHANGEDATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY')
    AND TO_DATE('{end}', 'MM/DD/YYYY')
"""

work_order_time_logs = """
SELECT
    LABTRANSID,
    REFWO as WONUM,
    CRAFT,
    TO_CHAR(TRUNC(STARTDATE) + (STARTTIME - TRUNC(STARTTIME)), 'YYYY-MM-DD"T"HH24:MI:SS') AS STARTDATETIME,
    TO_CHAR(TRUNC(FINISHDATE) + (FINISHTIME - TRUNC(FINISHTIME)), 'YYYY-MM-DD"T"HH24:MI:SS') AS FINISHDATETIME,
    REGULARHRS,
    ENTERDATE
FROM MAXIMO_DM.LKP_LABTRANS
WHERE
    SITEID='SBO' AND
    ENTERDATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY')
    AND TO_DATE('{end}', 'MM/DD/YYYY')
"""

work_order_materials = """
SELECT
    MATUSETRANSID,
    REFWO as WONUM,
    ITEMNUM,
    DESCRIPTION,
    ISSUETYPE,
    STORELOC,
    QUANTITY,
    SHIPTO as UNIT,
    LINECOST,
    ACTUALDATE
FROM MAXIMO_DM.DIM_MATUSETRANS
WHERE SITEID = 'SBO' AND
    TRANSDATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY')
    AND TO_DATE('{end}', 'MM/DD/YYYY')
"""

work_order_specifications = """
SELECT 
    spec.WORKORDERSPECID,
    spec.WONUM,
    spec.ASSETATTRID,
    lkp.DESCRIPTION,
    lkp.DATATYPE,
    lkp.DOMAINID,
    spec.ALNVALUE,
    spec.NUMVALUE,
    spec.MEASUREUNITID,
    spec.CHANGEDATE,
    spec.CLASSSTRUCTUREID
FROM MAXIMO_DM.DIM_WORKORDERSPEC spec
         LEFT JOIN (SELECT * FROM MAXIMO_DM.DIM_ASSETATTRIBUTE WHERE SITEID = 'SBO') lkp
                   ON lkp.ASSETATTRID = spec.ASSETATTRID
WHERE spec.SITEID = 'SBO'
  AND (spec.NUMVALUE IS NOT NULL OR spec.ALNVALUE IS NOT NULL) AND
    CHANGEDATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY')
    AND TO_DATE('{end}', 'MM/DD/YYYY')
"""

locations = """
SELECT LOCATION,
       CLASSSTRUCTUREID,
       ALNVALUE AS ADDRESS_DESCRIPTION
FROM MAXIMO_DM.DIM_LOCATIONSPEC
WHERE ASSETATTRID = 'ADDRESS_DESCRIPTION'
  AND SITEID = 'SBO'
  AND CHANGEDATE BETWEEN TO_DATE('{start}', 'MM/DD/YYYY') AND TO_DATE('{end}', 'MM/DD/YYYY')
"""

maximo_url_search_params = (
    "event=loadapp&value=sbo_wotrk&additionalevent=useqbe&additionaleventvalue=wonum="
)

QUERIES = {
    "work_orders": {
        "template": work_orders,
        "query_params": ["start", "end", "base_url"],
        "dataset_resource_id": "hjym-dxqr",
    },
    "service_requests": {
        "template": service_requests,
        "query_params": ["start", "end"],
        "dataset_resource_id": "2zms-x3x7",
    },
    "work_order_status_history": {
        "template": work_order_status_history,
        "query_params": ["start", "end"],
        "dataset_resource_id": "dbbh-gygn",
    },
    "work_order_time_logs": {
        "template": work_order_time_logs,
        "query_params": ["start", "end"],
        "dataset_resource_id": "gsg2-nuh6",
    },
    "work_order_materials": {
        "template": work_order_materials,
        "query_params": ["start", "end"],
        "dataset_resource_id": "gr65-gj74",
    },
    "work_order_specifications": {
        "template": work_order_specifications,
        "query_params": ["start", "end"],
        "dataset_resource_id": "nvm3-3kju",
    },
    "locations": {
        "template": locations,
        "query_params": ["start", "end"],
        "dataset_resource_id": "69pc-wtji",
    },
}
