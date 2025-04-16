query_template = """
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

maximo_url_search_params = "event=loadapp&value=sbo_wotrk&additionalevent=useqbe&additionaleventvalue=wonum="
