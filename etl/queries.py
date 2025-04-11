query_template = """
SELECT w.WONUM,
       w.DIM_WORKORDER_ID,
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
       w.PROBLEMCODE,
       w.CALCPRIORITY,
       w.CHARGESTORE,
       w.FAILURECODE,
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
       w.WOGROUP,
       w.ACTINTLABCOST,
       w.ACTINTLABHRS,
       w.ASSIGNEDOWNERGROUP,
       w.ROWSTAMP,
       w.CSR311NUMBER,
       w.ROW_PERMIT,
       w.ROW_EXPDATE,
       l.NUMVALUE AS CTM_SEGMENT_ID
FROM MAXIMO_DM.FCT_WORKORDER w
         LEFT OUTER JOIN (SELECT * from MAXIMO_DM.DIM_LOCATIONSPEC where ASSETATTRID = 'GIS_SEGMENT_ID') l
                         ON w.LOCATION = l.LOCATION
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
