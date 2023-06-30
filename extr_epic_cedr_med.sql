/*****************************************************************************************************************************
Extract Name: Cedr Medication Extract
Date        : 01/03/2022
BID         : Annetta Hoshor
Description : Pull all emergency vist medications.
Object Name : ah_cedr_meds_bigdata

Parameters  : FacCode, Facility

File Format : Pipe Delimited .txt
Frequency   : 15th of every month.  Pulls data from previous month.

Maintenance :  
MM/DD/CCYY OPID     CHANGE#    Assoc Ticket# Change Description
---------- -------- ---------- ------------- ---------------------------------------------------------------------------------
06/05/2023 BRC77C    N/A        N/A          Conversion to Snowflake and add in column to parse stage results
11/15/2022 BRC77C   N/A        N/A           Update om.order_class_c field to be a string.
05/03/2022 ARHE2B   N/A        N/A           Add order class of 'Historical Med' for recorded home meds.
04/13/2022 ARHE2B   N/A        N/A           Cast all strings to varchar(length)
03/30/2022 ARHE2B   N/A        N/A           Use ept_csn to pull csn. Pull by encounterFact instead of EdVisitFact
01/03/2022 ARHE2B   N/A        N/A           Initial Release

*****************************************************************************************************************************/

SELECT 
/*Administered medications for ED patients*/

pat.primarymrn as Pat_Id
,RN.RAW_11_DIGIT_NDC as Medication_Code
,CM.NAME as Medication_Name
,to_varchar(OM.ORDER_START_TIME,'MM/dd/yyyy HH:mm') as Med_Start_DTTM
,to_varchar(OM.order_end_time,'MM/dd/yyyy HH:mm')  as Med_Stop_DTTM
,to_varchar(MAI.TAKEN_TIME,'MM/dd/yyyy HH:mm')  as Admin_Effective_DTTM
,ZAR.NAME as Med_Route_Code
,ZAR.NAME as Med_Route_Text
,to_varchar(round(OM.MIN_DISCRETE_DOSE,2)) as Dose_Qty
,ZMU.NAME as Dose_Unit_Text
,ISI.IDENTITY_ID as Provider_NPI
,'NORMAL ORDER'  as Med_Source
,to_varchar(mai.saved_time ,'MM/dd/yyyy HH:mm')  as Document_Date
,EC.CSN as Visit_Key
,dept.locationEpicId 
,case       when dept.locationEpicId = '5133781'  then '431013'
            when dept.locationEpicId = '5133790'  then '431002'
            when dept.locationEpicId = '5133797'  then '431004'
            when dept.locationEpicId = '5134438'  then '431018'
            when dept.locationEpicId = '5133776'  then '431022'
            when dept.locationEpicId = '5133801'  then '642583'
            when dept.locationEpicId = '51315795' then '1003605'
            when dept.locationEpicId = '5134361'  then '415767'
            else 'XXXX'
 end as cedr_id  
from caboodle.encounterFact enc_fct 
  inner join caboodle.categorydim cd on cd.categorykey = enc_fct.patientclasscategorykey
  inner join caboodle.departmentdim dept on dept.departmentkey = enc_fct.departmentkey
  inner join caboodle.patientdim pat on pat.durablekey = enc_fct.patientdurablekey
  INNER JOIN clarity.ept_csn ec ON enc_fct.encounterepiccsn = ec.uci
  INNER JOIN CLARITY.MAR_ADMIN_INFO MAI ON enc_fct.encounterepiccsn = MAI.mar_enc_csn
	  AND MAI.MAR_ACTION_C IN ('1','6','13','9509')
	  /*given,newbag,push,given during downtime*/
  INNER JOIN CLARITY.ORDER_MED OM ON OM.ORDER_MED_ID = MAI.ORDER_MED_ID
  INNER JOIN CLARITY.CLARITY_MEDICATION CM
		ON om.medication_id = CM.MEDICATION_ID
  INNER JOIN CLARITY.ORDER_DISP_MEDS ODM
		ON ODM.ORDER_MED_ID = MAI.ORDER_MED_ID
  INNER JOIN CLARITY.RX_NDC_STATUS RNS
		ON ODM.DISP_NDC_CSN = RNS.CNCT_SERIAL_NUM
  INNER JOIN CLARITY.RX_NDC RN
		ON RNS.NDC_ID = RN.NDC_ID
  INNER JOIN CLARITY.IDENTITY_SER_ID ISI
		ON OM.AUTHRZING_PROV_ID = ISI.PROV_ID
		AND ISI.IDENTITY_TYPE_ID = 95052 
  INNER JOIN CLARITY.ZC_ADMIN_ROUTE ZAR
		ON ZAR.MED_ROUTE_C = OM.MED_ROUTE_C
  INNER JOIN CLARITY.ZC_MED_UNIT ZMU
		ON ZMU.DISP_QTYUNIT_C = OM.DOSE_UNIT_C
    
 WHERE 
    enc_fct.dischargeinstant >= last_day(current_date - interval '2 month') + interval '1 day' 
    and enc_fct.dischargeinstant < date_trunc('month', current_date)
    and dept.locationEpicId in ('5133781', '5133790','5133797','5134438','5133776','5133801','51315795','5134361')
    and cd.epiccategoryvalueid = '9504'
    and pat.isValid = 1
    and pat.iscurrent = 1

		 
UNION ALL

/*Prescription Discharge order for ED patients*/
SELECT pat.primarymrn as Pat_Id
, ' ' as Medication_Code /*prescriptions do not have NDCs*/
, CM.NAME as Medication_Name
,to_varchar(OM.ORDER_START_TIME,'MM/dd/yyyy HH:mm') as Med_Start_DTTM
,to_varchar(OM.order_end_time,'MM/dd/yyyy HH:mm')  as Med_Stop_DTTM
,' '  as Admin_Effective_DTTM
,ZAR.NAME as Med_Route_Code
,ZAR.NAME as Med_Route_Text
,to_varchar(OM.HV_DISCRETE_DOSE) as Dose_Qty
,ZMU.NAME as Dose_Unit_Text
,ISI.IDENTITY_ID as Provider_NPI
,'PRESCRIPTION DISCHARGE ORDER' as Med_Source
,to_varchar(om2.RX_WRITTEN_DATE,'MM/dd/yyyy HH:mm') Document_Date
,EC.CSN as Visit_Key
,dept.locationEpicId
,case       when dept.locationEpicId = '5133781'  then '431013'
            when dept.locationEpicId = '5133790'  then '431002'
            when dept.locationEpicId = '5133797'  then '431004'
            when dept.locationEpicId = '5134438'  then '431018'
            when dept.locationEpicId = '5133776'  then '431022'
            when dept.locationEpicId = '5133801'  then '642583'
            when dept.locationEpicId = '51315795' then '1003605'
            when dept.locationEpicId = '5134361'  then '415767'
            else 'XXXX'
 end as cedr_id 

from caboodle.encounterFact enc_fct 
  inner join caboodle.categorydim cd on cd.categorykey = enc_fct.patientclasscategorykey
  inner join caboodle.departmentdim dept on dept.departmentkey = enc_fct.departmentkey
  inner join caboodle.patientdim pat on pat.durablekey = enc_fct.patientdurablekey
  INNER JOIN clarity.ept_csn ec ON enc_fct.encounterepiccsn = ec.uci
	INNER JOIN CLARITY.ORDER_MED OM ON enc_fct.encounterepiccsn = OM.PAT_ENC_CSN_ID
    and om.ordering_mode_c = 1 /*Outpatient mode*/
	INNER JOIN CLARITY.ORDER_MED_2 OM2 ON OM2.ORDER_ID = OM.ORDER_MED_ID
		and om2.RX_WRITTEN_DATE is not null
	INNER JOIN CLARITY.CLARITY_MEDICATION CM ON OM.MEDICATION_ID = CM.MEDICATION_ID
	left JOIN CLARITY.ORDER_DISP_MEDS ODM ON ODM.ORDER_MED_ID = OM.ORDER_MED_ID
	left JOIN CLARITY.RX_NDC_STATUS RNS ON ODM.DISP_NDC_CSN = RNS.CNCT_SERIAL_NUM
	left JOIN CLARITY.RX_NDC RN ON RNS.NDC_ID = RN.NDC_ID
	INNER JOIN CLARITY.IDENTITY_SER_ID ISI ON OM.AUTHRZING_PROV_ID = ISI.PROV_ID
		AND ISI.IDENTITY_TYPE_ID = 95052 
	INNER JOIN CLARITY.ZC_ADMIN_ROUTE ZAR ON ZAR.MED_ROUTE_C = OM.MED_ROUTE_C
	INNER JOIN CLARITY.ZC_MED_UNIT ZMU ON ZMU.DISP_QTYUNIT_C = OM.HV_DOSE_UNIT_C

 WHERE 
    enc_fct.dischargeinstant >= last_day(current_date - interval '2 month') + interval '1 day' 
    and enc_fct.dischargeinstant < date_trunc('month', current_date)
 /*enc_fct.dischargeInstant >= date_trunc('day',from_timestamp(date_sub(months_add(now(),-1), interval (day(now())-1) day),'yyyy-MM-dd HH:mm:ss'))
    and enc_fct.dischargeInstant < date_trunc('day',from_timestamp(date_sub(now(), interval (day(now())-1) day),'yyyy-MM-dd HH:mm:ss'))
    and dept.locationEpicId in ('${facility}')*/
    and dept.locationEpicId in ('5133781', '5133790','5133797','5134438','5133776','5133801','51315795','5134361')
    and cd.epiccategoryvalueid = '9504'
    and pat.isValid = 1
    and pat.iscurrent = 1
		

UNION ALL


  /*Recorded Home Med for ED patients*/
   SELECT pat.primarymrn as Pat_Id
   ,' '  as Medication_Code /*home meds do not have NDCs*/
   ,CM.NAME as Medication_Name
   ,to_varchar(OM.START_DATE,'MM/dd/yyyy HH:mm') as Med_Start_DTTM
   ,to_varchar(OM.END_DATE,'MM/dd/yyyy HH:mm') as Med_Stop_DTTM
   ,' ' as Admin_Effective_DTTM
   ,ZAR.NAME as Med_Route_Code
   ,ZAR.NAME as Med_Route_Text
   ,to_varchar(OM.HV_DISCRETE_DOSE) as Dose_Qty
   ,ZMU.NAME as Dose_Unit_Text
   ,ISI.IDENTITY_ID as Provider_NPI
   ,'RECORDED HOME MEDICATION' as Med_Source
   ,to_varchar(OM.ORDERING_DATE,'MM/dd/yyyy HH:mm') as Document_Date
   ,EC.CSN as Visit_Key
   ,dept.locationEpicId
   ,case       when dept.locationEpicId = '5133781'  then '431013'
            when dept.locationEpicId = '5133790'  then '431002'
            when dept.locationEpicId = '5133797'  then '431004'
            when dept.locationEpicId = '5134438'  then '431018'
            when dept.locationEpicId = '5133776'  then '431022'
            when dept.locationEpicId = '5133801'  then '642583'
            when dept.locationEpicId = '51315795' then '1003605'
            when dept.locationEpicId = '5134361'  then '415767'
            else 'XXXX'
 end as cedr_id 
     
  from caboodle.encounterFact enc_fct 
  inner join caboodle.categorydim cd on cd.categorykey = enc_fct.patientclasscategorykey
  inner join caboodle.departmentdim dept on dept.departmentkey = enc_fct.departmentkey
  inner join caboodle.patientdim pat on pat.durablekey = enc_fct.patientdurablekey
  INNER JOIN clarity.ept_csn ec ON enc_fct.encounterepiccsn = ec.uci
	INNER JOIN CLARITY.ORDER_MED OM
		ON pat.patientepicid = OM.pat_id
    and om.order_class_c = '9503' /*historical med*/
    and om.act_order_c = 1 /*active order*/
	INNER JOIN CLARITY.CLARITY_MEDICATION CM
		ON OM.MEDICATION_ID = CM.MEDICATION_ID
	left outer JOIN CLARITY.IDENTITY_SER_ID ISI
		ON OM.AUTHRZING_PROV_ID = ISI.PROV_ID
		AND ISI.IDENTITY_TYPE_ID = 95052 
	left outer JOIN CLARITY.ZC_ADMIN_ROUTE ZAR
		ON ZAR.MED_ROUTE_C = OM.MED_ROUTE_C
	left outer JOIN CLARITY.ZC_MED_UNIT ZMU
		ON ZMU.DISP_QTYUNIT_C = OM.HV_DOSE_UNIT_C
    

  WHERE 
    enc_fct.dischargeinstant >= last_day(current_date - interval '2 month') + interval '1 day' 
    and enc_fct.dischargeinstant < date_trunc('month', current_date)
    /*enc_fct.dischargeInstant >= date_trunc('day',from_timestamp(date_sub(months_add(now(),-1), interval (day(now())-1) day),'yyyy-MM-dd HH:mm:ss'))
    and enc_fct.dischargeInstant < date_trunc('day',from_timestamp(date_sub(now(), interval (day(now())-1) day),'yyyy-MM-dd HH:mm:ss'))*/
    /*and dept.locationEpicId in ('${facility}') */
    and dept.locationEpicId in ('5133781', '5133790','5133797','5134438','5133776','5133801','51315795','5134361')
    and cd.epiccategoryvalueid = '9504' /*ED*/
    and pat.isValid = 1
    and pat.iscurrent = 1
