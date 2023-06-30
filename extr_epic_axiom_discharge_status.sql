/*

Extract name: DischargeStatus

Date: 12/10/2020

BID: Scott Bentler

Extract Description: Epic Clarity extract for Axiom DischargeStatus Reference table

data load

Maintenance:

MM/DD/CCYY OPID Change# Assoc Ticket# Change Description

---------- ------ ---------- ------------- -------------------------------------------
06/18/2023 NTK13C N/A        N/A           Conversion to Snowflake
*/
select ps.PAT_STATUS_C as DischargeStatus ,ps.ABBR as Description ,ps.NAME as LongDescription ,ps.INTERNAL_ID as DischargeStatus_Internal_ID 
from clarity.ZC_MC_PAT_STATUS ps
