SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

CREATE DATABASE IF NOT EXISTS RAW;
use RAW;

DROP table RAW.pg_outpatEx;

create EXTERNAL table RAW.pg_outpatEx (ClaimID VARCHAR(20),BeneID VARCHAR(20),ClaimStartDt VARCHAR(20), ClaimEndDt VARCHAR(20),Provider VARCHAR(20),InscClaimAmtReimbursed int,AttendingPhysician VARCHAR(20),OperatingPhysician VARCHAR(20),OtherPhysician VARCHAR(20),ClmDiagnosisCode_1 VARCHAR(20), ClmDiagnosisCode_2 VARCHAR(20),ClmDiagnosisCode_3 VARCHAR(20),ClmDiagnosisCode_4 VARCHAR(20), ClmDiagnosisCode_5 VARCHAR(20),ClmDiagnosisCode_6 VARCHAR(20),ClmDiagnosisCode_7 VARCHAR(20), ClmDiagnosisCode_8 VARCHAR(20),ClmProcedureCode_1 VARCHAR(20),ClmProcedureCode_2 VARCHAR(20),ClmProcedureCode_3 VARCHAR(20),DeductibleAmtPaid INT,ClmAdmitDiagnosisCode VARCHAR(20))

row format delimited

fields terminated by ','

LOCATION '/usr/hadoop/spark_pg/outpatient';

CREATE DATABASE IF NOT EXISTS DSL;
use DSL;

DROP table DSL.pg_outpat;

create table DSL.pg_outpat (ClaimID VARCHAR(20),BeneID VARCHAR(20),ClaimStartDt VARCHAR(20), ClaimEndDt VARCHAR(20),Provider VARCHAR(20),InscClaimAmtReimbursed int,AttendingPhysician VARCHAR(20),OperatingPhysician VARCHAR(20),OtherPhysician VARCHAR(20),ClmDiagnosisCode_1 VARCHAR(20), ClmDiagnosisCode_2 VARCHAR(20),ClmDiagnosisCode_3 VARCHAR(20),ClmDiagnosisCode_4 VARCHAR(20), ClmDiagnosisCode_5 VARCHAR(20),ClmDiagnosisCode_6 VARCHAR(20),ClmDiagnosisCode_7 VARCHAR(20), ClmDiagnosisCode_8 VARCHAR(20),ClmProcedureCode_1 VARCHAR(20),ClmProcedureCode_2 VARCHAR(20),ClmProcedureCode_3 VARCHAR(20),DeductibleAmtPaid INT,ClmAdmitDiagnosisCode VARCHAR(20))

row format delimited

fields terminated by ',';

insert overwrite table DSL.pg_outpat select * from RAW.pg_outpatEx;

--------------------------------------------------------------------------------

use RAW;

DROP table RAW.pg_inpatEx;

create EXTERNAL table RAW.pg_inpatEx (ClaimID VARCHAR(20), BeneID VARCHAR(20), ClaimStartDt VARCHAR(20),ClaimEndDt VARCHAR(20),Provider VARCHAR(20),InscClaimAmtReimbursed VARCHAR(20),AttendingPhysician VARCHAR(20),OperatingPhysician VARCHAR(20),OtherPhysician VARCHAR(20),AdmissionDt VARCHAR(20),ClmAdmitDiagnosisCode VARCHAR(20),DeductibleAmtPaid VARCHAR(20),DischargeDt VARCHAR(20),DiagnosisGroupCode VARCHAR(20),ClmDiagnosisCode_1 VARCHAR(20),ClmDiagnosisCode_2 VARCHAR(20),ClmDiagnosisCode_3 VARCHAR(20),ClmDiagnosisCode_4 VARCHAR(20), ClmDiagnosisCode_5 VARCHAR(20),ClmDiagnosisCode_6 VARCHAR(20),ClmDiagnosisCode_7 VARCHAR(20), ClmDiagnosisCode_8 VARCHAR(20),ClmProcedureCode_1 VARCHAR(20),ClmProcedureCode_2 VARCHAR(20),ClmProcedureCode_3 VARCHAR(20))

row format delimited

fields terminated by ','

LOCATION '/usr/hadoop/spark_pg/inpatient';

--------------------------------------------------------------------------------------------------------

use DSL;

DROP table DSL.pg_inpat;

create table DSL.pg_inpat (ClaimID VARCHAR(20), BeneID VARCHAR(20), ClaimStartDt VARCHAR(20),ClaimEndDt VARCHAR(20),Provider VARCHAR(20),InscClaimAmtReimbursed VARCHAR(20),AttendingPhysician VARCHAR(20),OperatingPhysician VARCHAR(20),OtherPhysician VARCHAR(20),AdmissionDt VARCHAR(20),ClmAdmitDiagnosisCode VARCHAR(20),DeductibleAmtPaid VARCHAR(20),DischargeDt VARCHAR(20),DiagnosisGroupCode VARCHAR(20),ClmDiagnosisCode_1 VARCHAR(20),ClmDiagnosisCode_2 VARCHAR(20),ClmDiagnosisCode_3 VARCHAR(20),ClmDiagnosisCode_4 VARCHAR(20), ClmDiagnosisCode_5 VARCHAR(20),ClmDiagnosisCode_6 VARCHAR(20),ClmDiagnosisCode_7 VARCHAR(20), ClmDiagnosisCode_8 VARCHAR(20),ClmProcedureCode_1 VARCHAR(20),ClmProcedureCode_2 VARCHAR(20),ClmProcedureCode_3 VARCHAR(20))

row format delimited

fields terminated by ',';

insert overwrite table DSL.pg_inpat select * from RAW.pg_inpatEx;

-----------------------------------------------------------------------------------------------------------------------

use RAW;

DROP table RAW.pg_beneEx;

create EXTERNAL table RAW.pg_beneEx (BeneID VARCHAR(20),DOB VARCHAR(20),DOD VARCHAR(20), Gender VARCHAR(20),Race VARCHAR(20),RenalDiseaseIndicator VARCHAR(20),State VARCHAR(10), County VARCHAR(20),NoOfMonths_PartACov INT, NoOfMonths_PartBCov INT,ChronicCond_Alzheimer VARCHAR(10), ChronicCond_Heartfailure VARCHAR(10), ChronicCond_KidneyDisease VARCHAR(10), ChronicCond_Cancer VARCHAR(10), ChronicCond_ObstrPulmonary VARCHAR(10),ChronicCond_Depression VARCHAR(10),ChronicCond_Diabetes VARCHAR(10),ChronicCond_IschemicHeart VARCHAR(10),ChronicCond_Osteoporasis VARCHAR(10),ChronicCond_rheumatoidarthritis VARCHAR(10),ChronicCond_stroke VARCHAR(10),IPAnnualReimbursementAmt INT,IPAnnualDeductibleAmt INT,OPAnnualReimbursementAmt INT,OPAnnualDeductibleAmt INT)

row format delimited

fields terminated by ','

LOCATION '/usr/hadoop/spark_pg/benefciary';

-------------------------------------------------------------------------------------------------------------

use DSL;

DROP table DSL.pg_bene;

create table DSL.pg_bene(BeneID VARCHAR(20),DOB VARCHAR(20),DOD VARCHAR(20), Gender VARCHAR(20),Race VARCHAR(20),RenalDiseaseIndicator VARCHAR(20),State VARCHAR(10), County VARCHAR(20),NoOfMonths_PartACov INT, NoOfMonths_PartBCov INT,ChronicCond_Alzheimer VARCHAR(10), ChronicCond_Heartfailure VARCHAR(10), ChronicCond_KidneyDisease VARCHAR(10), ChronicCond_Cancer VARCHAR(10), ChronicCond_ObstrPulmonary VARCHAR(10),ChronicCond_Depression VARCHAR(10),ChronicCond_Diabetes VARCHAR(10),ChronicCond_IschemicHeart VARCHAR(10),ChronicCond_Osteoporasis VARCHAR(10),ChronicCond_rheumatoidarthritis VARCHAR(10),ChronicCond_stroke VARCHAR(10),IPAnnualReimbursementAmt INT,IPAnnualDeductibleAmt INT,OPAnnualReimbursementAmt INT,OPAnnualDeductibleAmt INT)

row format delimited

fields terminated by ',';

insert overwrite table DSL.pg_bene select * from RAW.pg_beneEx;

----------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS DSL_hbase;

use DSL_hbase;

DROP table hbhive_pg_outpat1;

create table hbhive_pg_outpat1(ClaimID VARCHAR(20),BeneID VARCHAR(20),ClaimStartDt VARCHAR(20), ClaimEndDt VARCHAR(20),Provider VARCHAR(20),InscClaimAmtReimbursed int,AttendingPhysician VARCHAR(20),OperatingPhysician VARCHAR(20),OtherPhysician VARCHAR(20),ClmDiagnosisCode_1 VARCHAR(20), ClmDiagnosisCode_2 VARCHAR(20),ClmDiagnosisCode_3 VARCHAR(20),ClmDiagnosisCode_4 VARCHAR(20), ClmDiagnosisCode_5 VARCHAR(20),ClmDiagnosisCode_6 VARCHAR(20),ClmDiagnosisCode_7 VARCHAR(20), ClmDiagnosisCode_8 VARCHAR(20),ClmProcedureCode_1 VARCHAR(20),ClmProcedureCode_2 VARCHAR(20),ClmProcedureCode_3 VARCHAR(20),DeductibleAmtPaid INT,ClmAdmitDiagnosisCode VARCHAR(20))

stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'

with serdeproperties("hbase.columns.mapping"=":key,clm:BeneID,clm:ClaimStartDt,clm:ClaimEndDt,clm:Provider,clm:AttendingPhysician,clm:OperatingPhysician,clm:OtherPhysician,clm:InscClaimAmtReimbursed,clm:DeductibleAmtPaid,dc:ClmDiagnosisCode_1,dc:ClmDiagnosisCode_2,dc:ClmDiagnosisCode_3,dc:ClmDiagnosisCode_4,dc:ClmDiagnosisCode_5,dc:ClmDiagnosisCode_6,dc:ClmDiagnosisCode_7,dc:ClmDiagnosisCode_8,dc:ClmAdmitDiagnosisCode,clm:ClmProcedureCode_1,clm:ClmProcedureCode_2,clm:ClmProcedureCode_3")

tblproperties("hbase.table.name"="hbhive_pg_outpat1");

insert overwrite table hbhive_pg_outpat1 select * from DSL.pg_outpat;

-----------------------------------------------------------------------------------------------------------------------

DROP table DSL_hbase.hbhive_pg_inpat;

create table DSL_hbase.hbhive_pg_inpat (ClaimID VARCHAR(20), BeneID VARCHAR(20), ClaimStartDt VARCHAR(20),ClaimEndDt VARCHAR(20),Provider VARCHAR(20),InscClaimAmtReimbursed VARCHAR(20),AttendingPhysician VARCHAR(20),OperatingPhysician VARCHAR(20),OtherPhysician VARCHAR(20),AdmissionDt VARCHAR(20),ClmAdmitDiagnosisCode VARCHAR(20),DeductibleAmtPaid VARCHAR(20),DischargeDt VARCHAR(20),DiagnosisGroupCode VARCHAR(20),ClmDiagnosisCode_1 VARCHAR(20),ClmDiagnosisCode_2 VARCHAR(20),ClmDiagnosisCode_3 VARCHAR(20),ClmDiagnosisCode_4 VARCHAR(20), ClmDiagnosisCode_5 VARCHAR(20),ClmDiagnosisCode_6 VARCHAR(20),ClmDiagnosisCode_7 VARCHAR(20), ClmDiagnosisCode_8 VARCHAR(20),ClmProcedureCode_1 VARCHAR(20),ClmProcedureCode_2 VARCHAR(20),ClmProcedureCode_3 VARCHAR(20))

stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'

with serdeproperties("hbase.columns.mapping"=":key,clm:BeneID,clm:ClaimStartDt,clm:ClaimEndDt,prov:Provider,prov:AttendingPhysician,prov:OperatingPhysician,prov:OtherPhysician,pay:InscClaimAmtReimbursed,pay:DeductibleAmtPaid,clm:AdmissionDt,clm:DischargeDt,dc:DiagnosisGroupCode,dc:ClmDiagnosisCode_1,dc:ClmDiagnosisCode_2,dc:ClmDiagnosisCode_3,dc:ClmDiagnosisCode_4,dc:ClmDiagnosisCode_5,dc:ClmDiagnosisCode_6,dc:ClmDiagnosisCode_7,dc:ClmDiagnosisCode_8,dc:ClmAdmitDiagnosisCode,pc:ClmProcedureCode_1,pc:ClmProcedureCode_2,pc:ClmProcedureCode_3")

tblproperties("hbase.table.name"="hbhive_pg_inpat1");

insert overwrite table DSL_hbase.hbhive_pg_inpat select * from DSL.pg_inpat;

----------------------------------------------------------------------------------------

DROP table DSL_hbase.hbhive_pg_bene;

create table DSL_hbase.hbhive_pg_bene(BeneID VARCHAR(20),DOB VARCHAR(20),DOD VARCHAR(20), Gender VARCHAR(20),Race VARCHAR(20),RenalDiseaseIndicator VARCHAR(20),State VARCHAR(10), County VARCHAR(20),NoOfMonths_PartACov INT, NoOfMonths_PartBCov INT,ChronicCond_Alzheimer VARCHAR(10), ChronicCond_Heartfailure VARCHAR(10), ChronicCond_KidneyDisease VARCHAR(10), ChronicCond_Cancer VARCHAR(10), ChronicCond_ObstrPulmonary VARCHAR(10),ChronicCond_Depression VARCHAR(10),ChronicCond_Diabetes VARCHAR(10),ChronicCond_IschemicHeart VARCHAR(10),ChronicCond_Osteoporasis VARCHAR(10),ChronicCond_rheumatoidarthritis VARCHAR(10),ChronicCond_stroke VARCHAR(10),IPAnnualReimbursementAmt INT,IPAnnualDeductibleAmt INT,OPAnnualReimbursementAmt INT,OPAnnualDeductibleAmt INT)

stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'

with serdeproperties("hbase.columns.mapping"=":key,per:DOB,per:DOD,per:Gender,per:Race,pc:RenalDiseaseIndicator,ad:State , ad:County,ins:NoOfMonths_PartACov, insu:NoOfMonths_PartBCov,pc:ChronicCond_Alzheimer, pc:ChronicCond_Heartfailure, pc:ChronicCond_KidneyDisease, pc:ChronicCond_Cancer, pc:ChronicCond_ObstrPulmonary,pc:ChronicCond_Depression,pc:ChronicCond_Diabetes,pc:ChronicCond_IschemicHeart,pc:ChronicCond_Osteoporasis,pc:ChronicCond_rheumatoidarthritis,pc:ChronicCond_stroke,pay:IPAnnualReimbursementAmt,pay:IPAnnualDeductibleAmt,pay:OPAnnualReimbursementAmt ,pay:OPAnnualDeductibleAmt")

tblproperties("hbase.table.name"="hbhive_pg_bene1");

insert overwrite table DSL_hbase.hbhive_pg_bene select * from DSL.pg_bene;
