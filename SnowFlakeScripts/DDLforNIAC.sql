--creating database
create or replace database orif1;
--use that database and needed roles and dw
USE WAREHOUSE COMPUTE_WH;  
USE Database orif1;
USE orif1.public;
USE ROLE ACCOUNTADMIN;


--create integration storage for external s3 bucket
create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::042999375240:role/snowpipe-niac-role'
  storage_allowed_locations = ('s3://snowpipe-niac-files/');
  
---describe integration s3_int;

--create table niac

  CREATE OR REPLACE TABLE niactable
( 
DS Char(50),
VERSION CHAR(20),
DATE	Date,
TIME    Time,
--DOMAIN Char(5), -- No longer needed.
NAICS_CODE	CHAR(10),
NAICS_DESCRIPTION  CHAR(200),
NAICS_SUBSECTOR_CODE CHAR(10),
NAICS_SUBSECTOR_DESCRIPTION  CHAR(200),
NAICS_GROUP_CODE CHAR(10),
NAICS_GROUP_DESCRIPTION	 CHAR(200)

); 
  

--creating stage for external file   
create or replace stage niacstage
  url = 's3://snowpipe-niac-files/'
  storage_integration = s3_int;
  
--creating needed file format for niac files 
CREATE OR REPLACE FILE FORMAT CSV_COMMA_NIAC TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N') ENCODING = 'ISO-8859-1';-- | UTF8;
    

-- LOAD NAICS Records ------------------------------
create or replace pipe niacpipe auto_ingest=true as  
COPY INTO niactable FROM
(
SELECT 
METADATA$FILENAME, 
'V001-FULL', --POC Full. Eventual update 
CURRENT_DATE, 
CURRENT_TIME, 
--Substr(METADATA$FILENAME,8,5), --No longer needed.
($1) 	AS	NAICS_CODE,
($2) 	AS	NAICS_DESCRIPTION,
($3) 	AS	NAICS_SUBSECTOR_CODE,
($4) 	AS	NAICS_SUBSECTOR_DESCRIPTION	,
($5) 	AS	NAICS_GROUP_CODE,
($6) 	AS	NAICS_GROUP_DESCRIPTION	
    
FROM @orif1.public.niacstage ) 
file_format='CSV_COMMA_NIAC';   

--describe pipe niacpipe;

---select system$pipe_status('orif1.public.niacpipe');


--select * from niactable;