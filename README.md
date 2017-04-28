"# Apache Falcon data pipeline with Apache Atlas Lineage" 

Step 1: Create raw database:
******************************
create database cu_data_raw;

Step 2: Loan Transactional Raw: Bucketed and Transactional table
*******************************************************************
CREATE TABLE cu_data_raw.loan_application_transactional_raw(
application_id  string,
term_months string,
first_name string,
last_name string,
address string,
state string,
phone string,
type string,
origination_date string,
loan_decision_type string,
decided string,
loan_approved string,
denial_reason string,
loan_account_status string,
approved string,
payment_method string,
requested_amount string,
funded string,
funded_date string,
rate string,
ecoagroup string,
officer_id string,
denied_date string
)
CLUSTERED BY (loan_decision_type) into 4 buckets
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY","transactional"="true");


Note: 
Load data into Hive transactional table we have used Storm and Kafka.

Refer our blog "Hive Streaming with Kafka and Storm with Atlas" - http://www.treselle.com/blog/hive-streaming-with-kafka-and-storm-with-atlas/ 


The other way is to create temporary table and then load data into partition table, 
we have did this for “cu_account_raw” table.


Step 3: Loan Transactional Raw: Partitioned table
*********************************************
CREATE TABLE cu_data_raw.cu_loan_application_raw
(
application_id  string,
term_months int,
first_name string,
last_name string,
address string,
state string,
phone string,
type string,
origination_date date,
loan_decision_type string,
decided string,
loan_approved double,
denial_reason string,
loan_account_status string,
approved string,
payment_method string,
requested_amount double,
funded double,
funded_date string,
rate int,
ecoagroup string,
officer_id string,
denied_date string
)
partitioned by (insert_date string)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

Step 4: Insert into partition table from transactional table
***********************************************************
INSERT INTO TABLE cu_data_raw.cu_loan_application_raw  
PARTITION (insert_date)
SELECT REGEXP_REPLACE(application_id,'"',''),
CAST (REGEXP_REPLACE(term_months,'"','') AS int),REGEXP_REPLACE(first_name,'"',''),
REGEXP_REPLACE(last_name,'"',''),REGEXP_REPLACE(address,'"',''),REGEXP_REPLACE(state,'"',''),
REGEXP_REPLACE(phone,'"',''),REGEXP_REPLACE(type,'"',''),
TO_DATE(from_unixtime(UNIX_TIMESTAMP(REGEXP_REPLACE(origination_date,'"',''), 'MM/dd/yyyy'))),
REGEXP_REPLACE(loan_decision_type,'"',''),REGEXP_REPLACE(decided,'"',''),
CAST (REGEXP_REPLACE(loan_approved,'"','') AS double),REGEXP_REPLACE(denial_reason,'"',''),
REGEXP_REPLACE(loan_account_status,'"',''),REGEXP_REPLACE(approved,'"',''),REGEXP_REPLACE(payment_method,'"',''),
CAST (REGEXP_REPLACE(requested_amount,'"','') AS double),
CAST (REGEXP_REPLACE(funded,'"','') AS double),
REGEXP_REPLACE(funded_date,'"',''),
CAST (REGEXP_REPLACE(rate,'"','') AS int),REGEXP_REPLACE(ecoagroup,'"',''),
REGEXP_REPLACE(officer_id,'"',''),
REGEXP_REPLACE(denied_date,'"',''),current_date as insert_date FROM cu_data_raw.loan_application_transactional_raw;


Step 5: Create Account Raw tables
************************************
CREATE TABLE cu_account_raw_temp
(
account_id STRING COMMENT "ID of account",
application_id STRING COMMENT "ID of applicant",
type STRING COMMENT "Type of account",
open_date STRING COMMENT "Account opened date",
account_nick_name STRING COMMENT "Applicant nick name",
actual_balance STRING COMMENT "Actual balance in account",
available_balance INT COMMENT "Available balance in account",
minimum_balance STRING COMMENT "Minimum balance of account"
)
COMMENT 'This table contains the account details of the applicant'
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|"
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/tsldp/account_data/account_data.txt' INTO TABLE cu_account_raw_temp;

Note: Make sure accout data file exist in that location.

CREATE TABLE cu_account_raw
(
account_id STRING COMMENT "ID of account",
application_id STRING COMMENT "ID of applicant",
type STRING COMMENT "Type of account",
open_date DATE COMMENT "Account opened date",
account_nick_name STRING COMMENT "Applicant nick name",
actual_balance STRING COMMENT "Actual balance in account",
available_balance INT COMMENT "Available balance in account",
minimum_balance STRING COMMENT "Minimum balance of account"
)
COMMENT 'This table contains the account details of the applicant'
partitioned by (insert_date string)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT INTO account_raw
***************************
INSERT INTO TABLE cu_data_raw.cu_account_raw  
PARTITION (insert_date)
SELECT account_id,
application_id,type,
TO_DATE(from_unixtime(UNIX_TIMESTAMP(REGEXP_REPLACE(open_date,'"',''), 'MM/dd/yyyy'))),account_nick_name,actual_balance,
available_balance,minimum_balance,
current_date as insert_date FROM cu_data_raw.cu_account_raw_temp;


Step 6: Create transient database
**********************************
create database cu_data_transient;


Reference:
Data files are available in "datasets" folder.
Falcon data pipeline feed and process XML files are available in "falcon_xml" folder.

Visit http://www.treselle.com/blog/ to find more use case in big data, technology integration and QA.