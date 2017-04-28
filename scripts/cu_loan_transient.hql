CREATE TABLE cu_data_transient.cu_loan_application_transient(account_id string,application_id  string,term_months int,first_name string,last_name string,address string,state string,phone string,type string,origination_date date,
loan_decision_type string,decided string,loan_approved double,denial_reason string,loan_account_status string,
approved string,payment_method string,requested_amount double,funded double,funded_date string,rate int,
ecoagroup string,officer_id string,denied_date string,insert_date string)
ROW FORMAT delimited fields terminated by '|'
STORED AS TEXTFILE 
LOCATION '${input}';