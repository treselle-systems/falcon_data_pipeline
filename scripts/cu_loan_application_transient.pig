-- Load loan_raw and account_raw data
loan_raw_data = LOAD '$falcon_LoanApplicationRaw_database.$falcon_LoanApplicationRaw_table' USING org.apache.hive.hcatalog.pig.HCatLoader();
account_raw_data = LOAD '$falcon_AccountRaw_database.$falcon_AccountRaw_table' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Filter the needed columns 
filtered_loan_raw_data = FOREACH loan_raw_data {
  GENERATE application_id, term_months,first_name,last_name,address,state,phone,type,origination_date,
  loan_decision_type,decided,loan_approved,denial_reason,loan_account_status,approved,
  payment_method,requested_amount,funded,funded_date,rate,ecoagroup,officer_id,denied_date;
}

filtered_account_raw_data = FOREACH account_raw_data {
	GENERATE application_id,account_id;
}

-- join loan and account using applicationid
join_loan_and_account_data = JOIN filtered_loan_raw_data BY application_id LEFT OUTER, filtered_account_raw_data BY application_id;


-- Filter the needed columns from joined data
filtered_joined_data = FOREACH join_loan_and_account_data {
  GENERATE account_id,filtered_loan_raw_data::application_id,term_months,type,
           ToString(origination_date, 'YYYY-MM-dd') as origination_date,
           (loan_decision_type == 'Approve' ? 'Approved' : loan_decision_type) as loan_decision_type,
           (TRIM(decided) !='' ? ToString(ToDate(decided, 'MM/dd/yyyy'),'YYYY-MM-dd') : decided) as decided,
           loan_approved,
           (denial_reason == 'DTI ratio' ? 'Debit To Income Ratio' : 
           (denial_reason == 'Insufficient cash' ? 'Insufficient cash (downpayment, closing costs)' : denial_reason)) as denial_reason,
           loan_account_status,approved,payment_method,requested_amount,funded,
           (TRIM(funded_date) !='' ? ToString(ToDate(funded_date, 'MM/dd/yyyy'),'YYYY-MM-dd') : funded_date) as funded_date,
           rate,ecoagroup,officer_id,
           (TRIM(denied_date) !='' ? ToString(ToDate(denied_date, 'MM/dd/yyyy'),'YYYY-MM-dd') : denied_date) as denied_date,
           (chararray)ToString(CurrentTime(),'yyyy-MM-dd') AS insert_date;
}

-- STORED INTO HDFS
STORE filtered_joined_data INTO '$LoanApplicationTransient' USING PigStorage ('|');