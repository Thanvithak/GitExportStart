WITH  gl_acc_keys as (select GL_ACCOUNT_KEY as GL_ACCT_KEY from DIM_GL_ACCOUNT_T
where GL_ACCOUNT_NUM in ('5400035', '5400045', '5410000', '5431011','6047760', '5400256', '5400201', '5400275', '5400285', '6046040', '5400222', '5400267', '5400110','5400203','1180104',
'1180108', '1180109', '1180113', '5400002', '4313000')),

bill_cycle_tbl as (  
	select distinct dbct.bill_cycle_code_num bill_cycle_code_num, 
  first_value(bill_cycle_key) over (partition by dbct.bill_cycle_code_num order by dbct.BILL_CYCLE_END_DT desc) as bill_cycle_key
	from 
		DIM_BILL_CYCLE_T dbct
	where
		extract (year from dbct.ROW_EXP_DT) = 9999 and  
    dbct.BILL_CYCLE_END_DT < TO_DATE( context.JOB_BUSINESS_PROCESSING_DATE ,'YYYY-MM-DD')
),

cust_bill_cycle_tbl as (
	select sdt.customer_id customer_id, dct.customer_key customer_key, 
  sdt.master_subscriber_id master_subscriber_id, dst.subscriber_key subscriber_key, bct.bill_cycle_key  
	from 
		context.CONNECTION_EDW_DB_USER_WRK  .STG_PRE_CAR_DLY_DELTA sdt
	join
		dim_customer_t dct
	on
		(dct.customer_id = sdt.customer_id)
	join
		dim_subscriber_t dst
	on
		dst.master_subscriber_id = sdt.master_subscriber_id
	join
		bill_cycle_tbl bct
	on	
		(bct.BILL_CYCLE_CODE_NUM = 
		case 
		when VALIDATE_CONVERSION(sdt.BILL_CYCLE_CD AS NUMBER) = 0 then null
		else to_number(sdt.BILL_CYCLE_CD)
		end) 
),

bill_last_sscr_amt as (  
select cbct.master_subscriber_id master_subscriber_id, 
SUM(fsrgst.RCRR_AMT) as BILL_LAST_SSCR_MRC_AMT,
SUM(fsrgst.ONE_TM_DISC_AMT+fsrgst.RCRR_DISC_AMT+fsrgst.USG_DISC_AMT) as BILL_LAST_SSCR_DISC_AMT
from 
	cust_bill_cycle_tbl cbct
join 
	FTA_SSCR_REV_GL_SUMM_T fsrgst
on 
	(cbct.customer_key = fsrgst.cust_key and cbct.subscriber_key = fsrgst.sscr_key and cbct.bill_cycle_key = fsrgst.bill_cycl_key)
join
	gl_acc_keys gl
on
	gl.GL_ACCT_KEY = fsrgst.GL_ACCT_KEY
group by 
	cbct.master_subscriber_id)
  
SELECT /*+ context.MAIN_SQL_HINT */
TEMP1.master_subscriber_id AS SSCR_MSTR_ID
,bill_last_sscr_amt.BILL_LAST_SSCR_MRC_AMT
,bill_last_sscr_amt.BILL_LAST_SSCR_DISC_AMT
FROM context.CONNECTION_EDW_DB_USER_WRK  .STG_PRE_CAR_DLY_DELTA  TEMP1
LEFT JOIN bill_last_sscr_amt ON (bill_last_sscr_amt.master_subscriber_id = TEMP1.master_subscriber_id)
