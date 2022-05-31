
declare
   retcode integer;
 begin
  retcode:=opsoracle.PKG_ETL_USER_UTIL.TRUNCATE_TABLE (
            in_app_name => 'context.CONNECTION_IN_APP_NAME',
            in_table_owner =>'context.CONNECTION_EDW_DB_USER_WRK',
            in_table_name  =>'STG_PRE_CAR_DLY_BILLING'
          );

if retcode = -1 then 
     raise_application_error (-20010, 'TABLE is not added in ACL for truncate procedure');
end if;

  end;

