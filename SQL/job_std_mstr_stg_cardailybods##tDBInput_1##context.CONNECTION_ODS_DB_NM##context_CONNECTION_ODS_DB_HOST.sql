With delta_sscr as 
(select distinct subscriber_id from context.CONNECTION_EDW_DB_USER_WRK.STG_PRE_CAR_DLY_DELTA@context.CONNECTION_EDW_DBLINK_NAME)
--SELECT /*+ parallel(rxn,16) full(rxn) parallel(role,16) full(role) parallel(part,16) full(part) parallel(d,16) full(d)*/
SELECT /*+ context.MAIN_SQL_HINT */
 part.instance_id AS subscriber_id,
 rxn.start_time
FROM  table_intrxn rxn, table_con_sp_role role, table_site_part part, delta_sscr d
WHERE active = 1
 and d.subscriber_id = part.instance_id
 AND rxn.intrxn2contact = role.con_sp_role2contact
 AND role.con_sp_role2site_part = part.objid
 AND CASE
		 WHEN rxn.notes LIKE '%UPGRADE%' THEN 1
		 WHEN rxn.notes LIKE '%upgrade%' THEN 1
		 WHEN rxn.notes LIKE '%Upgrade%' THEN 1
		 ELSE 0
	 END > 0
GROUP BY
 part.instance_id, rxn.start_time
