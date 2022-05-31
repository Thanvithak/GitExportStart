WITH distinct_cust AS
             -- (SELECT /*+ parallel(delta,16) full(delta) */
             (SELECT /*+ context.SQL_HINT_1 */
               DISTINCT customer_id,
                        day_dt
              FROM
               context.CONNECTION_EDW_DB_USER_WRK  .STG_PRE_CAR_DLY_DELTA delta),
         distinct_sscr AS
             -- (SELECT /*+ parallel(delta,16) full(delta) */
             (SELECT /*+ context.SQL_HINT_2 */
               DISTINCT customer_id,
                        master_subscriber_id,
                        sscr_typ1_key,
                        day_dt
              FROM
               context.CONNECTION_EDW_DB_USER_WRK  .STG_PRE_CAR_DLY_DELTA delta),
         hrtc_cust AS
             (SELECT
               customer_id,
               MAX(modl_hrtc_cust_avg_scor_num) modl_hrtc_cust_avg_scor_num,
               MAX(modl_hrtc_cust_max_scor_num) modl_hrtc_cust_max_scor_num,
               MAX(modl_hrtc_cust_min_scor_num) modl_hrtc_cust_min_scor_num,
               MAX(modl_hrtc_cust_ten_scor_num) modl_hrtc_cust_ten_scor_num
              FROM
               (SELECT
                 *
                FROM
                 -- (SELECT /*+ parallel(f,16) full(f) parallel(dpm,16) full(dpm) parallel(dct,16) full(dct) parallel(dc,16) full(dc)*/
                 (SELECT /*+ context.SQL_HINT_3 */
                   dct.customer_id,
                   CASE
                       WHEN modl_var_nm IN ('AVERAGE') THEN f.modl_scor_num
                   END
                       AS modl_hrtc_cust_avg_scor_num,
                   CASE
                       WHEN modl_var_nm IN ('MAXIMUM') THEN f.modl_scor_num
                   END
                       AS modl_hrtc_cust_max_scor_num,
                   CASE
                       WHEN modl_var_nm IN ('MINIMUM') THEN f.modl_scor_num
                   END
                       AS modl_hrtc_cust_min_scor_num,
                   CASE
                       WHEN modl_var_nm IN ('LONGEST TENURE') THEN
                           f.modl_scor_num
                   END
                       AS modl_hrtc_cust_ten_scor_num,
                   ROW_NUMBER()
                   OVER
                   (
                       PARTITION BY dct.customer_id, dpm.modl_var_nm
                       ORDER BY f.scor_dt_key DESC, dpm.modl_ver_num DESC
                   )
                       AS model_rank
                  FROM
                   ft_pred_modl_cust_scor_t f,
                   dim_pred_modl_attrib_t dpm,
                   dim_customer_t dct,
                   distinct_cust dc
                  WHERE
                   dpm.pred_modl_attrib_key = f.pred_modl_attrib_key
                   AND dpm.modl_nm = 'AVCM'
                   AND dpm.modl_var_nm IN ('AVERAGE',
                                           'MAXIMUM',
                                           'MINIMUM',
                                           'LONGEST TENURE')
                   AND f.cust_key = dct.customer_key
                   AND dc.customer_id = dct.customer_id)
                WHERE
                 model_rank = 1)
              GROUP BY
               customer_id),
			   
MODL_ATTRIB2 AS (
SELECT CASE WHEN 
VALIDATE_CONVERSION(BAND_CD AS NUMBER) = 0 THEN -1 
ELSE TO_NUMBER(BAND_CD) END AS BAND_CD
,PRED_MODL_ATTRIB_KEY,MODL_NM,MODL_VER_NUM
FROM DIM_PRED_MODL_ATTRIB_T 
WHERE (modl_nm = 'AVCM' AND modl_var_nm = 'PRIMARY HOLDER') OR modl_nm in ('CPSEG','CVC')
),
		hrtc_cust2 AS
             (SELECT
               customer_id,
              MAX(modl_hrtc_cust_pri_scor_num) AS modl_hrtc_cust_pri_scor_num,
			  MAX(MODL_SEGNUM_CUST_SCOR_NUM) AS MODL_SEGNUM_CUST_SCOR_NUM,
			  MAX(modl_pcv_rnk_cust_scor_num) AS modl_pcv_rnk_cust_scor_num
              FROM
               (SELECT
                 *
                FROM
                 -- (SELECT /*+ parallel(f,16) full(f) parallel(dpm,16) full(dpm) parallel(dct,16) full(dct) parallel(dc,16) full(dc)*/
                 (SELECT /*+ context.SQL_HINT_4 */
                   dct.customer_id,
				   CASE WHEN modl_nm = 'AVCM' THEN f.modl_scor_num END AS modl_hrtc_cust_pri_scor_num,
				   CASE WHEN modl_nm = 'CPSEG' THEN dpm.BAND_CD END AS MODL_SEGNUM_CUST_SCOR_NUM,
				   CASE WHEN modl_nm = 'CVC' THEN dpm.BAND_CD END AS modl_pcv_rnk_cust_scor_num,
                   ROW_NUMBER()
                   OVER
                   (
                       PARTITION BY dct.customer_id, dpm.modl_nm
                       ORDER BY f.scor_dt_key DESC, dpm.modl_ver_num DESC
                   )
                       AS model_rank
                  FROM
                   ft_pred_modl_cust_scor_t f,
                   MODL_ATTRIB2 dpm,
                   dim_customer_t dct,
                   distinct_cust dc
                  WHERE
                   dpm.pred_modl_attrib_key = f.pred_modl_attrib_key
                   AND f.cust_key = dct.customer_key
                   AND dc.customer_id = dct.customer_id)
                WHERE
                 model_rank = 1)
				 group by
				 customer_id),
         acct_dfct AS
             -- (SELECT /*+ parallel(dcu,16) full(dcu) parallel(dc,16) full(dc) parallel(brc,16) full(brc) parallel(dcs,16) full(dcs) */
             (SELECT /*+ context.SQL_HINT_5 */
               dcu.customer_id,
               CASE
                   WHEN dcs.sscr_cnt_desc = '0' THEN brc.row_eff_dt
               END
                   AS ads_acct_dfct_dt,
               CASE
                   WHEN dcs.sscr_cnt_desc = '0' THEN 'Y'
                   ELSE 'N'
               END
                   AS ads_acct_dfct_ind
              FROM
               distinct_cust dcu
               INNER JOIN dim_customer_t dc
                   ON dcu.customer_id = dc.customer_id
                      AND dc.curr_ver_flg = 'Y'
               INNER JOIN brdg_cust_t1_sscr_prfl_t brc
                   ON brc.cust_typ1_key = dc.cust_typ1_key
                      AND extract(year from brc.row_exp_dt) = 9999
               INNER JOIN dim_cust_sscr_profile_t dcs
                   ON brc.cust_sscr_prfl_key = dcs.cust_sscr_profile_key),
MODL_SSCR AS (SELECT S.MASTER_SUBSCRIBER_ID,
	MAX(S.MODL_MVS_SSCR_SCOR_NUM ) AS MODL_MVS_SSCR_SCOR_NUM,
	MAX(S.MODL_TVS_SSCR_SCOR_NUM) AS MODL_TVS_SSCR_SCOR_NUM
FROM ( SELECT /*+ context.SQL_HINT_6 */
			SUBT.MASTER_SUBSCRIBER_ID,
			ROW_NUMBER() OVER(PARTITION BY SUB.MASTER_SUBSCRIBER_ID, MATR.MODL_NM, MATR.MODL_VAR_NM 
			ORDER BY S_SCR.SCOR_DT_KEY DESC, MATR.MODL_VER_NUM DESC ) AS RN,
			CASE WHEN MATR.MODL_NM = 'MVS' THEN S_SCR.MODL_SCOR_NUM END AS MODL_MVS_SSCR_SCOR_NUM,
			CASE WHEN MATR.MODL_NM = 'TVS' THEN S_SCR.MODL_SCOR_NUM END AS MODL_TVS_SSCR_SCOR_NUM
		FROM distinct_sscr SUB
			LEFT JOIN DIM_SUBSCRIBER_T SUBT ON SUBT.MASTER_SUBSCRIBER_ID = SUB.MASTER_SUBSCRIBER_ID
			INNER JOIN ft_pred_modl_sscr_scor_t S_SCR ON S_SCR.SSCR_KEY = SUBT.SUBSCRIBER_KEY
			INNER JOIN DIM_PRED_MODL_ATTRIB_T MATR ON MATR.PRED_MODL_ATTRIB_KEY = S_SCR.PRED_MODL_ATTRIB_KEY
			AND MATR.MODL_NM IN ('MVS','TVS')
 ) S
WHERE S.RN = 1
GROUP BY S.MASTER_SUBSCRIBER_ID)
,
MODL_ATTRIB AS (
SELECT CASE WHEN 
VALIDATE_CONVERSION(BAND_CD AS NUMBER) = 0 THEN -1 
ELSE TO_NUMBER(BAND_CD) END AS BAND_CD
,PRED_MODL_ATTRIB_KEY,MODL_NM,MODL_VER_NUM,MODL_VAR_NM 
FROM DIM_PRED_MODL_ATTRIB_T 
WHERE MODL_NM IN ('HRTCV2SCR','CVS')
),
MODL_SSCR2 AS (SELECT S.MASTER_SUBSCRIBER_ID,
	MAX(S.MODL_HRTC_SSCR_SCOR_NUM ) AS MODL_HRTC_SSCR_SCOR_NUM,
	MAX(S.MODL_PCV_RNK_SSCR_SCOR_NUM) AS MODL_PCV_RNK_SSCR_SCOR_NUM
FROM ( SELECT /*+ context.SQL_HINT_6 */
			SUBT.MASTER_SUBSCRIBER_ID,
			ROW_NUMBER() OVER(PARTITION BY SUB.MASTER_SUBSCRIBER_ID, MATR.MODL_NM, MATR.MODL_VAR_NM 
			ORDER BY S_SCR.SCOR_DT_KEY DESC, MATR.MODL_VER_NUM DESC) AS RN,
			CASE WHEN MATR.MODL_NM = 'HRTCV2SCR' THEN MATR.BAND_CD END AS MODL_HRTC_SSCR_SCOR_NUM,
			CASE WHEN MATR.MODL_NM = 'CVS' THEN MATR.BAND_CD END AS MODL_PCV_RNK_SSCR_SCOR_NUM
				FROM distinct_sscr SUB
			LEFT JOIN DIM_SUBSCRIBER_T SUBT ON SUBT.MASTER_SUBSCRIBER_ID = SUB.MASTER_SUBSCRIBER_ID
			INNER JOIN ft_pred_modl_sscr_scor_t S_SCR ON S_SCR.SSCR_KEY = SUBT.SUBSCRIBER_KEY
			INNER JOIN MODL_ATTRIB MATR ON MATR.PRED_MODL_ATTRIB_KEY = S_SCR.PRED_MODL_ATTRIB_KEY
 ) S
WHERE S.RN = 1
GROUP BY S.MASTER_SUBSCRIBER_ID),
modl_hrtccv_cust as (
select customer_id, max(MODL_HRTCCV_SSCR_SCOR_NUM) as modl_hrtccv_cust_scor_num from(
select sub.master_subscriber_id,
sub.customer_id,
CASE WHEN MODL_HRTC_SSCR_SCOR_NUM IN (1,2,3) and MODL_PCV_RNK_SSCR_SCOR_NUM = 0 then 1
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (1,2,3) AND MODL_PCV_RNK_SSCR_SCOR_NUM = 1 then 3
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (1,2,3) and MODL_PCV_RNK_SSCR_SCOR_NUM = 2 then 6
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (4,5,6) and MODL_PCV_RNK_SSCR_SCOR_NUM = 0 then 2
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (4,5,6) and MODL_PCV_RNK_SSCR_SCOR_NUM = 1 then 5
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (4,5,6) and MODL_PCV_RNK_SSCR_SCOR_NUM = 2 then 8
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (7,8,9,10) and MODL_PCV_RNK_SSCR_SCOR_NUM = 0 then 4
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (7,8,9,10) and MODL_PCV_RNK_SSCR_SCOR_NUM = 1 then 7
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (7,8,9,10) and MODL_PCV_RNK_SSCR_SCOR_NUM = 2 then 9 END AS MODL_HRTCCV_SSCR_SCOR_NUM
from distinct_sscr sub
INNER JOIN MODL_SSCR2 on MODL_SSCR2.master_subscriber_id = sub.master_subscriber_id
INNER JOIN dim_subscriber_t subt
   ON subt.master_subscriber_id = sub.master_subscriber_id
INNER JOIN brdg_sscr_t1_dvc_t1_t bdvc
   ON subt.sscr_typ1_key = bdvc.sscr_typ1_key
	  AND extract(year from bdvc.row_exp_dt) = 9999
INNER JOIN dim_device_t dvc
   ON bdvc.dvc_typ1_key = dvc.dvc_typ1_key
	  AND extract(year from dvc.row_exp_dt) = 9999
WHERE dvc.equipment_category2_desc IN ('Smartphone', 'Feature Phone')) group by customer_id
)
    -- SELECT /*+ parallel(dss,16) full(dss) parallel(dst,16) full(dst) parallel(ft,16) full(ft) parallel(fts_d,16) full(fts_d) parallel(ad,16) full(ad)  parallel(hc,16) full(hc) parallel(ccs,16) full(ccs) parallel(MODL_SSCR,16) parallel(MODL_SSCR2,16) parallel(modl_hrtccv_cust,16)*/
    SELECT /*+ context.MAIN_SQL_HINT */
     dss.master_subscriber_id AS sscr_mstr_id,
     ads_acct_dfct_dt,
     ads_acct_dfct_ind,
     CASE
         WHEN CAST
              (
                  NVL(ft.addn_cntd_cnt, 0) - NVL(ft.addn_offset_cnt, 0) AS NUMBER(6)
              ) = 1
              AND CAST
                  (
                      NVL(ft.addn_cntd_cnt, 0)
                      - NVL(ft.addn_offset_cnt, 0)
                      - NVL(ft.dfct_invl_cntd_cnt, 0)
                      - NVL(ft.dfct_voln_cntd_cnt, 0)
                      NVL(ft.dfct_invl_offset_cnt, 0)
                      NVL(ft.dfct_voln_offset_cnt, 0) AS NUMBER(6)
                  ) = 1 THEN
             1
         ELSE
             0
     END
         AS ads_addn_cnt,
	CAST
     (
			CASE
                  WHEN NVL(ft.dfct_invl_gross_cnt, 0) NVL(ft.dfct_voln_gross_cnt, 0) <= 0 THEN NULL
                  WHEN NVL(ft.dfct_invl_gross_cnt, 0) > 0 THEN 1
                  ELSE 0
            END AS NUMBER(6)
     )
         AS ads_dfct_invol_cnt,
    CAST
     (
			CASE
                  WHEN NVL(ft.dfct_invl_gross_cnt, 0) NVL(ft.dfct_voln_gross_cnt, 0) <= 0 THEN NULL
                  WHEN NVL(ft.dfct_voln_gross_cnt, 0) > 0 AND NVL(ft.dfct_invl_gross_cnt, 0) = 0 THEN 1
                  ELSE 0
            END AS NUMBER(6)
     )
         AS ads_dfct_voln_cnt,
     modl_hrtc_cust_avg_scor_num,
     modl_hrtc_cust_max_scor_num,
     modl_hrtc_cust_min_scor_num,
     modl_hrtc_cust_pri_scor_num,
     modl_hrtc_cust_ten_scor_num,
     modl_hrtccv_cust_scor_num,
     modl_pcv_rnk_cust_scor_num
,MODL_TVS_SSCR_SCOR_NUM
,MODL_HRTC_SSCR_SCOR_NUM
,MODL_MVS_SSCR_SCOR_NUM
,MODL_PCV_RNK_SSCR_SCOR_NUM
,MODL_SEGNUM_CUST_SCOR_NUM
,CASE WHEN MODL_HRTC_SSCR_SCOR_NUM IN (1,2,3) and MODL_PCV_RNK_SSCR_SCOR_NUM = 0 then 1
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (1,2,3) AND MODL_PCV_RNK_SSCR_SCOR_NUM = 1 then 3
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (1,2,3) and MODL_PCV_RNK_SSCR_SCOR_NUM = 2 then 6
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (4,5,6) and MODL_PCV_RNK_SSCR_SCOR_NUM = 0 then 2
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (4,5,6) and MODL_PCV_RNK_SSCR_SCOR_NUM = 1 then 5
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (4,5,6) and MODL_PCV_RNK_SSCR_SCOR_NUM = 2 then 8
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (7,8,9,10) and MODL_PCV_RNK_SSCR_SCOR_NUM = 0 then 4
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (7,8,9,10) and MODL_PCV_RNK_SSCR_SCOR_NUM = 1 then 7
WHEN MODL_HRTC_SSCR_SCOR_NUM IN (7,8,9,10) and MODL_PCV_RNK_SSCR_SCOR_NUM = 2 then 9 END AS MODL_HRTCCV_SSCR_SCOR_NUM
    FROM
     context.CONNECTION_EDW_DB_USER_WRK  .STG_PRE_CAR_DLY_DELTA dss
     INNER JOIN dim_subscriber_t dst
         ON dss.master_subscriber_id = dst.master_subscriber_id
            AND dst.curr_ver_flg = 'Y'
     LEFT JOIN ft_sscr_ads_t ft
         ON ft.subscriber_key = dst.subscriber_key
            AND ft.ads_event_date_key =
                    TO_NUMBER(TO_CHAR(dss.day_dt - 1, 'YYYYMMDD'))
     LEFT JOIN acct_dfct ad ON dss.customer_id = ad.customer_id
     LEFT JOIN hrtc_cust hc ON dss.customer_id = hc.customer_id
     LEFT JOIN hrtc_cust2 hc2 ON dss.customer_id = hc2.customer_id
     LEFT JOIN modl_hrtccv_cust mc ON dss.customer_id = mc.customer_id
     LEFT JOIN MODL_SSCR ms
         ON dss.master_subscriber_id = ms.master_subscriber_id
     LEFT JOIN MODL_SSCR2 ms2
         ON dss.master_subscriber_id = ms2.master_subscriber_id
