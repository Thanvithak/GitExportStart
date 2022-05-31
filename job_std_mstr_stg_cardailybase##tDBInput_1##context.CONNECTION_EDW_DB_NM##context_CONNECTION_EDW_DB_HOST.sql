WITH distinct_cust AS
            -- (SELECT /*+ parallel(delta,16) full(delta) */
             (SELECT /*+ context.SQL_HINT_1 */
               DISTINCT customer_id,
                        day_dt
              FROM
               dwwrk.STG_PRE_CAR_DLY_DELTA delta),
         distinct_sscr AS
             -- (SELECT /*+ parallel(delta,16) full(delta) */
              (SELECT /*+ context.SQL_HINT_2 */
               DISTINCT customer_id,
                        master_subscriber_id,
                        subscriber_id,
                        sscr_typ1_key,
                        day_dt
              FROM
               dwwrk.STG_PRE_CAR_DLY_DELTA delta),
         new_cust AS
             (SELECT
               DISTINCT customer_id,
                        CASE
                            WHEN no_of_cancels = 0 THEN
                                'Y'
                            WHEN customer_curr_status = 'Active'
                                 AND no_of_cancels > 0 THEN
                                'N'
                            WHEN customer_curr_status = 'Ceased'
                                 AND no_of_cancels > 1 THEN
                                'N'
                            WHEN customer_curr_status = 'Ceased'
                                 AND no_of_cancels = 1 THEN
                                'Y'
                        END
                            AS ads_new_cust_ind
              FROM
               -- (SELECT /*+ ordered parallel(dct,16) full(dct) parallel(bccst,16) full(bccst)  parallel(dcst,16) full(dcst) */
                (SELECT /*+ context.SQL_HINT_3 */
                 delta.customer_id,
                 SUM
                 (
                     CASE
                         WHEN customer_status_cd = 'C' THEN 1
                         ELSE 0
                     END
                 )
                 OVER (PARTITION BY delta.customer_id)
                     AS no_of_cancels,
                 MAX
                 (
                     CASE
                         WHEN bccst.row_exp_dt =
                                  TO_DATE('12/31/9999', 'MM/DD/YYYY')
                              AND customer_status_cd = 'C' THEN
                             'Ceased'
                         ELSE
                             'Active'
                     END
                 )
                 OVER (PARTITION BY delta.customer_id)
                     AS customer_curr_status
                FROM
                 distinct_cust delta
                 INNER JOIN dim_customer_t dct
                     ON dct.customer_id = delta.customer_id
                        AND dct.curr_ver_flg = 'Y'
                 INNER JOIN brdg_cust_t1_cust_stat_t bccst
                     ON bccst.cust_typ1_key = dct.cust_typ1_key
                 INNER JOIN dim_customer_status_t dcst
                     ON dcst.customer_status_key = bccst.cust_stat_key)),
         bods_upgd_touch_cnt AS
             -- (SELECT /*+ parallel(bods,16) full(bods) parallel(delta,16) full(delta) */
              (SELECT /*+ context.SQL_HINT_4 */
               delta.master_subscriber_id AS master_subscriber_id,
               COUNT(1) AS cust_last_90_upgd_touch_cnt
              FROM
               dwwrk.STG_PRE_CAR_DLY_BODS bods
               INNER JOIN distinct_sscr delta
                   ON bods.subscriber_id = delta.subscriber_id
                      AND bods.start_time >= (delta.day_dt - 90)
			  group by delta.master_subscriber_id),
         dim_ctrc AS
             -- (SELECT /*+ ordered parallel(delta,16) full(delta) parallel(dct,16) full(dct) */
              (SELECT /*+ context.SQL_HINT_5 */
               master_subscriber_id,
               instl_recur_amt,
               ROW_NUMBER()
               OVER
               (
                   PARTITION BY master_subscriber_id
                   ORDER BY row_eff_dt DESC, ctrct_id DESC
               )
                   rnk
              FROM
               distinct_sscr delta
               INNER JOIN dim_ctrct_t dct
                   ON dct.mstr_sscr_id = delta.master_subscriber_id
                      AND dct.ctrct_type_cd = 'EIP'
                      AND dct.row_exp_dt = TO_DATE('12/31/9999', 'MM/DD/YYYY')),
         dvc_actvn AS
             (SELECT
               customer_id,
               cust_last_dvc_actvn_dt
              FROM
               -- (SELECT /*+ ordered parallel(delta,16) full(delta) parallel(dct,16) full(dct) parallel(fdae,16) full(fdae) parallel(dt,16) full(dt)*/
               (SELECT /*+ context.SQL_HINT_6 */
                 dct.customer_id,
                 dt.day_dt AS cust_last_dvc_actvn_dt,
                 ROW_NUMBER()
                 OVER
                 (
                     PARTITION BY dct.customer_id
                     ORDER BY actvn_event_dt_key DESC
                 )
                     AS rnk
                FROM
                 distinct_cust delta
                 INNER JOIN dim_customer_t dct
                     ON delta.customer_id = dct.customer_id
                 INNER JOIN ft_dvc_actvn_event_t fdae
                     ON dct.customer_key = fdae.customer_key
                        AND fdae.actvn_event_type_key IN
                                -- (SELECT /*+ parallel(ref_code_t,16) full(ref_code_t) */
                                (SELECT /*+ context.SQL_HINT_7 */
                                  ref_code_key
                                 FROM
                                  ref_code_t
                                 WHERE
                                  domain_nm = 'Device Activation Event Type'
                                  AND reference_cd = 'ACT'
                                  AND active_ind = 'Y')
                 INNER JOIN dim_date_t dt
                     ON dt.date_key = actvn_event_dt_key)
              WHERE
               rnk = 1),
         cust_event AS
            -- (SELECT /*+ parallel(dt,16) full(dt) */
             (SELECT /*+ context.SQL_HINT_8 */
               customer_id,
               dt.day_dt AS cust_myaccount_reg_dt
              FROM
               --(SELECT /*+ ordered parallel(delta,16) full(delta) parallel(dct,16) full(dct) parallel(fcet,16) full(fcet) */
                (SELECT /*+ context.SQL_HINT_9 */
                 delta.customer_id,
                 evnt_dt_key
                FROM
                 distinct_cust delta
                 INNER JOIN dim_customer_t dct
                     ON delta.customer_id = dct.customer_id
                 INNER JOIN ft_cust_evnt_t fcet
                     ON dct.customer_key = fcet.cust_key
                        AND fcet.evnt_typ_key IN
                                -- (SELECT /*+ parallel(ref_code_t,16) full(ref_code_t) */
                                 (SELECT /*+ context.SQL_HINT_10 */
                                  ref_code_key
                                 FROM
                                  ref_code_t
                                 WHERE
                                  domain_nm = 'Customer Event Type'
                                  AND reference_cd = 'CUST_PORTAL_REGISTRATION'
                                  AND active_ind = 'Y')) a
               INNER JOIN dim_date_t dt ON dt.date_key = a.evnt_dt_key),
         fin_acct AS
             (SELECT
               customer_id,
               account_collection_status_desc
              FROM
              --  (SELECT /*+ ordered parallel(delta,16) full(delta) parallel(fa,16) full(fa) */
               (SELECT /*+ context.SQL_HINT_11 */
                 customer_id,
                 account_collection_status_desc,
                 ROW_NUMBER()
                 OVER
                 (
                     PARTITION BY customer_id
                     ORDER BY
                         (CASE
                              WHEN account_collection_status_desc = 'Active' THEN
                                  1
                              WHEN account_collection_status_desc = 'Suspended' THEN
                                  2
                              WHEN account_collection_status_desc = 'Cancelled' THEN
                                  3
                              ELSE
                                  4
                          END)
                 )
                     AS rnk
                FROM
                 distinct_cust delta
                 INNER JOIN dim_financial_account_t fa
                     ON fa.src_customer_id = delta.customer_id
                        AND delta.day_dt BETWEEN fa.row_eff_dt
                                             AND fa.row_exp_dt)
              WHERE
               rnk = 1),
         promo_card AS
            -- (SELECT /*+ parallel(dt,16) full(dt) */
             (SELECT /*+ context.SQL_HINT_12 */
               DISTINCT customer_id,
                        master_subscriber_id,
                        day_dt AS promo_card_last_assign_dt,
                        promo_card_cnt,
                        promo_card_tot_amt,
                        cust_rnk
              FROM
              -- (SELECT /*+ parallel(fpca,16) full(fpca) parallel(dct,16) full(dct) parallel(dst,16) full(dst) */
               (SELECT /*+ context.SQL_HINT_13 */
                 dct.customer_id,
                 dst.master_subscriber_id,
                 COUNT(promo_card_key) OVER (PARTITION BY dct.customer_id)
                     AS promo_card_cnt,
                 SUM(promo_card_amt) OVER (PARTITION BY dct.customer_id)
                     AS promo_card_tot_amt,
                 MAX(assgn_dt_key)
                 OVER (PARTITION BY dct.customer_id, dst.master_subscriber_id)
                     AS promo_card_last_assign_dt_key,
                 ROW_NUMBER() OVER(PARTITION BY dct.customer_id ORDER BY 1)
                     AS cust_rnk
                FROM
                 fas_promo_card_assgn_t fpca
                 INNER JOIN dim_customer_t dct
                     ON dct.customer_key = fpca.cust_key
                        AND fpca.promo_card_key > 0
                        AND fpca.actvn_rslt_key IN
                              --   (SELECT /*+ parallel(ref_code_t,16) full(ref_code_t) */
                                (SELECT /*+ context.SQL_HINT_14 */
                                  ref_code_key
                                 FROM
                                  ref_code_t
                                 WHERE
                                  domain_nm = 'Promo Card Activation Result'
                                  AND reference_cd = 'SUCC'
                                  AND active_ind = 'Y')
                 INNER JOIN dim_subscriber_t dst
                     ON dst.subscriber_key = fpca.sscr_key) a
               INNER JOIN dim_date_t dt
                   ON a.promo_card_last_assign_dt_key = dt.date_key),
         pob_amt AS
            --  (SELECT /*+ ordered parallel(delta,16) full(delta) parallel(bspbt,16) full(bspbt) parallel(dpobt,16) full(dpobt)*/
             (SELECT /*+ context.SQL_HINT_15 */
               delta.sscr_typ1_key,
               SUM(case when dpobt.bill_offer_type_desc = 'Additional' AND dpobt.bill_offer_service_type_desc IN ('DATA', 'DATA ACCESS') then sscr_recurring_charge_amt else 0 end) AS sa_data_optnl_mrc_amt,
			   max(case when dpobt.bill_offer_marketing_nm IN ( 'OTH17', 'PPR' ) AND ( dpobt.bill_offer_nm LIKE 'High Speed Internet%' OR dpobt.bill_offer_nm LIKE 'Internet Backup%' ) AND dpobt.bill_offer_type_desc = 'Primary' AND dpobt.bill_offer_service_type_desc =	'CONNECTED DEVICE'   THEN 'Y' ELSE 'N' END) AS HSI_SSCR_Flag

              FROM
               distinct_sscr delta
               INNER JOIN brdg_sscr_t1_pob_t1_t bspbt
                   ON bspbt.sscr_typ1_key = delta.sscr_typ1_key
                      AND delta.day_dt BETWEEN bspbt.enroll_eff_dt AND bspbt.enroll_exp_dt
               INNER JOIN dim_product_offer_billable_t dpobt
                   ON dpobt.prod_offr_bill_typ1_key =
                          bspbt.prod_offr_bill_typ1_key
                      AND dpobt.row_exp_dt =
                              TO_DATE('12/31/9999', 'MM/DD/YYYY')
                     
              GROUP BY
               delta.sscr_typ1_key),
			   
         sscr_event AS
               -- (SELECT /*+ ordered parallel(delta,16) full(delta) parallel(dct,16) full(dct) parallel(dst,16) full(dst) parallel(fse,16) full(fse) parallel(de,16) full(de)*/
             (SELECT /*+ context.SQL_HINT_16 */
               DISTINCT delta.customer_id,
                        delta.master_subscriber_id,
                        'Y' AS sa_nap_chng_last_1_3_mth_ind
              FROM
               distinct_sscr delta
               INNER JOIN dim_customer_t dct
                   ON delta.customer_id = dct.customer_id
               INNER JOIN dim_subscriber_t dst
                   ON delta.master_subscriber_id = dst.master_subscriber_id
               INNER JOIN ft_sscr_event_t fse
                   ON dct.customer_key = fse.customer_key
                      AND dst.subscriber_key = fse.subscriber_key
                      AND event_date_key >=
                              TO_CHAR(ADD_MONTHS(day_dt, -3), 'YYYYMMDD')
               INNER JOIN dim_event_type_t de
                   ON fse.event_type_key = de.event_type_key
                      AND de.event_type_lbl = 'PRICE PLAN MIGRATION'
                      AND de.row_exp_dt = TO_DATE('12/31/9999', 'MM/DD/YYYY')),
		 dr_collection_sub_list AS
             -- (SELECT /*+ parallel(dat,8) full(dat) full(ds)  */
			  (SELECT /*+ context.SQL_HINT_17 */
                              ds.master_subscriber_id
                  FROM
                              dwdal.dat_t_csm_account_t dat
                              INNER JOIN dwdpl.dim_subscriber_t ds
                                    ON dat.ban = ds.fin_acct_id
                                          AND ds.row_exp_dt = TO_DATE( '12/31/9999', 'MM/DD/YYYY')
                  WHERE
                              dat.row_exp_dt = TO_DATE( '12/31/9999', 'MM/DD/YYYY')
                              AND dat.coll_ind = 'Y'
                              AND dat.coll_status != 'NONE')

    -- SELECT /*+ parallel(delta,16) full(delta) parallel(dct,16) full(dct) parallel(dlt,16) full(dlt) parallel(dcdt,16) parallel(dcdt) parallel(bsdt,16) full(bsdt) parallel(FPS_SSCR_CMPGN_MART_MV,16) full(FPS_SSCR_CMPGN_MART_MV) */
     SELECT /*+ context.MAIN_SQL_HINT */
     DISTINCT 
     delta.master_subscriber_id as sscr_mstr_id
,FPS_SSCR_CMPGN_MART_MV.EQUIP_ELIG_STAT_DESC AS CTRCT_DVC_ELIG_STAT_DESC
,NVL(FPS_SSCR_CMPGN_MART_MV.CTRCT_PAY_REMAIN_CNT,0) AS CTRCT_EIP_INSTALL_REMAIN_CNT
,EMAIL_BEST_MRKTNG_ADDR
,CASE WHEN dct.addr_mktg_line_1 IS NOT NULL THEN dct.addr_mktg_line_1 ELSE dct.addr_bill_line_1 END AS ADDR_BILL_LINE_1 
,CASE WHEN dct.addr_mktg_line_1 IS NOT NULL THEN dct.addr_mktg_line_2 ELSE dct.addr_bill_line_2 END AS ADDR_BILL_LINE_2 
,CASE WHEN dct.addr_mktg_line_1 IS NOT NULL THEN dct.addr_mktg_city_nm ELSE dct.addr_bill_city_nm END AS ADDR_BILL_CITY_NM 
,CASE WHEN dct.addr_mktg_line_1 IS NOT NULL THEN dct.addr_mktg_zip_plus4_cd ELSE dct.addr_bill_zip_plus4_cd END AS ADDR_BILL_ZIP_PLUS4_CD 
,CASE WHEN dct.addr_mktg_line_1 IS NOT NULL THEN dct.addr_mktg_state_cd ELSE dct.addr_bill_state_cd END AS ADDR_BILL_STATE_CD 
,CASE WHEN dct.addr_mktg_line_1 IS NOT NULL THEN dct.addr_mktg_zip_cd ELSE NVL(dct.addr_bill_zip_cd,'{UNK}') END AS ADDR_BILL_ZIP_CD 
,INITIAL_CREDIT_CLASS_GRP_DESC
,DCT.MARKET_NM
,DCT.REGION_NM
,MRKT_HIER_SUBMRKT_CD
,MRKT_HIER_SUBMRKT_NM
,DCT.TERRITORY_NM
,CUSTOMER_FIRST_NM
,CUSTOMER_LAST_NM
,CASE WHEN dr.master_subscriber_id IS NULL THEN 'N' ELSE 'Y' END AS fin_acct_delq_ind
,NVL(OPT_CPNI_DESC,'{UNK}') AS OPT_CPNI_DESC
,delta.OPT_SSCR_MDN_DNC_IND
,delta.SSCR_PRIM_MDN_IND
,delta.CURR_SUBSCRIBER_STATUS_DESC AS SSCR_STAT_DESC
,NVL(ROUND(MONTHS_BETWEEN(delta.day_dt, delta.SUBSCRIBER_ACTIVATION_DT)),0) AS SSCR_TENURE_MTH_NUM
     ,delta.customer_id,
     delta.src,
     NVL(nc.ads_new_cust_ind, 'N') AS ads_new_cust_ind,
     NVL(delta.ctrct_pay_cnt, 0) AS ctrct_eip_install_paid_cnt,
     NVL(dc.instl_recur_amt, 0) AS ctrct_eip_install_recur_amt,
     dct.email_best_mrktng_addr_src_cd AS cust_best_email_addr_src,
     NVL(dct.bill_cycle_cd, '{UNK}') AS cust_bill_cycl_cd,
     dct.addr_bill_zip_plus4_cd AS cust_bill_zip_ext_cd,
     dct.customer_birth_dt AS cust_brth_dt,
     NVL(dct.credit_class_cd, '{UNK}') AS cust_cred_cls_cd,
     NVL(dct.credit_class_desc, '{UNK}') AS cust_cred_cls_curr_desc,
     NVL(dct.credit_class_desc, '{UNK}') AS cust_cred_cls_desc,
     NVL(dct.credit_class_group_desc, '{UNK}') AS cust_cred_cls_grp_curr_desc,
     NVL(dct.initial_credit_class_desc, '{UNK}') AS cust_cred_cls_init_desc,
     dct.language_preference_cd AS cust_lang_pref_cd,
     NVL(bods.cust_last_90_upgd_touch_cnt, 0) AS cust_last_90_upgd_touch_cnt,
     NVL(da.cust_last_dvc_actvn_dt, TO_DATE('01/01/1901', 'MM/DD/YYYY'))
         AS cust_last_dvc_actvn_dt,
     dct.addr_mktg_zip_cd AS cust_mrkt_geo_zip_cd,
     NVL(dct.myacct_enroll_ind, 'N') AS cust_myaccount_ind,
     ce.cust_myaccount_reg_dt AS cust_myaccount_reg_dt,
     NVL(dct.revenue_generating_ind, 'N') AS cust_revn_gen_ind,
     NVL(dlt.sale_chnl_lvl_1_desc, '{UNK}') AS cust_sale_chnl_curr_lvl_1_desc,
     NVL(dlt.sale_chnl_lvl_2_desc, '{UNK}') AS cust_sale_chnl_curr_lvl_2_desc,
     NVL(dlt.sale_chnl_lvl_3_desc, '{UNK}') AS cust_sale_chnl_curr_lvl_3_desc,
     CASE
         WHEN dct.curr_customer_status_desc = 'Suspended' THEN 'S'
         WHEN dct.curr_customer_status_desc = 'Active' THEN 'A'
         WHEN dct.curr_customer_status_desc = 'Cancelled' THEN 'C'
         WHEN dct.curr_customer_status_desc = 'Tentative' THEN 'T'
         WHEN dct.curr_customer_status_desc = '{NA}' THEN '{NA}'
         ELSE '{UNK}'
     END
         AS cust_stat_cd,
     NVL(dct.curr_customer_status_desc, '{UNK}') AS cust_stat_desc,
     CASE
         WHEN cust_init_actvn_dt IS NULL THEN 0
         ELSE NVL(ROUND(MONTHS_BETWEEN(day_dt, cust_init_actvn_dt)), 0)
     END
         AS cust_tenure_mth_num,
     NVL(dcdt.buy_chnl_prfr_inet_prpn_rnk, '-2') AS demo_buy_pref_inet_prpn_rnk,
     NVL(dcdt.buy_chnl_prfr_mail_prpn_rnk, '-2') AS demo_buy_pref_mail_prpn_rnk,
     NVL(dcdt.buy_chnl_prfr_phn_prpn_rnk, '-2') AS demo_buy_pref_phon_prpn_rnk,
     CASE
         WHEN dcdt.chld_feml_age_0_2_psnt_ind = 'Y'
              OR dcdt.chld_feml_age_3_5_psnt_ind = 'Y'
              OR dcdt.chld_feml_age_6_10_psnt_ind = 'Y'
              OR dcdt.chld_male_age_0_2_psnt_ind = 'Y'
              OR dcdt.chld_male_age_3_5_psnt_ind = 'Y'
              OR dcdt.chld_male_age_6_10_psnt_ind = 'Y'
              OR dcdt.chld_unkgen_age_0_2_psnt_ind = 'Y'
              OR dcdt.chld_unkgen_age_3_5_psnt_ind = 'Y'
              OR dcdt.chld_unkgen_age_6_10_psnt_ind = 'Y' THEN
             'Y'
         ELSE
             'N'
     END
         AS demo_chld_age_0_10_psnt_ind,
     CASE
         WHEN dcdt.chld_feml_age_11_15_psnt_ind = 'Y'
              OR dcdt.chld_male_age_11_15_psnt_ind = 'Y'
              OR dcdt.chld_unkgen_age_11_15_psnt_ind = 'Y' THEN
             'Y'
         ELSE
             'N'
     END
         AS demo_chld_age_11_15_psnt_ind,
     CASE
         WHEN dcdt.chld_feml_age_16_17_psnt_ind = 'Y'
              OR dcdt.chld_male_age_16_17_psnt_ind = 'Y'
              OR dcdt.chld_unkgen_age_16_17_psnt_ind = 'Y' THEN
             'Y'
         ELSE
             'N'
     END
         AS demo_chld_age_16_17_psnt_ind,
     dcdt.psnt_of_chld_ind AS demo_chld_psnt_ind,
     CASE
         WHEN dct.customer_birth_dt IS NULL THEN
             NULL
         WHEN (EXTRACT(YEAR FROM day_dt)
               - EXTRACT(YEAR FROM dct.customer_birth_dt)) > 100 THEN
             NULL
         ELSE
             EXTRACT(YEAR FROM day_dt)
             - EXTRACT(YEAR FROM dct.customer_birth_dt)
     END
         AS demo_cust_age_num,
     dcdt.fst_indv_edu_desc AS demo_frst_indv_educ_desc,
     dcdt.fst_indv_gndr_cd AS demo_frst_indv_gndr_cd,
     dcdt.fst_indv_ocpt_desc AS demo_frst_indv_ocpt_desc,
     dcdt.home_mkt_val_desc AS demo_home_mrkt_val,
     dcdt.incm_desc AS demo_incm_desc,
     dcdt.lgth_of_rsdn_desc AS demo_length_of_rsdn,
     dcdt.num_of_adlt_desc AS demo_num_adlt_cnt,
     dcdt.num_of_chld_cnt AS demo_num_chld_cnt,
     dcdt.home_ownr_rntr_cd AS demo_ownr_rntr_cd,
     NVL(dcdt.prsnx_clstr_desc, '{UNK}') AS demo_prsnx_clust_desc,
     dcdt.rcnt_home_buyr_ind AS demo_rcnt_home_buyr_ind,
     dct.rfscore_num AS demo_rfscore_num,
     dcdt.scnd_indv_edu_desc AS demo_scnd_indv_educ_desc,
     dcdt.scnd_indv_gndr_cd AS demo_scnd_indv_gndr_cd,
     dcdt.scnd_indv_ocpt_desc AS demo_scnd_indv_ocpt_desc,
     NVL(fa.account_collection_status_desc, '{UNK}') AS fin_acct_coll_stat_desc,
     NVL(dct.opt_dnc_phone_home_ind, 'N') AS opt_cust_phone_home_dnc_ind,
     NVL(dct.opt_dnc_phone_prim_ind, 'N') AS opt_cust_phone_prim_dnc_ind,
     NVL(dct.opt_dnc_phone_work_ind, 'N') AS opt_cust_phone_work_dnc_ind,
     NVL(pc_cust.promo_card_cnt, 0) AS promo_card_cnt,
     NVL
     (
         pc_sscr.promo_card_last_assign_dt,
         TO_DATE('01/01/1901', 'MM/DD/YYYY')
     )
         AS promo_card_last_assign_dt,
     NVL(pc_cust.promo_card_tot_amt, 0) AS promo_card_tot_amt,
     NVL(pa.sa_data_optnl_mrc_amt, 0) AS sa_data_optnl_mrc_amt,
     NVL(se.sa_nap_chng_last_1_3_mth_ind, 'N') AS sa_nap_chng_last_1_3_mth_ind,
     delta.subscriber_id AS sscr_bill_id,
     CASE
         WHEN bsdt.row_eff_dt IS NULL THEN 0
         ELSE ROUND(MONTHS_BETWEEN(day_dt, bsdt.row_eff_dt))
     END
         AS sscr_mth_in_dvc_cnt,
     delta.curr_subscriber_status_cd AS sscr_stat_cd
,'N' as OPT_CUST_BEST_EMAIL_DNC_IND,
dim_sscr.addr_fxd_wrls_block_grp_id,
dim_sscr.OPT_DNC_SMS_MDN_IND ,
NVL(pa.HSI_SSCR_Flag,'N') as HSI_SSCR_Flag
    FROM
     dwwrk.STG_PRE_CAR_DLY_DELTA delta
     INNER JOIN dim_customer_t dct
         ON delta.customer_id = dct.customer_id
            AND dct.curr_ver_flg = 'Y'
     LEFT JOIN dim_location_t dlt
         ON delta.sscr_location_id = dlt.location_id
            AND dlt.curr_ver_flg = 'Y'
     LEFT JOIN dim_customer_demo_t dcdt
         ON delta.customer_id = dcdt.cust_id
            AND dcdt.row_exp_dt = TO_DATE('12/31/9999', 'MM/DD/YYYY')
     LEFT JOIN brdg_sscr_t1_dvc_t1_t bsdt
         ON bsdt.sscr_typ1_key = delta.sscr_typ1_key
            AND bsdt.row_exp_dt = TO_DATE('12/31/9999', 'MM/DD/YYYY')
     LEFT JOIN pob_amt pa ON delta.sscr_typ1_key = pa.sscr_typ1_key
     LEFT JOIN promo_card pc_cust
         ON pc_cust.customer_id = delta.customer_id
            AND pc_cust.cust_rnk = 1
     LEFT JOIN fin_acct fa ON fa.customer_id = delta.customer_id
     LEFT JOIN dim_ctrc dc
         ON dc.master_subscriber_id = delta.master_subscriber_id
            AND dc.rnk = 1
     LEFT JOIN dvc_actvn da ON da.customer_id = delta.customer_id
     LEFT JOIN cust_event ce ON ce.customer_id = delta.customer_id
     LEFT JOIN sscr_event se
         ON se.customer_id = delta.customer_id
            AND se.master_subscriber_id = delta.master_subscriber_id
     LEFT JOIN new_cust nc ON nc.customer_id = delta.customer_id
     LEFT JOIN promo_card pc_sscr
         ON pc_sscr.customer_id = delta.customer_id
            AND pc_sscr.master_subscriber_id = delta.master_subscriber_id
     LEFT JOIN bods_upgd_touch_cnt bods ON bods.master_subscriber_id = delta.master_subscriber_id			
     LEFT JOIN FPS_SSCR_CMPGN_MART_MV
			ON delta.master_subscriber_id =  FPS_SSCR_CMPGN_MART_MV.MSTR_SSCR_ID
            AND DCT.customer_id = FPS_SSCR_CMPGN_MART_MV.cust_id
	LEFT JOIN dr_collection_sub_list dr
                  ON delta.master_subscriber_id = dr.master_subscriber_id
	LEFT JOIN dim_subscriber_t dim_sscr
	     on delta.master_subscriber_id = dim_sscr.master_subscriber_id and dim_sscr.row_exp_dt =
                               TO_DATE('12/31/9999', 'mm/dd/yyyy')
