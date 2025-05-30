MODEL (
  name mt5_prod.union_users,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key login,
    forward_only true,
    on_destructive_change warn
  ),
  start '2024-01-01',
  cron '*/5 * * * *',
  grain (login),
);
JINJA_QUERY_BEGIN;
{%- set platforms = ['p03_ts01', 'p03_ts02'] %}

with unioned as (
-- rename as per bi.mt5_users
{%- for platform in platforms %}
  SELECT NULL AS binary_user_id
       , CAST(login AS INT64) AS login
       , NULL AS srvid
       , account
       , NULL AS username
       , CAST(agent AS INT64) AS agent
       , NULL AS audit_action
       , balance
       , balanceprevday AS balance_prev_day
       , balanceprevmonth AS balance_prev_month
       , CAST(leverage AS INT64) AS leverage
       , credit
       , interestrate AS interest_rate
       , commissiondaily AS commission_daily
       , commissionmonthly AS commission_monthly
       , NULL AS commission_agent_daily
       , NULL AS commission_agent_monthly
       , equityprevday AS equity_prev_day
       , equityprevmonth AS equity_prev_month
       , `group`
       , lastip AS last_ip
       , company
       , country
       , email
       , status
       , comment AS mtcomment
       , leadsource AS lead_source
       , leadcampaign AS lead_campaign
       , TIMESTAMP(lastpasschange) AS last_pass_change_ts
       , TIMESTAMP(registration) AS registration_ts
       , TIMESTAMP(lastaccess) AS last_access_ts
       , city
       , state
       , zipcode AS post_code
       , address
       , phone
       , id
       , mqid AS metaquotes_id
       , CAST(rights AS INT64) AS rights
       , CAST(certserialnumber AS STRING) AS cert_serial_number
       , CAST(color AS INT64) AS color
       , NULL AS color_hex
       , NULL AS color_name
       , NULL AS mt5_operation
       , NULL AS insert_time
       , NULL AS is_connect_enabled
       , NULL AS is_change_password_allowed
       , NULL AS is_trade_disabled
       , NULL AS is_investor
       , NULL AS is_certification_confirmed
       , NULL AS is_trailing_stops_allowed
       , NULL AS is_expert_advisors_allowed
       , NULL AS is_obsolete
       , NULL AS is_trade_reports_allowed
       , NULL AS is_readonly
       , NULL AS is_reset_password
       , NULL AS is_otp_enabled
       , NULL AS is_sponsored_hosting_allowed
       , NULL AS is_api_enabled
       , NULL AS is_archived
       , NULL AS is_synthetic
       , NULL AS is_financial
       , NULL AS is_coverage
       , CASE WHEN `group` LIKE '%svg%' THEN 'DSVG'
              WHEN `group` LIKE '%bvi%' THEN 'DBVI'
              WHEN `group` LIKE '%labuan%' THEN 'DFX'
              WHEN `group` LIKE '%vanuatu%' THEN 'DVL'
              WHEN `group` LIKE '%maltainvest%' THEN 'DIEL'
              WHEN `group` LIKE '%dml%' THEN 'DML'
              WHEN `group` LIKE '%iom%' THEN 'BIOM'
              WHEN `group` LIKE '%malta_std_eur%' THEN 'DEL'
              WHEN `group` LIKE '%dcl%' THEN 'DCL'
              WHEN `group` IN ('real\\feed_for_demo_server', 'real\\costarica') THEN 'LEGACY'
         ELSE NULL
         END AS landing_company
       , NULL AS first_deposit_ts
       , NULL AS first_trade_ts
       , NULL AS first_take_profit_ts
       , NULL AS first_sell_loss_ts
       , NULL AS first_stop_out_ts
       , NULL AS first_withdrawal_ts
       , NULL AS last_deposit_ts
       , TIMESTAMP_MICROS(timestamptrade) AS last_trade_ts
       , NULL AS last_withdrawal_ts
       , NULL AS currency
       , NULL AS is_anonymized
       , NULL AS cfd_platform
       , 'MTR' AS login_prefix
       , NULL AS balance_usd
       , NULL AS balance_prev_day_usd
       , NULL AS balance_prev_month_usd
       , NULL AS region
       , NULL AS sub_region
       , NULL AS is_internal_client
       , NULL AS is_failed
       , CONCAT('MTR', CAST(login AS STRING)) AS loginid
  FROM `data-tribe-production.mt5_prod_real_{{ platform }}_fivetran_public.mt5_users`
  WHERE _fivetran_synced >= @start_ts
    AND _fivetran_synced <= @end_ts
  {%- if not loop.last %} UNION ALL {% endif %}
{%- endfor %}

)

SELECT * FROM unioned

JINJA_END;