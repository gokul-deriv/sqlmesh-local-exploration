

MODEL (
  name mt5_prod.union_deals,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column time_deal,
    partition_by_time_column false,
    forward_only true,
    on_destructive_change warn
  ),
  partitioned_by (DATE_TRUNC(time_deal, MONTH)),
  start '2024-01-01',
  cron '*/5 * * * *',
  grain (deal),
);

JINJA_QUERY_BEGIN;
{% set platforms = ['p03_ts01', 'p03_ts02'] %}

WITH deals AS (

  {% for platform in platforms %}

    SELECT *, '{{ platform }}' AS srvid
    FROM mt5_real_{{ platform }}.deals
    WHERE timemsc >= CAST(@start_ts AS DATETIME)
      AND timemsc <= CAST(@end_ts AS DATETIME)

    {% if not loop.last %}
        UNION ALL
    {% endif %}

  {% endfor %}

)

SELECT
  deal,
  login,
  dealer,
  timemsc AS time_deal,
  `order`,
  `action`,
  entry,
  digits,
  digitscurrency AS digits_currency,
  contractsize AS contract_size,
  price,
  volume,
  volumeext AS volume_ext,
  volumeclosed AS volume_closed,
  volumeclosedext AS volume_closed_ext,
  profit,
  storage,
  commission,
  rateprofit AS rate_profit,
  ratemargin AS rate_margin,
  expertid AS expert_id,
  positionid AS position_id,
  profitraw AS profit_raw,
  priceposition AS price_position,
  tickvalue AS tick_value,
  ticksize AS tick_size,
  flags,
  reason,
  pricegateway AS price_gateway,
  symbol,
  comment,
  gateway,
  modifyflags AS modification_flags,
  externalid AS external_id,
  pricesl AS price_stop_loss,
  pricetp AS price_take_profit,
  0 AS mt5_operation,
  'NA' AS audit_action,
  srvid,
  DATETIME_TRUNC(timemsc, MONTH) AS year_month,
  volume AS volume_usd,
  symbol AS symbol_path,
  profit AS profit_usd,
  storage AS storage_usd,
  commission AS commission_usd,
  price AS price_usd,
  'MT5' AS cfd_platform
FROM deals

JINJA_END;