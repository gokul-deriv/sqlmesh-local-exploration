

MODEL (
  name mt5_real_p03_ts01.deals,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column timemsc,
    partition_by_time_column false
  ),
  partitioned_by (DATE_TRUNC(timemsc, MONTH)),
  column_descriptions (
    deal = 'Unique identifier for the deal (change desc)',
    timestamp = 'Timestamp of the deal'
  ),
  start '2024-01-01',
  cron '*/5 * * * *',
  signals [
    check_source_readiness(
      source_table := '`data-tribe-production.mt5_prod_real_p03_ts01_fivetran_public.mt5_deals_*`',
      -- source_table := '`data-tribe-sandbox-438709.mt5_real_p03_ts02.deals`',
      time_column := 'timemsc',
      time_column_type := 'DATETIME',
    ),

  ],
  interval_unit 'five_minute',
  grain (deal)
);

SELECT
    deal,
    timestamp,
    externalid,
    login,
    dealer,
    `order`,
    action,
    entry,
    reason,
    digits,
    digitscurrency,
    contractsize,
    time,
    timemsc,
    symbol,
    price,
    volumeext,
    profit,
    storage,
    commission,
    fee,
    rateprofit,
    ratemargin,
    expertid,
    positionid,
    comment,
    profitraw,
    priceposition,
    pricesl,
    pricetp,
    volumeclosedext,
    tickvalue,
    ticksize,
    flags,
    value,
    gateway,
    pricegateway,
    modifyflags,
    marketbid,
    marketask,
    marketlast,
    volume,
    volumeclosed
FROM `data-tribe-production.mt5_prod_real_p03_ts01_fivetran_public.mt5_deals_*`
WHERE timemsc >= CAST(@start_ts AS DATETIME)
  AND timemsc <= CAST(@end_ts AS DATETIME)