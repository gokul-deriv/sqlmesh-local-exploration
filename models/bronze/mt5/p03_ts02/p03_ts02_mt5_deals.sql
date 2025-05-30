

MODEL (
  name mt5_real_p03_ts02.deals,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column timemsc,
    partition_by_time_column false
  ),
  partitioned_by (DATE_TRUNC(timemsc, MONTH)),
  start '2024-01-01',
  cron '*/5 * * * *',
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
FROM `data-tribe-production.mt5_prod_real_p03_ts02_fivetran_public.mt5_deals_*`
WHERE timemsc >= CAST(@start_ts AS DATETIME)
  AND timemsc <= CAST(@end_ts AS DATETIME)