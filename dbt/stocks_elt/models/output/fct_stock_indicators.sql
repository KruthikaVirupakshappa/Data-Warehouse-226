with base as (
  select
    symbol,
    date,
    close,
    volume
  from {{ ref('stg_stock_prices') }}
),

-- 1) compute lag once
lagged as (
  select
    symbol,
    date,
    close,
    volume,
    lag(close) over (partition by symbol order by date) as prev_close
  from base
),

-- 2) compute rolling metrics, but only over plain columns from this layer
rolled as (
  select
    symbol,
    date,
    close,
    prev_close,
    volume,
    avg(close) over (
      partition by symbol
      order by date
      rows between 19 preceding and current row
    ) as sma_20,
    sum(close) over (
      partition by symbol
      order by date
      rows between 49 preceding and current row
    ) as sum_50
  from lagged
)

select
  symbol,
  date,
  close,
  prev_close,
  volume,
  close - prev_close as day_change,
  sma_20,
  sum_50
from rolled
