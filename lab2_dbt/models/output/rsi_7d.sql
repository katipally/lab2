with price_changes as (
    select
        symbol,
        date,
        close,
        lag(close) over (partition by symbol order by date) as previous_close,
        case
            when close > lag(close) over (partition by symbol order by date) then close - lag(close) over (partition by symbol order by date)
            else 0
        end as gain,
        case
            when close < lag(close) over (partition by symbol order by date) then lag(close) over (partition by symbol order by date) - close
            else 0
        end as loss
    from {{ ref('input_stock_data') }}
),
avg_gains_losses as (
    select
        symbol,
        date,
        avg(gain) over (
            partition by symbol
            order by date
            rows between 6 preceding and current row
        ) as avg_gain,
        avg(loss) over (
            partition by symbol
            order by date
            rows between 6 preceding and current row
        ) as avg_loss
    from price_changes
)
select
    symbol,
    date,
    100 - (100 / (1 + (avg_gain / nullif(avg_loss, 0)))) as rsi_7d
from avg_gains_losses
