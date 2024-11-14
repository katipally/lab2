
  create or replace   view lab2_db.analytics.moving_average_7d
  
   as (
    select
    symbol,
    date,
    close,
    avg(close) over (
        partition by symbol
        order by date
        rows between 6 preceding and current row
    ) as moving_average_7d
from lab2_db.analytics.input_stock_data
  );

