
  create or replace   view lab2_db.analytics.input_stock_data
  
   as (
    select * from lab2_db.raw_data.stock_prices
  );

