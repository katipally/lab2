create or replace database lab2_db;
create or replace schema analytics;
create or replace schema raw_data;

use schema raw_data;
CREATE OR REPLACE TABLE raw_data.stock_prices (
    date DATE PRIMARY KEY,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT,
    symbol STRING
);

select * from stock_prices;

CREATE OR REPLACE TABLE raw_data.stock_forecasts (
    date DATE PRIMARY KEY,
    close FLOAT,
    symbol STRING
);

select * from stock_forecasts order by date;

use schema analytics;
select * from input_stock_data order by date desc;

select * from rsi_7d order by date desc,symbol;

select * from MOVING_AVERAGE_7D order by date desc,symbol;

select * from MOVING_AVERAGE_7D_SNAPSHOT order by symbol_date desc;
select * from RSI_7D_SNAPSHOT order by symbol_date desc;
