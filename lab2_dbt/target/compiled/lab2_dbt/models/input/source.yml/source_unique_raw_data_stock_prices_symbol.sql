
    
    

select
    symbol as unique_field,
    count(*) as n_records

from lab2_db.raw_data.stock_prices
where symbol is not null
group by symbol
having count(*) > 1


