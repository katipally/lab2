select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select close
from lab2_db.raw_data.stock_prices
where close is null



      
    ) dbt_internal_test