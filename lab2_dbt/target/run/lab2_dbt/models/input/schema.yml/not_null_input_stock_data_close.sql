select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select close
from lab2_db.analytics.input_stock_data
where close is null



      
    ) dbt_internal_test