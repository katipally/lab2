select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from lab2_db.analytics.moving_average_7d
where symbol is null



      
    ) dbt_internal_test