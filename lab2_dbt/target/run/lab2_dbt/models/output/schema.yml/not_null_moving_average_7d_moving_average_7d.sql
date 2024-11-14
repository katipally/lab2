select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select moving_average_7d
from lab2_db.analytics.moving_average_7d
where moving_average_7d is null



      
    ) dbt_internal_test