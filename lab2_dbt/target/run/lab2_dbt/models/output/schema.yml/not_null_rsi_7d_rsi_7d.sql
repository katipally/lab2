select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select rsi_7d
from lab2_db.analytics.rsi_7d
where rsi_7d is null



      
    ) dbt_internal_test