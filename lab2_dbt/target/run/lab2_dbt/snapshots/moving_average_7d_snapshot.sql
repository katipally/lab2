
      
  
    

        create or replace transient table lab2_db.snapshots.moving_average_7d_snapshot
         as
        (
    

    select *,
        md5(coalesce(cast(symbol_date as varchar ), '')
         || '|' || coalesce(cast(updated_at as varchar ), '')
        ) as dbt_scd_id,
        updated_at as dbt_updated_at,
        updated_at as dbt_valid_from,
        nullif(updated_at, updated_at) as dbt_valid_to
    from (
        



select
    symbol,
    date as symbol_date,
    close,
    moving_average_7d,
    current_timestamp() as updated_at
from lab2_db.analytics.moving_average_7d

    ) sbq



        );
      
  
  