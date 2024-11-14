select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select moving_average_7d as from_field
    from lab2_db.analytics.moving_average_7d
    where moving_average_7d is not null
),

parent as (
    select  as to_field
    from lab2_db.analytics.input_stock_data
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test