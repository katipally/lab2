{% snapshot moving_average_7d_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='symbol_date',  
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select
    symbol,
    date as symbol_date,
    close,
    moving_average_7d,
    current_timestamp() as updated_at
from {{ ref('moving_average_7d') }}

{% endsnapshot %}
