
    
    

select
    date as unique_field,
    count(*) as n_records

from lab2_db.analytics.rsi_7d
where date is not null
group by date
having count(*) > 1


