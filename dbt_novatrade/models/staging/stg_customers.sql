with source as (
    select * from {{ source('raw', 'ntg_customers') }}
),

cleaned as (
    select 
        "CustomerID"::varchar   as customerid,
        "Segment"::varchar      as segment,    
        "LoyaltyTier"::varchar  as loyaltytier,
        "Country"::varchar      as country,
        "Region"::varchar       as region,
        "JoinDate"::date        as joindate,
        "Channel"::varchar      as channel,
	concat("FirstName", ' ', "LastName") as FullName
    from source
    where "CustomerID" is not null
)

select * from cleaned
