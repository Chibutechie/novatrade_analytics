with source as (
    select * from {{ source('raw', 'ntg_stores') }}
),

cleaned as (
    select
        "StoreID"::varchar as store_id,
        "StoreName"::varchar as store_name,
        "StoreType"::varchar as store_type,
        "City"::varchar as city,
        "Country"::varchar as country,
        "Region"::varchar as region,
        "OpenDate"::date as open_date,
        "SquareFootage"::int as square_foot,
        "StoreManager"::varchar as store_manager
    from source
    where "StoreID" is not null
)

select * from cleaned
