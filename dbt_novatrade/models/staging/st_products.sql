with source as (
    select *  from {{ source('raw', 'ntg_products')}}
),

cleaned as (
    select
        "ProductID"::varchar as product_id,
        "ProductName"::varchar as product_name,
        "Category"::varchar as category,
        "SubCategory"::varchar as sub_category,
        "Brand"::varchar as brand,
        round("CostPrice"::numeric, 2) as cost_price,
        "Tier"::varchar as tier
    from source 
    where "ProductID" is not null
)

select * from cleaned