with source as (
    select *  from {{ source('raw', 'ntg_products')}}
),

cleaned as (
    select
        "ProductID"::varchar as productid,
        "ProductName"::varchar as productname,
        "Category"::varchar as category,
        "SubCategory"::varchar as subcategory,
        "Brand"::varchar as brand,
        "CostPrice"::float as unitprice,
        "Tier"::varchar as tier
    from source 
    where "ProductID" is not null
)

select * from cleaned