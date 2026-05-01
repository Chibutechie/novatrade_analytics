with source as (
    select * from {{ source('raw', 'ntg_sales') }}
),

cleaned as (
    select
        "TransactionID"::varchar as trxn_id,
        "OrderDate"::date as order_date,
        "CustomerID"::varchar as customer_id,
        "ProductID"::varchar as product_id,
        "StoreID"::varchar as store_id,
        "Quantity"::int as quantity,
        round("UnitPrice"::numeric, 2) as unit_price,
        round(("DiscountPct"::numeric / 100), 4) as discount,
        "ReturnFlag"::boolean as return_flag,
        "ReturnDate"::date as return_date,
        round("ShipCost"::numeric, 2) as ship_cost

    from {{ source('raw', 'ntg_sales') }}
)

select * from cleaned