with source as (
    select * from {{ source('raw', 'ntg_sales') }}
),

cleaned as (
    select
        "TransactionID"::varchar as trxid,
        "OrderDate"::date as orderdate,
        "CustomerID"::varchar as customerid,
        "ProductID"::varchar as productid,
        "StoreID"::varchar as storeid,
        "Quantity"::int as quantity,
        round("UnitPrice"::numeric, 2) as unitprice,
        round(("DiscountPct"::numeric / 100), 4) as discountpct,
        "ReturnFlag"::boolean as returnflag,
        "ReturnDate"::date as returndate,
        round("ShipCost"::numeric, 2) as shipcost

    from {{ source('raw', 'ntg_sales') }}
)

select * from cleaned