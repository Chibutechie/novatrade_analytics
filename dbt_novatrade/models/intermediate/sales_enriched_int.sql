with joined as (
    
    select
        s.trxn_id,
        s.order_date,
        s.customer_id,
        s.product_id,
        s.store_id,
        s.quantity,
        s.unit_price,
        s.discount,
        s.return_flag,
        s.return_date,
        p.category,
        p.sub_category,
        p.tier,
        o.region,
        o.store_type,
        o.country,
        s.unit_price * s.quantity                     as revenue_gross,
        p.cost_price * s.quantity                       as cogs

    from {{ ref('sales_stg') }}     s
    left join {{ ref('products_stg') }} p on s.product_id = p.product_id
    left join {{ ref('outlet_stg') }}   o on s.store_id   = o.store_id

)

select * from joined