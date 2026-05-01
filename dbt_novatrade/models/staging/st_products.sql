select
    "ProductID",
    "ProductName",
    "Category",
    "SubCategory",
    "Brand",
    "CostPrice",
    "Tier"
from {{ source('raw', 'ntg_products') }}
