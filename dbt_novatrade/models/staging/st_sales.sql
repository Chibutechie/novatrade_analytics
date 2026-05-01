select
    "TransactionID",
    "OrderDate",
    "CustomerID",
    "ProductID",
    "StoreID",
    "Quantity",
    "UnitPrice",
    "DiscountPct",
    "ReturnFlag",
    "ReturnDate",
    "ShipCost"
from {{ source('raw', 'ntg_sales') }}
