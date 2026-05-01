select
    "StoreID",
    "StoreName",
    "StoreType",
    "City",
    "Country",
    "Region",
    "OpenDate",
    "SquareFootage",
    "StoreManager"
from {{ source('raw', 'ntg_stores') }}
