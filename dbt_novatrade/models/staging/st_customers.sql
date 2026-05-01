select 
        "CustomerID",
        "Segment",
        "LoyaltyTier",
        "Country",
        "Region",
        "JoinDate",
        "Channel",
        concat("FirstName", ' ', "LastName") as FullName
from {{ source('raw', 'ntg_customers') }}
