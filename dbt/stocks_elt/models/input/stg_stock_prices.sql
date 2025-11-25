select
    symbol,
    open,
    close,
    volume,  
    date
from {{ source('raw', 'stock_prices') }}