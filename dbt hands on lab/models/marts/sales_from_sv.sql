{{ config(materialized='view') }}

select *
from semantic_view(
    {{ ref('tasty_bytes_sales_sv') }}
    METRICS TOTAL_REVENUE, TOTAL_ITEMS_SOLD, TOTAL_ORDERS
    DIMENSIONS TRUCK_BRAND_NAME, MENU_TYPE, ITEM_CATEGORY, ORDER_TS
)
