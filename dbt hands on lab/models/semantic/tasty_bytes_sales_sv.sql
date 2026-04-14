{{ config(materialized='semantic_view') }}

TABLES(
    ORDERS      as {{ ref('stg_order_header') }}  primary key (ORDER_ID),
    ORDER_ITEMS as {{ ref('stg_order_details') }} primary key (ORDER_ID, MENU_ITEM_ID),
    MENU        as {{ source('tasty_bytes', 'raw_pos_menu') }} primary key (MENU_ITEM_ID)
)

RELATIONSHIPS(
    ORDERS_TO_ITEMS as ORDER_ITEMS(ORDER_ID)     references ORDERS(ORDER_ID),
    ITEMS_TO_MENU   as ORDER_ITEMS(MENU_ITEM_ID) references MENU(MENU_ITEM_ID)
)

FACTS(
    ORDER_ITEMS.QUANTITY        as QUANTITY,
    ORDER_ITEMS.LINE_TOTAL      as LINE_TOTAL,
    ORDERS.ORDER_NET_TOTAL      as ORDER_NET_TOTAL
)

DIMENSIONS(
    ORDERS.ORDER_TS             as ORDER_TS,
    ORDERS.TRUCK_ID             as TRUCK_ID,
    MENU.TRUCK_BRAND_NAME       as TRUCK_BRAND_NAME,
    MENU.MENU_TYPE              as MENU_TYPE,
    MENU.ITEM_CATEGORY          as ITEM_CATEGORY,
    MENU.ITEM_NAME              as ITEM_NAME
)

METRICS(
    ORDER_ITEMS.TOTAL_REVENUE as SUM(LINE_TOTAL)
        WITH SYNONYMS = ('revenue', 'total sales', 'sales amount'),
    ORDER_ITEMS.TOTAL_ITEMS_SOLD as SUM(QUANTITY)
        WITH SYNONYMS = ('items sold', 'unit sales', 'quantity sold'),
    ORDER_ITEMS.TOTAL_ORDERS as COUNT(DISTINCT ORDER_ID)
        WITH SYNONYMS = ('number of orders', 'order count', 'orders')
)
