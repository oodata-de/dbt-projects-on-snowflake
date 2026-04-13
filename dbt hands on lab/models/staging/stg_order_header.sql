with source as (
    select * from {{ source('tasty_bytes', 'raw_pos_order_header') }}
),

cleaned as (
    select
        order_id,
        truck_id,
        order_ts,
        customer_id,
        order_discount_amount as order_discount,
        order_tax_amount as order_tax,
        order_total as order_net_total,
        _fivetran_synced as loaded_at
    from source
    where order_id is not null
)

select * from cleaned
