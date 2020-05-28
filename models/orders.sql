{{
  config(
    materialized='view'
  )
}}
with payments as (
    select * from {{ ref('stg_payment') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select orders.order_id, orders.customer_id, sum(amount)
    from orders
    join payments using(order_id)
    group by orders.order_id, orders.customer_id
)

select * from final