with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
order_payment as (
    select customer_id, order_id, total from {{ ref('orders') }}
),
customer_orders as (

    select
        orders.customer_id,

        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders,
        sum(order_payment.total) as lifetime_value

    from orders
    join order_payment 
    on orders.customer_id = order_payment.customer_id and orders.order_id = order_payment.order_id

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        customer_orders.lifetime_value,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final  
