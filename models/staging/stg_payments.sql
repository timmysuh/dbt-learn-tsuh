select
    id,
    orderid as order_id,
    paymentmethod,
    status,
    amount/100 as amount,
    created

from raw.stripe.payment 