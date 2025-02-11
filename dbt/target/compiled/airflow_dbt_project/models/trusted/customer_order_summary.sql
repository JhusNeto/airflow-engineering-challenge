-- Objetivo:
-- Agregação diária das vendas por categoria de produto. 
-- Essa tabela fornece, por dia, o total vendido, a média dos tickets e o número de transações.



with orders as (
    select 
        customer_id,
        sale_date,
        total_amount
    from "ecommerce"."stage"."carts"
)

select 
    customer_id,
    count(*) as num_orders,
    sum(total_amount) as total_spent,
    min(sale_date) as first_order,
    max(sale_date) as last_order
from orders
group by customer_id
order by total_spent desc