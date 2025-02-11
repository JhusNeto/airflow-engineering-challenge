-- Objetivo:
-- Resumir os dados por método de pagamento, 
-- mostrando quantas transações ocorreram, 
-- o total faturado e o ticket médio por método. 
-- Isso pode revelar quais canais de pagamento são mais eficientes ou populares.



with payments as (
    select 
        payment_info_method as payment_method, 
        total_amount
    from "ecommerce"."stage"."carts"
)

select 
    payment_method,
    count(*) as num_transactions,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_transaction
from payments
group by payment_method
order by total_revenue desc