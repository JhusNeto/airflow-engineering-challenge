-- Objetivo:
-- Criar um resumo do comportamento dos clientes, 
-- agrupando o número de pedidos, total gasto, 
-- data da primeira e da última compra para cada cliente. 
-- Isso ajuda a identificar clientes recorrentes, o ticket médio e a evolução das compras.



with carts as (
    select 
        sale_date::date as sale_date,
        items_product_id,
        total_amount
    from "ecommerce"."stage"."carts"
),
products as (
    select 
        id,
        category
    from "ecommerce"."stage"."products"
)

select 
    c.sale_date as date,
    p.category,
    sum(c.total_amount) as total_sales,
    avg(c.total_amount) as avg_ticket,
    count(c.items_product_id) as num_transactions
from carts c
join products p on c.items_product_id = p.id
group by 1, 2
order by 1, 2