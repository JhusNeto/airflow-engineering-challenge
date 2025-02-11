-- Objetivo:
-- Calcular métricas de RFM (Recência, Frequência e Valor Monetário) para cada cliente,
-- permitindo identificar os clientes mais valiosos e seu comportamento de compra.
-- 
-- Recência: Tempo desde a última compra
-- Frequência: Quantidade total de compras
-- Monetário: Valor total gasto em compras

{{ config(materialized='table') }}

-- Obtemos os pedidos (carrinhos) com status 'DELIVERED'
with orders as (
    select 
        customer_id,
        sale_date,
        total_amount
    from {{ source('stage', 'carts') }}
    where lower(status) = 'delivered'
),

-- Agregamos para calcular a última compra, quantidade de pedidos e total gasto
rfm as (
    select 
        customer_id,
        max(sale_date) as last_purchase,
        count(*) as frequency,
        sum(total_amount) as monetary
    from orders
    group by customer_id
),

-- Calculamos a recência com base na data atual (a execução pode ser parametrizada)
rfm_calculated as (
    select
        customer_id,
        last_purchase,
        frequency,
        monetary,
        date_part('day', current_date - last_purchase) as recency
    from rfm
)

select 
    c.id as customer_id,
    c.full_name,
    c.email,
    r.recency,
    r.frequency,
    r.monetary
from rfm_calculated r
left join {{ source('stage', 'customers') }} c on r.customer_id = c.id
order by r.monetary desc