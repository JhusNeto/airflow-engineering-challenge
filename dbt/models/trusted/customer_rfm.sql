/*
Modelo: Customer RFM (Recency, Frequency, Monetary)

Objetivo: 
Calcular métricas RFM para segmentação e análise do valor dos clientes.

Métricas calculadas:
- Recência (R): Número de dias desde a última compra do cliente
- Frequência (F): Quantidade total de compras realizadas pelo cliente
- Monetário (M): Valor total gasto pelo cliente em todas as compras

Tabelas fonte:
- stage.carts: Dados dos pedidos/carrinhos
- stage.customers: Dados cadastrais dos clientes

Regras de negócio:
- Considera apenas pedidos com status 'DELIVERED'
- Recência calculada em dias usando a data atual como referência
- Ordenação final por valor monetário (clientes mais valiosos primeiro)
*/

{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

-- CTE para filtrar apenas pedidos entregues e selecionar campos relevantes
with orders as (
    select 
        customer_id,
        sale_date,
        total_amount
    from {{ source('stage', 'carts') }}
    where lower(status) = 'delivered'  -- Filtra apenas pedidos entregues
),

-- CTE para calcular as métricas base do RFM por cliente
rfm as (
    select 
        customer_id,
        max(sale_date) as last_purchase,    -- Data da compra mais recente
        count(*) as frequency,              -- Total de compras
        sum(total_amount) as monetary       -- Valor total gasto
    from orders
    group by customer_id
),

-- CTE para adicionar o cálculo de recência em dias
rfm_calculated as (
    select
        customer_id,
        last_purchase,
        frequency,
        monetary,
        date_part('day', current_date - last_purchase) as recency  -- Dias desde última compra
    from rfm
)

-- Query final combinando métricas RFM com dados do cliente
select 
    c.id as customer_id,     -- ID único do cliente (PK)
    c.full_name,             -- Nome completo
    c.email,                 -- Email para contato
    r.recency,              -- Dias desde última compra
    r.frequency,            -- Número total de compras
    r.monetary              -- Valor total gasto
from rfm_calculated r
inner join {{ source('stage', 'customers') }} c on r.customer_id = c.id  -- Garante integridade referencial
order by r.monetary desc     -- Ordena por valor gasto (maior para menor)