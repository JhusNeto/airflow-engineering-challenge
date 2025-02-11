-- Objetivo:
-- Calcular a tendência semanal de vendas por categoria de produto, mostrando o total vendido
-- a cada semana e a variação percentual em relação à semana anterior. Isso auxilia no 
-- acompanhamento do desempenho de vendas ao longo do tempo e permite identificar padrões
-- sazonais e categorias com melhor performance.

{{ config(materialized='table') }}

with sales as (
    select 
        sale_date::date as sale_date,
        items_product_id,
        total_amount
    from {{ source('stage', 'carts') }}
    where lower(status) = 'delivered'
),

products as (
    select 
        id,
        category
    from {{ source('stage', 'products') }}
),

-- Junta os dados de vendas com os produtos para obter a categoria
sales_with_category as (
    select
        s.sale_date,
        p.category,
        s.total_amount
    from sales s
    join products p on s.items_product_id = p.id
),

-- Agrega as vendas por semana e categoria
weekly_sales as (
    select
        date_trunc('week', sale_date) as week_start,
        category,
        sum(total_amount) as total_weekly_sales
    from sales_with_category
    group by 1, 2
),

-- Calcula a variação percentual em relação à semana anterior para cada categoria
sales_trend as (
    select
        week_start,
        category,
        total_weekly_sales,
        lag(total_weekly_sales) over (partition by category order by week_start) as prev_week_sales,
        case 
            when lag(total_weekly_sales) over (partition by category order by week_start) = 0 
                 then null 
            else round(((total_weekly_sales - lag(total_weekly_sales) over (partition by category order by week_start)) / lag(total_weekly_sales) over (partition by category order by week_start)) * 100, 2)
        end as pct_change
    from weekly_sales
)

select
    week_start,
    category,
    total_weekly_sales,
    prev_week_sales,
    pct_change
from sales_trend
order by week_start, category