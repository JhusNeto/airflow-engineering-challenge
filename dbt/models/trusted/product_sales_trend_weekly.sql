/*
Modelo: Tendência Semanal de Vendas por Categoria de Produto

Objetivo: 
Calcular a tendência semanal de vendas por categoria de produto, mostrando o total vendido
a cada semana e a variação percentual em relação à semana anterior.

Métricas calculadas:
- Total de vendas por semana e categoria
- Variação percentual em relação à semana anterior
- Vendas da semana anterior (para comparação)

Tabelas fonte:
- stage.carts: Dados dos pedidos/carrinhos
- stage.products: Dados dos produtos e categorias

Regras de negócio:
- Considera apenas pedidos com status 'DELIVERED'
- Agrupamento semanal usando início da semana como referência
- Ordenação por data e categoria
*/

{{ config(materialized='table') }}

-- CTE para extrair dados relevantes de vendas concluídas
with sales as (
    select 
        sale_date::date as sale_date,      -- Converte para data
        items_product_id,                   -- ID do produto vendido
        total_amount                        -- Valor total da venda
    from {{ source('stage', 'carts') }}
    where lower(status) = 'delivered'       -- Filtra apenas vendas entregues
),

-- CTE para obter categorias dos produtos
products as (
    select 
        id,
        category
    from {{ source('stage', 'products') }}
),

-- Combina dados de vendas com categorias dos produtos
sales_with_category as (
    select
        s.sale_date,
        p.category,
        s.total_amount
    from sales s
    join products p on s.items_product_id = p.id
),

-- Agrega vendas por semana e categoria
weekly_sales as (
    select
        date_trunc('week', sale_date) as week_start,  -- Início da semana
        category,
        sum(total_amount) as total_weekly_sales       -- Total vendido na semana
    from sales_with_category
    group by 1, 2
),

-- Calcula variação percentual em relação à semana anterior
sales_trend as (
    select
        week_start,
        category,
        total_weekly_sales,
        lag(total_weekly_sales) over (
            partition by category 
            order by week_start
        ) as prev_week_sales,               -- Vendas da semana anterior
        case 
            when lag(total_weekly_sales) over (
                partition by category 
                order by week_start
            ) = 0 then null 
            else round(((total_weekly_sales - lag(total_weekly_sales) over (
                partition by category 
                order by week_start
            )) / lag(total_weekly_sales) over (
                partition by category 
                order by week_start
            )) * 100, 2)
        end as pct_change                   -- Variação percentual
    from weekly_sales
)

-- Query final com todas as métricas calculadas
select
    week_start,                            -- Data de início da semana
    category,                              -- Categoria do produto
    total_weekly_sales,                    -- Total vendido na semana
    prev_week_sales,                       -- Vendas da semana anterior
    pct_change                             -- Variação percentual
from sales_trend
order by week_start, category              -- Ordena por data e categoria