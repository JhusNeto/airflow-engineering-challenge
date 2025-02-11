/*
Modelo: Desempenho Logístico (Logistics Performance)

Objetivo: 
Calcular métricas de desempenho logístico para análise da eficiência das transportadoras.

Métricas calculadas:
- Tempo médio de entrega em dias por transportadora e tipo de serviço
- Quantidade total de entregas realizadas

Tabelas fonte:
- stage.carts: Dados dos pedidos e informações de entrega
- stage.logistics: Dados das transportadoras e serviços

Regras de negócio:
- Considera apenas pedidos com status 'DELIVERED'
- Ignora registros sem data de venda ou entrega
- Ordenação por tempo médio de entrega (mais rápidos primeiro)
*/

{{ config(
    materialized='table',
    unique_key='logistic_id'
) }}

-- CTE para extrair dados relevantes de entregas concluídas
with shipments as (
    select 
        logistic_id,
        sale_date,
        shipping_info_tracking_history_updated_at::timestamp as delivery_date  -- Converte data de entrega para timestamp
    from {{ source('stage', 'carts') }}
    where lower(status) = 'delivered'  -- Filtra apenas entregas concluídas
    and logistic_id is not null  -- Garante integridade referencial
),

-- CTE para calcular o tempo de entrega em dias para cada envio
shipping_times as (
    select
        logistic_id,
        date_part('day', delivery_date - sale_date) as delivery_days  -- Calcula diferença em dias
    from shipments
    where delivery_date is not null and sale_date is not null  -- Remove registros com datas nulas
)

-- Query final agregando métricas por transportadora e tipo de serviço
select
    l.id as logistic_id,              -- ID único da transportadora (PK)
    l.company_name,                    -- Nome da empresa transportadora
    l.service_type,                    -- Tipo de serviço oferecido
    round(avg(s.delivery_days)::numeric, 2) as avg_delivery_days,  -- Tempo médio de entrega
    count(*) as num_shipments          -- Total de entregas realizadas
from {{ source('stage', 'logistics') }} l  -- Tabela principal para garantir todas as transportadoras
left join shipping_times s on s.logistic_id = l.id  -- Left join para manter transportadoras sem entregas
group by l.id, l.company_name, l.service_type
order by avg_delivery_days  -- Ordena do mais rápido para o mais lento