-- Objetivo:
-- Calcular o desempenho logístico medindo o tempo médio de entrega (em dias) para cada transportadora,
-- agrupado pelo serviço prestado. Essa métrica pode ajudar a identificar qual transportadora ou 
-- serviço é mais eficiente.



with shipments as (
    select 
        logistic_id,
        sale_date,
        -- Converter a coluna de entrega para timestamp (assumindo que os dados já estejam no formato correto)
        shipping_info_tracking_history_updated_at::timestamp as delivery_date
    from "ecommerce"."stage"."carts"
    where lower(status) = 'delivered'
),

shipping_times as (
    select
        logistic_id,
        date_part('day', delivery_date - sale_date) as delivery_days
    from shipments
    where delivery_date is not null and sale_date is not null
)

select
    l.id as logistic_id,
    l.company_name,
    l.service_type,
    round(avg(s.delivery_days)::numeric, 2) as avg_delivery_days,
    count(*) as num_shipments
from shipping_times s
join "ecommerce"."stage"."logistics" l on s.logistic_id = l.id
group by l.id, l.company_name, l.service_type
order by avg_delivery_days