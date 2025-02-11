-- Objetivo:
-- Analisar o desempenho logístico através da agregação diária
-- dos envios por status de rastreamento. Esta tabela fornece
-- uma visão consolidada da quantidade de pedidos em cada etapa
-- do processo de entrega, permitindo identificar gargalos e
-- monitorar a eficiência da operação logística.



with shipments as (
    select 
        sale_date::date as sale_date,
        shipping_info_tracking_history_status as tracking_status
    from "ecommerce"."stage"."carts"
)

select 
    sale_date,
    tracking_status,
    count(*) as num_shipments
from shipments
group by sale_date, tracking_status
order by sale_date, tracking_status