
  
    

  create  table "ecommerce"."trusted"."product_discount_analysis__dbt_tmp"
  
  
    as
  
  (
    -- Objetivo:
-- Analisar o uso de descontos nos produtos, 
-- mostrando o desconto médio aplicado, o total faturado (após desconto)
-- e o número de pedidos por produto. 
-- Essa tabela ajuda a entender a estratégia de descontos e sua influência nas vendas.



with discount as (
    select 
        items_product_id,
        items_discount,
        total_amount
    from "ecommerce"."stage"."carts"
)

select 
    items_product_id,
    avg(items_discount) as avg_discount,
    sum(total_amount) as total_revenue,
    count(*) as num_orders
from discount
group by items_product_id
order by total_revenue desc
  );
  