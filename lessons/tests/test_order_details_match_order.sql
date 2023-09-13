{{config(severity = 'error')}}

WITH order_details AS (
SELECT
	order_id,
	COUNT(*) AS num_of_items_in_order

FROM {{ref('stg_ecommerce__order_items')}}
GROUP BY 1

)


SELECT
	o.order_id,
	o.num_of_item,
	od.num_of_items_in_order


FROM {{ref("stg_ecommerce__orders")}} as o

FULL OUTER JOIN order_details AS od USING (order_id)

WHERE
	od.order_id IS NULL
	OR o.order_id IS NULL
	OR o.num_of_item != od.num_of_items_in_order

