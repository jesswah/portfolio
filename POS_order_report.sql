SELECT 
delivered_date
,order_id
,store_id
,store_address
,store_locality
,order_status
,created_at_local
,updated_at_local
,served_at_local
,delivered_at_local
,order_channel
,daypart
,case when order_channel='delivery' then 'delivery' else 'dine-in' end as order_type
,payment_type
,payment_processor
,is_employee
,is_comped
,is_refunded
,promo_code
,promo_category
,num_food_items
,num_drinks
,num_items
,total_charged_before_refund
,total_charged_to_card
,total_gross_revenue
,total_food_revenue
,total_beverage_revenue
,promo_redemption_amount
,comp_amount_without_tax
,total_refund_amount
,total_sales_tax
,total_food_revenue
,total_beverage_sales_tax
,total_bottled_water_tax
,total_refunded_taxes
,imputed_sales_tax
,gross_sales_without_taxes
,net_sales
,food_net_sales
,beverage_net_sales
,checksum

FROM reporting.t_pos_orders
--where param_date_filter
where date(delivered_at_local)>=current_date-interval '30 days'
order by 7 desc