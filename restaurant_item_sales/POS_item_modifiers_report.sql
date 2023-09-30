select *, 
total_consumption-(total_sold+total_marketing_comps+total_make_good_comps+total_test_order_comps+
	total_employee_meals+total_waste) as checksum
from (
	select 
		date(COALESCE(v.served_at_local, v.created_at_local)) as served_at_local,
		v.store_id,
		v.store_address,
		v.store_locality,
		line_items.item_id,
		line_items.item_name,
		'Item' as item_or_modifier,
		line_items.item_category,
		 sum(case when (total_charged_before_refund>0 and (total_food_revenue + total_beverage_revenue) - v.promo_redemption_amount > 0) OR v.promo_category ilike '%delivery%' or (num_items=num_free_water and v.promo_category is null) then 1 else 0 end) as total_sold,
		 sum(case when total_charged_to_card=0  and total_charged_before_refund=0 and v.promo_category in ('Marketing', 'Other') and line_items.reorder_id is null then 1 else 0 end) as total_marketing_comps,
		 sum(case when ((total_food_revenue + total_beverage_revenue) - v.promo_redemption_amount  <=0) and total_charged_before_refund=0 
				and (v.promo_category = 'Marketing - Customer Success' or (line_items.is_comped='true' and line_items.reorder_id is null) or v.is_comped=true) then 1 else 0 end) as total_make_good_comps,
		 sum(case when total_charged_to_card=0 and v.promo_category like '%Test%' then 1 else 0 end) as total_test_order_comps,
		 sum(case when total_charged_to_card=0 and total_charged_before_refund=0 and v.promo_category like '%Employee%' and line_items.reorder_id is null then 1 else 0 end) as total_employee_meals,
		 sum(case when (v.promo_redemption_amount=0 or (total_charged_to_card=0 and v.promo_category NOT ilike '%delivery%' and v.promo_category not ilike '%test%')) and line_items.reorder_id is not null 
				and total_charged_before_refund=0 then 1 else 0 end) as total_waste,
		 sum(1) as total_consumption
	FROM reporting.t_pos_orders AS v
	JOIN reporting.t_line_items AS line_items ON v.order_id = line_items.order_id
	where 1=1
		and v.order_status = 'delivered to customer'
		--AND param_date_filter
		and v.delivered_date>=current_date-interval '1 month'
	group by 1,2,3,4,5,6,7,8
	
	UNION
	select 
		date(COALESCE(v.served_at_local, v.created_at_local)) as served_at_local,
		v.store_id,
		v.store_address,
		v.store_locality,
		--md.name as item_type, 
		md.modifier_id,
		md.modifier_name,
		'Modifier' as item_or_modifier,
		line_items.item_category,
		 sum(case when (total_charged_before_refund>0 and (total_food_revenue + total_beverage_revenue) - v.promo_redemption_amount > 0) OR v.promo_category ilike '%delivery%' then 1 else 0 end) as total_sold,
		 sum(case when total_charged_to_card=0  and total_charged_before_refund=0 and v.promo_category in ('Marketing', 'Other') and line_items.reorder_id is null then 1 else 0 end) as total_marketing_comps,
		 sum(case when ((total_food_revenue + total_beverage_revenue) - v.promo_redemption_amount  <=0) and total_charged_before_refund=0 and (v.promo_category = 'Marketing - Customer Success' or (line_items.is_comped='true' and line_items.reorder_id is null) or v.is_comped=true) then 1 else 0 end) as total_make_good_comps,
		 sum(case when total_charged_to_card=0 and v.promo_category like '%Test%' then 1 else 0 end) as total_test_order_comps,
		 sum(case when total_charged_to_card=0 and total_charged_before_refund=0 and v.promo_category like '%Employee%' and line_items.reorder_id is null then 1 else 0 end) as total_employee_meals,
		 sum(case when (v.promo_redemption_amount=0 or (total_charged_to_card=0 and v.promo_category NOT ilike '%delivery%' and v.promo_category not ilike '%test%')) and line_items.reorder_id is not null and total_charged_before_refund=0 then 1 else 0 end) as total_waste,
		 sum(1) as total_consumption
	FROM reporting.t_pos_orders AS v
	JOIN reporting.t_line_items AS line_items ON v.order_id = line_items.order_id
	JOIN reporting.t_modifications AS md ON v.order_id=md.order_id AND line_items.line_item_id=md.line_item_id
	where 1=1 
		and v.order_status = 'delivered to customer'
		--AND param_date_filter
		and v.delivered_date>=current_date-interval '1 month'
		--AND date(v.delivered_at_local)=current_date-interval '1 day'
		--AND date_trunc('month',v.delivered_at_local)=date_trunc('month',current_date-interval '1 day')
	group by 1,2,3,4,5,6,7,8 
) p
