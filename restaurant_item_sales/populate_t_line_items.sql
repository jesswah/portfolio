drop table if exists reporting.t_line_items;
CREATE TABLE if not exists reporting.t_line_items (
	order_id			varchar(180),
	user_id				varchar(180),
	user_account			varchar(1020),
	visit_number			integer,
	store_id			varchar(180),
	store_address			varchar(1020),
	store_locality			varchar(1020),
	order_status			varchar(40),
	created_at_local		timestamp,
	updated_at_local		timestamp,
	queued_at_local			timestamp,
	started_at_local		timestamp,
	served_at_local			timestamp,
	delivered_at_local		timestamp,
	order_ahead_sched_time		timestamp,
	order_channel			varchar(20),
	daypart				varchar(20),
	line_item_id			varchar(180),
	item_id				varchar(180),
	item_name			varchar(1020),
	item_type			varchar(180),
	item_subtype			varchar(180),
	item_category			varchar(180),
	is_employee			boolean,
	is_test_order			boolean,
	promo_code			varchar(20),
	promo_category		 	varchar(40),
	is_reorder			integer,
	item_total			integer,
	item_taxes			integer,
	net_sales			integer,
	reorder_id			varchar(180),
	is_finance			integer,
	repeat_order			integer,
	email_receipt_rating		varchar(20),
	food_score			integer,
	ordering_exp_score	 	integer,
	store_exp_score 		integer
)
distkey(order_id)
sortkey(created_at_local,store_address);
TRUNCATE TABLE reporting.t_line_items;

INSERT INTO reporting.t_line_items
SELECT
	o.order_id
	,o.user_id
	,o.user_account
	,r.visit_number
	,o.store_id
	,o.store_address
	,o.store_locality
	,o.order_status
	,o.created_at_local
	,o.updated_at_local
	,o.queued_at_local
	,o.started_at_local
	,o.served_at_local
	,o.delivered_at_local
	,o.order_ahead_sched_time
	,o.order_channel
	,o.daypart
	,l.id as line_item_id
	,l.item_id
	,l.name as item_name
	,l.item_type
	,l.item_subtype 
	,case when l.item_type='drink' and (l.name ilike '%water%' or l.name ilike '%coke%' or l.name ilike '%ginger ale%' or l.name ilike '%root beer%'
		or l.name='Sprite' or l.name ilike '%iced tea%' or l.name='Lemonade' or l.name='Pellegrino' or l.name ilike '%iced coffee%') then 'Cold Beverage'
		when l.item_type='drink' then 'Hot Beverage'
		when l.name ilike '%bao%' and l.item_type='side' then 'Bao'
		when l.item_type='side' then 'Donut'
		when l.item_type='entree' then  
			case when l.name ilike '%broth%' or l.name ilike '%soup%' then 'Soups'
			when l.name ilike '%potstickers%' or l.name ilike '%dumplings%' then 'Potstickers/Dumplings'
			when l.name ilike '%salad%' then 'Salad'
			when l.item_subtype='bowl' or l.name='All Vegetable' then 'Bowl'
			when l.item_subtype ilike '%breakfast%' then 'Breakfast Bowl' else 'Unknown' end
		else'Unknown' end as item_category
	,o.is_employee
	,o.is_test_order
	,o.promo_code
	,o.promo_category
	,o.is_reorder
	,l.item_total
	,l.line_item_total_taxes as item_taxes
	,case when o.order_channel='delivery' then coalesce(l.gross_total,0)
		when l.is_refunded='true' then 0 else coalesce(l.line_item_total,0) end as net_sales
	,l.reorder_id
	,o.is_finance
	,sum(case when l.reorder_id is null then 1 else 0 end) over (partition by o.user_account order by o.created_at_local rows between unbounded preceding and current row) as repeat_order
	,r.email_receipt_rating
	,r.food_score
	,r.ordering_exp_score
	,r.store_exp_score
	
FROM reporting.t_pos_orders o
	LEFT OUTER JOIN (
		SELECT
			order_id
			,visit_number
			,email_receipt_rating
			,food_score
			,store_exp_score
			,ordering_exp_score
		FROM reporting.t_orders
		) r on o.order_id=r.order_id
	LEFT OUTER JOIN (
		SELECT
			lines.id
			,lines.order_id
			,lines.is_comped
			,coalesce(lines.line_item_total,0) as item_total
			,lines.item_id
			,lines.is_refunded
			,lines.line_item_total_taxes
			,i.item_type
			,i.name,i.effective_date
			,i.expiration_date
			,i.item_subtype
			,coalesce(lines.gross_total,0) as gross_total
			,lines.line_item_total,lines.reorder_id
		FROM hub.line_items lines
		LEFT OUTER JOIN reference.items i ON lines.item_id=i."id"
			AND lines.created_at>=i.effective_date
			AND lines.created_at<i.expiration_date
	) l ON o.order_id=l.order_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,35,36,37,38,o.user_account,o.created_at_local
;
