DROP TABLE IF EXISTS reporting.t_pos_orders;
CREATE TABLE if not exists reporting.t_pos_orders (
delivered_date 			date,
order_id 			varchar(180),
store_id 			varchar(180),
user_account 			varchar(1020),
user_id				varchar(180),
store_address 			varchar(1020),
store_locality 			varchar(1020),
order_status 			varchar(40),
order_notes			varchar(65535),
created_at 					timestamp,
created_at_local		timestamp,
updated_at_local		timestamp,
queued_at_local 		timestamp,
started_at_local 		timestamp,
served_at_local 		timestamp,
delivered_at_local		timestamp,
order_ahead_sched_time 		timestamp,
fulfillment_time 		integer,
initial_eta 			integer,
absolute_eta_error		integer,
order_channel 			varchar(20),
daypart 			varchar(12),
payment_type 			varchar(12),
payment_processor 		varchar(10),
is_employee 			boolean,
is_test_order			boolean,
is_comped 			boolean,
is_refunded 			boolean,
promo_code			varchar(20),
promo_category 			varchar(40),
num_food_items 			integer,
num_drinks 			integer,
num_free_water 			integer,
num_items 			integer,
total_charged_before_refund	decimal(38,2),
total_charged_to_card		decimal(38,2),
total_gross_revenue		decimal(38,2),
total_food_revenue		decimal(38,2),
total_beverage_revenue		decimal(38,2),
promo_redemption_amount		decimal(38,2),
comp_amount_without_tax		decimal(38,2),
total_refund_amount		decimal(38,2),
total_sales_tax 		decimal(38,2),
total_food_sales_tax		decimal(38,2),
total_beverage_sales_tax	decimal(38,2),
total_bottled_water_tax		decimal(38,2),
total_refunded_taxes		decimal(38,2),
imputed_sales_tax		decimal(38,2),
gross_sales_without_taxes	decimal(38,2),
net_sales 			decimal(38,2),
food_net_sales			decimal(38,2),
beverage_net_sales		decimal(38,2),
checksum 			decimal(38,2),
is_reorder			integer,
is_water_only_order		integer,
is_finance 			integer
)
distkey(order_id)
sortkey(created_at_local,store_address);

TRUNCATE TABLE reporting.t_pos_orders;

INSERT INTO reporting.t_pos_orders

with v_pos_orders as (
SELECT
l.order_id
,o.store_id
,coalesce(u.in_store_receipt_email::text,o.user_id::text) as user_account
,o.user_id
,s.address as store_address
,s.locality as store_locality
,CASE o.status WHEN 0 THEN 'in queue' 
	WHEN 100 THEN 'scheduled' 
	WHEN 300 THEN 'on the line' 
	WHEN 400 THEN 'ready for pickup' 
	WHEN 500 THEN 'delivered to customer' 
	WHEN 700 THEN 'customer cancelled' 
	WHEN 800 THEN 'attendant cancelled'  
	WHEN 900 THEN 'hold for recubby' 
	WHEN 1000 THEN 'ready to recubby' end as order_status
,o.notes as order_notes
,o.created_at
,convert_timezone(s.timezone,o.created_at) as created_at_local
,convert_timezone(s.timezone,o.updated_at) as updated_at_local
,convert_timezone(s.timezone,o.queued_at) as queued_at_local
,convert_timezone(s.timezone,o.started_at) as started_at_local
,convert_timezone(s.timezone,o.served_at) as served_at_local
,convert_timezone(s.timezone,o.delivered_at) as delivered_at_local
,convert_timezone(s.timezone,o.scheduled_time) as order_ahead_sched_time
,DATEDIFF ('second',COALESCE(o.scheduled_to_fire_at, o.queued_at), o.served_at) AS "fulfillment_time"
,o.initial_eta
,case when o.initial_eta is not null then abs(DATEDIFF ('second',COALESCE(o.scheduled_to_fire_at, o.queued_at), o.served_at)-o.initial_eta) end as absolute_eta_error
,case when p.category=1200 and p.redemption_code<>'WOWBAO' then 'delivery'
	when o.is_mobile='true' then 'mobile'
	else 'kiosk' end as order_channel
,case when date_part('hour',convert_timezone(s.timezone,o.delivered_at))+(date_part('minute',convert_timezone(s.timezone,o.delivered_at))/60.0)<10.5 then 'Breakfast'
	when date_part('hour',convert_timezone(s.timezone,o.delivered_at))+(date_part('minute',convert_timezone(s.timezone,o.delivered_at))/60.0)>=16.5 then 'Dinner'
	when o.delivered_at is null and o.scheduled_to_fire_at is null then null else 'Lunch'
	end as daypart
,case f.cc_type when 0 then 'visa'
  when 100 then 'amex'
  when 200 then 'master card'
  when 300 then 'discover'
  when 400 then 'diners club'
  when 500 then 'maestro'
  when 600 then  'jcb'
  else 'unknown' end as payment_type
,case when o.payment_processor=300 then 'vantiv'
  when o.payment_processor=100 then 'braintree'
  else 'other' end as payment_processor
,case when p.category=400 or p.redemption_code='WOWBAO' then true else false end as is_employee
,case when p.category=300 then true else false end as is_test_order
,case when l.is_comped='true' then true else false end as is_comped
,case when o.is_refunded='true' then true else false end as is_refunded
,case when pa.redemption_amount>0 then p.redemption_code end as promo_code
,case when p.redemption_code is not null and pa.redemption_amount>0 then
		case when p.category=1200 and p.redemption_code<>'WOWBAO' then 'Third Party Delivery'
			when p.category=100 and p.multi_usage_authorization_cap=1 then 'Marketing - Customer Success'
			when p.category=100 then 'Marketing'
			when p.category=300 then 'Testing'
			when p.category=400 OR p.redemption_code='WOWBAO' then 'Employee Discount'
			else 'Unknown' end
		else null end as promo_category
,sum(case when l.item_type='entree' or l.item_type='side' then 1 else 0 end) as num_food_items
,sum(case when l.item_type='drink' then 1 else 0 end) as num_drinks
,sum(case when l.name='Free H20' then 1 else 0 end) as num_free_water
,sum(case when l.item_type in ('entree','side','drink') then 1 else 0 end) as num_items
,((max(o.total) + max(coalesce(r.refund_total,0)))/100.0)::decimal(38,2) as total_charged_before_refund
,(max(o.total)/100.0)::decimal(38,2) as total_charged_to_card
,((max(o.total_without_adjustments) + max(coalesce(r.refund_total,0)) - max(coalesce(r.refund_taxes,0)))/100.0)::decimal(38,2) as total_gross_revenue --without tax
,(sum(case when l.item_type='side' or l.item_type='entree' then coalesce(l.gross_total,0) else 0 end)/100.0)::decimal(38,2) as total_food_revenue
,(sum(case when l.item_type='drink' then coalesce(l.gross_total,0) else 0 end)/100.0)::decimal(38,2) as total_beverage_revenue
,(max(coalesce(pa.redemption_amount,0))/100.0)::decimal(38,2) as promo_redemption_amount
,((sum(coalesce(l.item_total,0))-max(o.total_without_adjustments)-(max(coalesce(r.refund_total,0))-max(coalesce(r.refund_taxes,0))))/100.0)::decimal(38,2) as comp_amount_without_tax
,(max(coalesce(r.refund_total,0))/100.0)::decimal(38,2) as total_refund_amount
,((max(o.total_taxes) + max(coalesce(r.refund_taxes,0)))/100.0)::decimal(38,2) as total_sales_tax
,(sum(case when l.item_type='side' or l.item_type='entree' then coalesce(l.line_item_total_taxes,0) else 0 end)/100.0)::decimal(38,2) as total_food_sales_tax
,(sum(case when l.name ilike '%bottled%' then ceil(coalesce(l.line_item_total,0)*.115)
    when l.item_type='drink' then coalesce(l.line_item_total_taxes,0) else 0 end)/100.0)::decimal(38,2) as total_beverage_sales_tax
,(sum(case when l.name ilike '%bottled%' then ceil(coalesce(l.line_item_total,0)*.0325) else 0 end)/100.0)::decimal(38,2) as total_bottled_water_tax
,(max(coalesce(r.refund_taxes,0))/100.0)::decimal(38,2) as total_refunded_taxes
,(sum(coalesce(l.item_taxes,0))/100.0)::decimal(38,2) as imputed_sales_tax
,(sum(coalesce(l.item_total,0))/100.0)::decimal(38,2) as gross_sales_without_taxes
,(sum(case when p.category=1200 and p.redemption_code<>'WOWBAO' then coalesce(l.gross_total,0)
  when l.is_refunded='true' then 0 else coalesce(l.line_item_total,0) end)/100.0)::decimal(38,2) as net_sales
,(sum(case when l.item_type='entree' or l.item_type='side' then
    case when p.category=1200 and p.redemption_code<>'WOWBAO' then coalesce(l.gross_total,0)
    when l.is_refunded='true' then 0 else coalesce(l.line_item_total,0) end
    else 0 end)/100.0)::decimal(38,2) as food_net_sales
,(sum(case when l.item_type='drink' then
    case when p.category=1200 and p.redemption_code<>'WOWBAO' then coalesce(l.gross_total,0)
    when l.is_refunded='true' then 0 else coalesce(l.line_item_total,0) end
    else 0 end)/100.0)::decimal(38,2) as beverage_net_sales

FROM "hub".orders o
LEFT OUTER JOIN (
  SELECT
    lines.id
    ,lines.order_id
    ,lines.is_comped
    ,coalesce(lines.gross_total,0) as gross_total
    ,lines.item_id,coalesce(mods.mods_price,0) as mods_price
    ,lines.line_item_total
    ,lines.line_item_total_taxes
    ,lines.is_refunded
    ,i.item_type
    ,i.price
    ,i.name
    ,i.effective_date
    ,i.expiration_date
    ,i.price+coalesce(mods.mods_price,0) as item_total
    ,ceil((case when i.name ilike '%bottled%' then 0.1475 else 0.1150 end)*(i.price+coalesce(mods.mods_price,0))) as item_taxes
  FROM "hub".line_items lines
  LEFT OUTER JOIN reference.items i ON lines.item_id=i."id"
  	AND lines.created_at>=i.effective_date
		AND lines.created_at<i.expiration_date
  LEFT OUTER JOIN (
    SELECT
      ms.line_item_id
      ,sum(coalesce(md.price,0)) as mods_price
    FROM "hub".modifications ms LEFT OUTER JOIN reference.modifiers md on ms.modifier_id=md.id
      and ms.created_at>=md.effective_date 
      and ms.created_at<md.expiration_date
    GROUP BY 1
    )mods ON lines.id=mods.line_item_id
 ) l ON o.id=l.order_id
LEFT OUTER JOIN reference.stores s on o.store_id=s.id
	AND o.created_at>=s.effective_date
	AND o.created_at<s.expiration_date
LEFT OUTER JOIN hub.users u on o.user_id=u.id
LEFT OUTER JOIN "hub".funds f ON o.user_id=f.user_id
LEFT OUTER JOIN "hub".refunds r ON o.id=r.order_id
LEFT OUTER JOIN "hub".promo_authorizations pa ON o.id=pa.order_id AND o.user_id=pa.user_id
LEFT OUTER JOIN hub.promos p ON pa.promo_id=p."id"
WHERE convert_timezone(s.timezone,o.delivered_at) >= '20171127'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
)

SELECT
date(delivered_at_local) as delivered_date
,v_pos_orders.*
,(total_charged_before_refund + promo_redemption_amount) - (total_food_revenue + total_beverage_revenue + total_sales_tax) as checksum
,coalesce(is_reorder,0) as is_reorder
,case when num_free_water=num_items then 1 else 0 end as is_water_only_order
,case when is_employee =false and is_comped=false and total_charged_before_refund>0 and order_status= 'delivered to customer' and payment_processor is not null
and payment_type !='Unknown Card Type' then 1 else 0 end as is_finance
from v_pos_orders
LEFT JOIN
  (SELECT
    DISTINCT line_items.reorder_id,
    1 AS is_reorder
   FROM hub.line_items) reorder ON reorder.reorder_id::text = v_pos_orders.order_id::text
;
