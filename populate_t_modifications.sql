CREATE TABLE IF NOT EXISTS reporting.t_modifications (
	order_id							varchar(180),
	user_id								varchar(180),
	user_account					varchar(1020),
	visit_number					integer,
	modification_id 			varchar(180),
	modifier_id 					varchar(180),
	modifier_name 				varchar(1020),
	modification_total 		integer,
	line_item_id 					varchar(180),
	item_name 						varchar(1020),
	item_type							varchar(180),
	item_category					varchar(180),
	store_id							varchar(180),
	store_address					varchar(1020),
	store_locality				varchar(1020),
	created_at_local			timestamp,
	updated_at_local			timestamp,
	is_employee						boolean,
	is_test_order					boolean,
	is_comped							boolean,
	is_refunded						boolean,
	has_reorder						boolean,
	item_total						integer,
	item_taxes						integer,
	item_net_sales				integer,
	reorder_id						varchar(180),
	repeat_order					integer
)

distkey(line_item_id)
sortkey(created_at_local,store_id);

TRUNCATE TABLE reporting.t_modifications;

INSERT INTO reporting.t_modifications

SELECT
l.order_id
,l.user_id
,l.user_account
,l.visit_number
,mods.id as modification_id
,mods.modifier_id
,mds."name" as modifier_name
,mods.modification_total
,l.line_item_id
,l.item_name
,l.item_type
,l.item_category
,l.store_id
,l.store_address
,l.store_locality
,convert_timezone('CDT',mods.created_at) as created_at_local
,convert_timezone('CDT',mods.updated_at) as updated_at_local
,l.is_employee
,l.is_test_order
,l.is_comped
,l.is_refunded
,l.has_reorder
,l.item_total
,l.item_taxes
,l.net_sales as item_net_sales
,l.reorder_id
,l.repeat_order
FROM reporting.t_line_items l
JOIN hub.modifications mods on l.line_item_id=mods.line_item_id
LEFT OUTER JOIN reference.modifiers mds on mods.modifier_id=mds.id 
	AND mods.created_at>=mds.effective_date
	AND mods.created_at<mds.expiration_date
ORDER BY 1,7,5
;