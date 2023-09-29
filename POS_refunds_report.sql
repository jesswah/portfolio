SELECT
r.order_id
,o.store_id
,o.store_address
,o.store_locality
,r.id as refund_id
,r.created_at::timestamptz AT TIME ZONE 'CST' as refund_created_at_local
,r.updated_at::timestamptz AT TIME ZONE 'CST' as refund_updated_at_local
,o.order_status
,o.created_at_local
,o.updated_at_local
,o.served_at_local
,o.delivered_at_local
,o.order_channel
,o.daypart
,o.order_notes as refund_reason
,o.payment_type 
,case when r.refund_payment_processor=300 then 'vantiv' 
	when r.refund_payment_processor=100 then 'braintree'
	else 'other' end as refund_payment_processor 
,case when r.refunded_payment_processor=300 then 'vantiv' 
	when r.refunded_payment_processor=100 then 'braintree'
	else 'other' end as refunded_payment_processor 
,o.is_employee
,o.is_comped
,o.promo_code
,o.promo_category 
,o.total_charged_before_refund
,o.total_charged_to_card
,o.promo_redemption_amount
,(max(coalesce(r.refund_total,0))/100.0)::decimal(38,2) as total_refund_amount
,(max(coalesce(r.refund_taxes,0))/100.0)::decimal(38,2) as total_refunded_taxes

FROM hub.refunds r 
LEFT OUTER JOIN reporting.t_pos_orders o ON r.order_id=o.order_id
--WHERE param_date_filter
--WHERE date(r.updated_at::timestamptz AT TIME ZONE 'CST') = current_date-interval '1 day'
WHERE date_trunc('month',r.updated_at::timestamptz AT TIME ZONE 'CST') = date_trunc('month',current_date-interval '1 day')
AND date(o.created_at_local)>='2017-11-27'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25