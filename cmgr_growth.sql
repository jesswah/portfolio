/************************************************
CMGR - Compounded Monthly Growth Rate
    Using Realized gross total 
************************************************/

WITH shift_bill_data AS ( 
  SELECT 
    LSB.id AS shift_bill_id
    , shift_agreement_id 
    --, LSB.fee_total_amount_cents 
    , LSB.payor_total_amount_cents 
    , SA.start_time_local 
    , FA.system_or_facility_type
  FROM 
    {{ @latest_shift_bill AS LSB }}
    JOIN 
      {{ @facility_attributes AS FA }}
        USING (facility_id)
    JOIN  
      api.shift_agreements AS SA
        ON LSB.shift_agreement_id = SA.id
    LEFT JOIN 
      api.work_proposals AS WP 
        ON SA.work_proposal_id = WP.id
  WHERE 
    LSB.payor = 1  --facility payors
    AND SA.state = 3  -- worked
    AND WP.work_type < 4  -- regular shifts
    AND LSB.state = 2  -- closed 
    AND LSB.bill_type = 2  -- real
    AND DATE_TRUNC('month', SA.start_time_local) < DATE_TRUNC('month', current_date) -- only data for complete months
)
, monthly_data AS (
  SELECT 
    DATE_TRUNC('month', start_time_local) AS month
    , system_or_facility_type
    , SUM(SB.payor_total_amount_cents) / 100. AS gtv_realized
  FROM 
    shift_bill_data AS SB
  GROUP BY 
    DATE_TRUNC('month', start_time_local)
    , system_or_facility_type
)
-- Aggregates for 3, 6, 12 month rolling periods
, rolling_data AS (
  SELECT 
    *
    , LAG(gtv_realized, 3) OVER W AS gtv_3mo_prev
    , LAG(gtv_realized, 6) OVER W AS gtv_6mo_prev
    , LAG(gtv_realized, 12) OVER W AS gtv_12mo_prev
  FROM 
    monthly_data
  WINDOW 
    W AS (PARTITION BY system_or_facility_type ORDER BY month)
)
-- Union CMGR values for graphing purposes
, cmgr_data AS ( 
  SELECT 
    month
    , system_or_facility_type
    , 'CMGR 3' AS metric
    , CASE 
        WHEN gtv_3mo_prev IS NOT NULL THEN (1. * gtv_realized / gtv_3mo_prev) ^ (1./3) - 1 
        ELSE NULL 
        END AS cmgr_value
  FROM 
    rolling_data
  
  UNION
  SELECT 
    month
    , system_or_facility_type
    , 'CMGR 6'
    , CASE 
        WHEN gtv_6mo_prev IS NOT NULL THEN (1. * gtv_realized / gtv_6mo_prev) ^ (1./6) - 1 
        ELSE NULL 
        END
  FROM 
    rolling_data
  
  UNION
  SELECT 
    month
    , system_or_facility_type
    , 'CMGR 12'
    , CASE 
        WHEN gtv_12mo_prev IS NOT NULL THEN (1. * gtv_realized / gtv_12mo_prev) ^ (1./12) - 1 
        ELSE NULL 
        END 
  FROM 
    rolling_data
)
SELECT 
  * 
FROM (
  SELECT 
    month 
    , system_or_facility_type
    , gtv_realized
  FROM 
    rolling_data
  ) AS G
  LEFT JOIN 
    cmgr_data AS C
      USING (month, system_or_facility_type)
