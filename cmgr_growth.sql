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

*********************************************************************************************

Business Growth - Filled Shifts 

WITH shift_bill_data AS ( 
  SELECT 
    DATE_TRUNC('month', SA.start_time_local) AS month
    , FA.system_or_facility_type
    , COALESCE(FA.hospital_id, FA.facility_id) AS facility_id 
    , COUNT(DISTINCT SA.id) AS shifts
    , MIN(SA.end_time_local) AS first_monthly
    , SUM(LSB.payor_total_amount_cents) / 100. AS gtv_realized
  FROM 
    {{ @latest_shift_bill AS LSB }}
    JOIN 
      {{ @facility_attributes AS FA }}
        USING(facility_id)
    JOIN  
      api.shift_agreements AS SA
        ON LSB.shift_agreement_id = SA.id
    JOIN 
      api.work_proposals AS WP 
        ON SA.work_proposal_id = WP.id 
  WHERE 
    LSB.payor = 1  --facility payors
    AND SA.state = 3  -- worked
    AND WP.work_type < 4  -- regular shifts
    AND LSB.state = 2  -- closed 
    AND LSB.bill_type = 2  -- real
    AND DATE_TRUNC('month', SA.start_time_local) < DATE_TRUNC('month', current_date)  -- only data for complete months
  GROUP BY 
    DATE_TRUNC('month', SA.start_time_local)
    , FA.system_or_facility_type
    , COALESCE(FA.hospital_id, FA.facility_id)
) 
, facility_start_dates AS (
  SELECT 
    facility_id 
    , system_or_facility_type
    , MIN(first_monthly) AS first_shift
  FROM 
    shift_bill_data 
  GROUP BY 
    facility_id
    , system_or_facility_type
)
-- series of all possible months, to force 0's into months w/o postings (for churned/resurrected)
, month_series AS (
  SELECT 
    *
    , generate_series(DATE_TRUNC('month', first_shift), DATE_TRUNC('month', current_date - interval '1 month'), '1 month') AS month
  FROM 
    facility_start_dates
)
, facility_data AS (
  SELECT 
    MS.*
    , COALESCE(SB.shifts, 0) AS shifts
    , COALESCE(SB.gtv_realized, 0) AS gtv_realized
    -- for getting status of facility; shift expansion/contraction calc
    , COALESCE(LAG(SB.shifts) OVER W, 0) AS prev_month
    , COALESCE(LAG(SB.shifts, 2) OVER W, 0) AS prev_2_mo
    -- for expansion/contraction calculation
    , COALESCE(LAG(SB.gtv_realized) OVER W, 0) AS prev_gtv_realized
    , COALESCE(LAG(SB.gtv_realized, 2) OVER W, 0) AS prev_2_mo_gtv
  FROM 
    month_series AS MS
    LEFT JOIN
      shift_bill_data AS SB 
        USING (facility_id, month, system_or_facility_type)
  WINDOW 
    W AS (PARTITION BY MS.facility_id ORDER BY MS.month)
)
, monthly_data AS ( 
  SELECT DISTINCT
    month 
    , facility_id
    , system_or_facility_type
    , CASE 
        WHEN DATE_TRUNC('month', first_shift) = month THEN 'New'
        WHEN shifts > 0 AND prev_month = 0 THEN 'Reactivated' -- or is it defined by gtv not shift posting?
        WHEN shifts = 0 AND prev_month > 0 THEN 'Churned'
        WHEN shifts = 0 THEN 'Inactive'
        WHEN shifts > 0 AND gtv_realized >= prev_gtv_realized THEN 'Expansion'
        WHEN shifts > 0 AND gtv_realized < prev_gtv_realized THEN 'Contraction'
        ELSE 'Other'
        END AS facility_status
    , CASE 
        WHEN DATE_TRUNC('month', first_shift) = month - interval '1 month' THEN 'New' -- should it be first filled (billed) shift?
        WHEN prev_month > 0 AND prev_2_mo = 0 THEN 'Reactivated'
        WHEN prev_month = 0 AND prev_2_mo > 0 THEN 'Churned'
        WHEN prev_month = 0 THEN 'Inactive'
        WHEN prev_month > 0 AND prev_gtv_realized >= prev_2_mo_gtv THEN 'Expansion'
        WHEN prev_month > 0 AND prev_gtv_realized < prev_2_mo_gtv THEN 'Contraction'
        ELSE 'Other'
        END AS previous_status
    , CASE 
        WHEN DATE_TRUNC('month', first_shift) = month THEN 'New'
        WHEN gtv_realized > 0 AND prev_gtv_realized = 0 THEN 'Reactivated' -- or is it defined by gtv not shift posting?
        WHEN gtv_realized = 0 AND prev_gtv_realized > 0 THEN 'Churned'
        WHEN gtv_realized = 0 THEN 'Inactive'
        WHEN gtv_realized > 0 AND gtv_realized >= prev_gtv_realized THEN 'Expansion'
        WHEN gtv_realized > 0 AND gtv_realized < prev_gtv_realized THEN 'Contraction'
        ELSE 'Other'
        END AS gtv_status
    , CASE 
        WHEN DATE_TRUNC('month', first_shift) = month - interval '1 month' THEN 'New'
        WHEN prev_gtv_realized > 0 AND prev_2_mo_gtv = 0 THEN 'Reactivated'
        WHEN prev_gtv_realized = 0 AND prev_2_mo_gtv > 0 THEN 'Churned'
        WHEN prev_gtv_realized = 0 THEN 'Inactive'
        WHEN prev_gtv_realized > 0 AND prev_gtv_realized >= prev_2_mo_gtv THEN 'Expansion'
        WHEN prev_gtv_realized > 0 AND prev_gtv_realized < prev_2_mo_gtv THEN 'Contraction'
        ELSE 'Other'
        END AS previous_gtv_status
    , gtv_realized
    , prev_gtv_realized
  FROM 
    facility_data
)
, assets_and_liabilities AS (
  SELECT 
    month 
    , system_or_facility_type
    , SUM(gtv_realized - prev_gtv_realized) FILTER (WHERE gtv_status IN ('New', 'Reactivated', 'Expansion')) AS assets 
    , SUM(prev_gtv_realized - gtv_realized) FILTER (WHERE gtv_status IN ('Contraction', 'Churned')) AS liabilities
  FROM 
    monthly_data
  GROUP BY 
    month 
    , system_or_facility_type
)
, quick_ratios AS (
  SELECT
    month 
    , system_or_facility_type
    , SUM(assets) OVER W * 1. / SUM(liabilities) OVER W AS quick_ratio_3mo
    , assets * 1. / liabilities AS quick_ratio
  FROM 
    assets_and_liabilities
  WINDOW
    W AS (PARTITION BY system_or_facility_type ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
, ratios AS ( 
  SELECT 
    month
    , system_or_facility_type
    , SUM(prev_gtv_realized - gtv_realized) FILTER (WHERE gtv_status IN ('Churned', 'Contraction', 'Expansion')) * 1. 
        / NULLIF(SUM(prev_gtv_realized), 0) AS net_churned_gtv
    -- MoM retention of facilities (based on posting/not posting shifts; or gtv)
    , (SUM(prev_gtv_realized) - SUM(prev_gtv_realized - gtv_realized) FILTER (WHERE gtv_status IN ('Churned', 'Contraction'))) * 1.
        / NULLIF(SUM(prev_gtv_realized), 0) AS retention_gtv
    , COUNT(DISTINCT facility_id) FILTER (WHERE facility_status IN ('Expansion', 'Contraction') AND previous_status IN ('New', 'Expansion', 'Contraction', 'Reactivated')) * 1. 
        / NULLIF(COUNT(DISTINCT facility_id) FILTER (WHERE previous_status IN ('New', 'Expansion', 'Contraction', 'Resurrected')), 0) AS retention 
    -- Ratio of facilities gained / lost
    , COUNT(DISTINCT facility_id) FILTER (WHERE facility_status IN ('New', 'Reactivated')) * 1. 
        / NULLIF(COUNT(DISTINCT facility_id) FILTER (WHERE facility_status = 'Churned'), 0) AS quick_ratio 
  FROM 
    monthly_data
  GROUP BY 
    month
    , system_or_facility_type
)
, facility_counts AS (
  SELECT 
    month 
    , system_or_facility_type
    , facility_status AS status
    , COUNT(DISTINCT facility_id) * CASE WHEN facility_status = 'Churned' THEN -1 ELSE 1 END AS facility_count
  FROM 
    monthly_data 
  WHERE 
    facility_status <> 'Inactive'
  GROUP BY 
    month 
    , system_or_facility_type
    , facility_status
)
  -- data using GTV to determine facility status
, gtv_totals AS ( 
  SELECT 
    month
    , system_or_facility_type
    , gtv_status AS status
    , SUM(gtv_realized) - SUM(prev_gtv_realized) gtv_growth
    , SUM(gtv_realized) AS current_gtv
    , SUM(prev_gtv_realized) AS prev_gtv
  FROM 
    monthly_data
  WHERE 
    gtv_status <> 'Inactive'
  GROUP BY 
    month 
    , system_or_facility_type
    , gtv_status
)
SELECT
  COALESCE(FC.month, R.month) AS month
  , COALESCE(FC.system_or_facility_type, R.system_or_facility_type) AS system_or_facility_type
  , FC.status AS "Status"
  , FC.facility_count AS "Facility Count"
  , R.quick_ratio AS "Quick Ratio"
  , R.retention AS "Retention"
  , GTV.current_gtv AS "Current GTV"
  , GTV.prev_gtv AS "Previous GTV"
  , GTV.gtv_growth AS "GTV Growth"
  , QR.quick_ratio AS "GTV Quick Ratio"
  , QR.quick_ratio_3mo AS "GTV 3m Quick Ratio"
  , R.net_churned_gtv AS "GTV Net Churn"
  , R.retention_gtv AS "GTV Retention"
FROM 
  ratios AS R
  JOIN
    facility_counts AS FC 
      USING (month, system_or_facility_type)
  LEFT JOIN 
    quick_ratios AS QR 
      USING (month, system_or_facility_type)
  LEFT JOIN 
    gtv_totals AS GTV
      ON R.month = GTV.month
      AND FC.status = GTV.status
      AND FC.system_or_facility_type = GTV.system_or_facility_type
