-- This test checks that early_stop_rate is always between 0 and 1

SELECT *
FROM {{ ref('mrt_ctgov_early_stop_analysis') }}
WHERE early_stop_rate IS NOT NULL
  AND (early_stop_rate < 0 OR early_stop_rate > 1)
