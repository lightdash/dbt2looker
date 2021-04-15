{{
  config(
    materialized='table'
  ) 
}}

WITH stage_1 AS (

  SELECT
    *,
    timestamp AS viewed_at,
    DATE(timestamp) AS date,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp desc) as dedupe_helper
  FROM {{ source('landing_page', 'pages') }}

)

SELECT * EXCEPT(dedupe_helper)
FROM stage_1
WHERE dedupe_helper = 1
