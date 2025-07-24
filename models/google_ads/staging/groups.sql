WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY id, campaign_id, customer_id, date, device
           ORDER BY cost_micros DESC  -- or impressions DESC
         ) AS row_num
  FROM {{ source('google_ads', 'ad_group_stats') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
