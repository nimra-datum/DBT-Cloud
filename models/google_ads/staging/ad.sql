WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY ad_id, ad_group_id, campaign_id, customer_id, date, device
           ORDER BY cost_micros DESC
         ) AS row_num
  FROM {{ source('google_ads', 'ad_stats') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
