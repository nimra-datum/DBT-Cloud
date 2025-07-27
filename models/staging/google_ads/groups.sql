WITH ranked AS (
  SELECT id,name,status,campaign_id,
         ROW_NUMBER() OVER (
           PARTITION BY id, name, campaign_id, status
         ) AS row_num
  FROM {{ source('google_ads', 'ad_group_history') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
