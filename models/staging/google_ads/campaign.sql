WITH ranked AS (
  SELECT id,name,status,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY updated_at DESC NULLS LAST  
         ) AS row_num
  FROM {{ source('google_ads', 'campaign_history') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
