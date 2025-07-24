WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY base_ad_group_id
           ORDER BY updated_at DESC NULLS LAST  -- If available
         ) AS row_num
  FROM {{ source('google_ads', 'ad_group_history') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
