WITH ranked AS (
    SELECT account_id,id as campaign_id, name,status, 
    ROW_NUMBER() OVER (PARTITION BY id, account_id ORDER BY updated_time DESC NULLS LAST) AS row_num
    FROM {{ source('facebooks_ads', 'campaign_history') }}
)

SELECT campaign_id,name,status
FROM ranked
WHERE row_num = 1
