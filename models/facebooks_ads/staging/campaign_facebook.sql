WITH ranked AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY campaign_id, account_id, date ORDER BY spend DESC NULLS LAST) AS row_num
    FROM {{ source('facebooks_ads', 'basic_campaign') }}
)

SELECT 
    campaign_id, account_id, impressions, date, cpc, cpm, ctr, frequency, spend, campaign_name, reach
FROM ranked
WHERE row_num = 1
