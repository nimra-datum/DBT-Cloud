WITH ranked AS (
    SELECT account_id, adset_id, date, country, region, dma, device_platform, publisher_platform, platform_position, spend, impressions, 
    ROW_NUMBER() OVER (PARTITION BY account_id, adset_id, country, region, date ORDER BY spend DESC NULLS LAST) AS row_num
    FROM {{ source('facebooks_ads', 'marketing_mix_modeling') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
