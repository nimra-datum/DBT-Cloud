WITH ranked AS (
    SELECT ad_id, account_id, ad_name,adset_name, impressions, date, cpc, cpm, ctr, frequency, spend, reach,
    ROW_NUMBER() OVER (PARTITION BY ad_id,account_id, date ORDER BY spend DESC NULLS LAST) AS row_num
    FROM {{ source('facebooks_ads', 'basic_ad') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
