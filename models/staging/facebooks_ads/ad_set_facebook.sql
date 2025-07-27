WITH ranked AS (
    SELECT adset_id, adset_name, campaign_name, account_id, date, impressions, reach, cpc, cpm, ctr, frequency, spend, inline_link_clicks as clicks,
    ROW_NUMBER() OVER (PARTITION BY adset_id, account_id, campaign_name, date ORDER BY spend DESC NULLS LAST) AS row_num
    FROM {{ source('facebooks_ads', 'basic_ad_set') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
