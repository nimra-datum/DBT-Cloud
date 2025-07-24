WITH ranked AS (
    SELECT account_id, region, date, impressions, reach, cpc, cpm, ctr, frequency, spend, 
    ROW_NUMBER() OVER (PARTITION BY account_id, region,date ORDER BY spend DESC NULLS LAST) AS row_num
    FROM {{ source('facebooks_ads', 'demographics_region') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
