WITH ranked AS (
    SELECT account_id, age, gender, date, impressions, reach, cpc, cpm, ctr, frequency, spend, 
    ROW_NUMBER() OVER (PARTITION BY account_id, age, gender,date ORDER BY spend DESC NULLS LAST) AS row_num
    FROM {{ source('facebooks_ads', 'demographics_age_and_gender') }}
)

SELECT *
FROM ranked
WHERE row_num = 1
