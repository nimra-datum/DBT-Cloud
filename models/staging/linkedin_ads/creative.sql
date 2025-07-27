WITH ranked AS (
    SELECT ac.creative_id,ch.name as creative_name,CAST(ac.day AS DATE) AS date,ac.clicks,ch.account_id, ac.impressions,ac.cost_in_usd,
        ROW_NUMBER() OVER (
            PARTITION BY creative_id, day  
            ORDER BY cost_in_usd DESC NULLS LAST  
        ) AS row_num
    FROM {{ source('linkedin_ads' , 'ad_analytics_by_creative') }} ac
    LEFT JOIN {{source('linkedin_ads' , 'creative_history')}} ch ON ac.creative_id = ch.id

)

SELECT *
FROM ranked
WHERE row_num = 1
