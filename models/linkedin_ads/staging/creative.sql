WITH ranked AS (
    SELECT
        ac.creative_id,
        ch.name,
        EXTRACT(MONTH FROM ac.day) AS month_,
        ac.clicks,
        ac.impressions,
        ac.cost_in_usd,
        ac.cost_in_local_currency,
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
