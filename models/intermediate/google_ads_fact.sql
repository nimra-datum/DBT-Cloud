WITH calculation_cte AS (
    SELECT a.ad_id, a.ad_name, g.name as ad_group_name , c.name as campaign_name, c.status, a.date ,  
    SUM(a.impressions) AS total_impressions,
    SUM(a.clicks) AS total_clicks,
    SUM(a.cost_micros)/1000000  AS total_spend
    FROM {{ ref('ad') }} a
    LEFT JOIN {{ ref('groups')}} g ON a.campaign_id = g.campaign_id
    LEFT JOIN {{ref('campaign')}} c ON a.campaign_id = c.id
    GROUP BY a.ad_id, g.name, g.name , a.date ,c.status,c.name, a.ad_name
)

SELECT *,
  ROUND((cte.total_clicks::NUMERIC / NULLIF(cte.total_impressions, 0)) * 100, 2) AS CTR, ROUND(cte.total_spend / NULLIF(cte.total_clicks, 0), 2) AS CPC
FROM calculation_cte cte




