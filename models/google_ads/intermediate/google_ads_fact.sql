WITH calculation_cte AS (
    SELECT a.ad_id, a.ad_group_id, g.campaign_id ,g. customer_id , a.date , a.device,  
    SUM(a.impressions) AS total_impressions,
    SUM(a.clicks) AS total_clicks,
    (SUM(a.cost_micros)/1000000) + SUM(a.cost_per_conversion) AS total_cost
    FROM {{ ref('ad') }} a
    LEFT JOIN {{ ref('groups')}} g ON a.ad_group_id = g.id
    GROUP BY a.ad_id, a.ad_group_id, g.campaign_id , g.customer_id , a.date , a.device
)

SELECT 
  cte.ad_id, cte.ad_group_id, cte.campaign_id,cte.customer_id,cte.date,cte.device,cte.total_impressions,cte.total_clicks,cte.total_cost AS total_spent,
  ROUND((cte.total_clicks::NUMERIC / NULLIF(cte.total_impressions, 0)) * 100, 2) AS CTR, ROUND(cte.total_cost / NULLIF(cte.total_clicks, 0), 2) AS CPC,
  agh.campaign_name, agh.status, agh.type
FROM calculation_cte cte
LEFT JOIN {{ ref('campaign') }} agh ON agh.base_ad_group_id = cte.ad_group_id




