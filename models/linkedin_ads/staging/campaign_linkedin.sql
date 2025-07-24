WITH ranked_campaigns AS (
  SELECT 
    c.campaign_id,
    cg.name,
    cg.status,
    cg.account_id,
    c.day,
    EXTRACT(MONTH FROM c.day) AS month_,
    c.clicks,
    c.impressions,
    c.cost_in_local_currency,
    c.cost_in_usd,
    ROW_NUMBER() OVER (
      PARTITION BY c.campaign_id, EXTRACT(MONTH FROM c.day)
      ORDER BY c.day DESC
    ) AS row_num
  FROM {{ source('linkedin_ads', 'ad_analytics_by_campaign') }} c
  LEFT JOIN {{ source('linkedin_ads', 'campaign_history') }} cg 
    ON c.campaign_id = cg.id
)

SELECT *
FROM ranked_campaigns
WHERE row_num = 1
