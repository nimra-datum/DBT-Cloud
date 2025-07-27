SELECT 
  cd.date,cd.creative_name,ccd.name AS campaign_name,ccd.status,cd.account_id,
  
  SUM(ccd.cost_in_usd) AS total_spend,
  SUM(ccd.clicks) AS total_clicks,
  SUM(ccd.impressions) AS total_impressions,

  ROUND(SUM(ccd.cost_in_usd) / NULLIF(SUM(ccd.clicks), 0), 2) AS CPC,
  ROUND(SUM(ccd.clicks)::NUMERIC / NULLIF(SUM(ccd.impressions), 0), 4) AS CTR

FROM {{ ref('creative') }} cd
JOIN {{ ref('campaign_linkedin') }} ccd ON cd.account_id = ccd.account_id

GROUP BY 
  cd.date,cd.creative_name,cd.creative_id,cd.account_id,ccd.name,ccd.status
