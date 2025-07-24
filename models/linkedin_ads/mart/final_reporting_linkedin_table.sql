SELECT creative_name, campaign_name, month_ AS Month, total_cost_local AS total_cost,
  total_clicks, total_impressions, cpc AS CPC, ctr AS CTR , (cpc*total_clicks) AS total_spent

FROM {{ ref('linkedin_ads_inter') }}
