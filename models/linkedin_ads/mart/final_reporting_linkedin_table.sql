SELECT creative_name, campaign_name, month_ AS Month, total_cost_local AS total_cost,
  total_clicks, total_impressions, cpc AS CPC, ctr AS CTR

FROM {{ ref('linkedin_ads_inter') }}
