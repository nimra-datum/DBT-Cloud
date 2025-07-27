SELECT creative_name, campaign_name, date, status,total_spend,total_clicks, total_impressions, cpc AS CPC, ctr AS CTR

FROM {{ ref('linkedin_ads_inter') }}
