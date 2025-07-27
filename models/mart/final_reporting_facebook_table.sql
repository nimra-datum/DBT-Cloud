SELECT ad_name, adset_name, campaign_name, date, status, total_impressions, total_clicks, total_spend, CPC,CTR
FROM {{ref('facebook_ads_fact')}}