SELECT 'facebook' AS platform,ad_name, adset_name AS AD_Group_SET_Creative, campaign_name, date, status, total_impressions, total_clicks, total_spend, CPC,CTR
FROM {{ref('facebook_ads_fact')}}

UNION
SELECT 'google' AS platform, ad_name, ad_group_name AS AD_Group_SET_Creative ,campaign_name, date, status, total_impressions, total_clicks , total_spend, CPC ,CTR
FROM {{ ref('google_ads_fact') }}

UNION
SELECT 'linkedin' AS platform, 'N/A' AS ad_name, creative_name AS AD_Group_SET_Creative, campaign_name, date, status,total_impressions, total_clicks, total_spend, cpc AS CPC, ctr AS CTR
FROM {{ ref('linkedin_ads_inter') }}