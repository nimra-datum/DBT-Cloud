SELECT 'facebook' AS platform,ad_name, adset_name AS AD_Group_SET_Creative, campaign_name, date, status, total_impressions, total_clicks, total_spend, CPC,CTR
FROM {{ref('final_reporting_facebook_table')}}

UNION
SELECT 'google' AS platform, ad_name, ad_group_name AS AD_Group_SET_Creative ,campaign_name, date, status, total_impressions, total_clicks , total_spend, CPC ,CTR
FROM {{ ref('final_repoting_google_table') }}

UNION
SELECT 'linkedin' AS platform, 'N/A' AS ad_name, creative_name AS AD_Group_SET_Creative, campaign_name, date, status,total_impressions, total_clicks, total_spend, cpc AS CPC, ctr AS CTR
FROM {{ ref('final_reporting_linkedin_table') }}