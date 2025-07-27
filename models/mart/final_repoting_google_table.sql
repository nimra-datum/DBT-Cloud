SELECT ad_name, ad_group_name,campaign_name, status, date, total_clicks , total_impressions, total_spend, CTR, CPC
FROM {{ ref('google_ads_fact') }}
