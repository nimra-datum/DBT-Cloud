SELECT ad_id, ad_group_id, campaign_id, customer_id, date, device, total_impressions,
  total_clicks, total_spent, CTR, CPC, campaign_name, status, type
FROM {{ ref('google_ads_fact') }}
