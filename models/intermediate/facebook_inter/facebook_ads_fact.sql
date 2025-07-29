{{ config(materialized='table') }}

SELECT ad.ad_id, ad.ad_name, c.campaign_id , c.name AS campaign_name, ads.adset_name, ad.account_id, ad.date, c.status,
  SUM(ad.impressions) AS total_impressions, 
  SUM(ads.clicks) AS total_clicks,
  ROUND(AVG(ad.cpc)::NUMERIC, 2) AS CPC,  
  ROUND(AVG(ad.ctr)::NUMERIC, 4) AS CTR, 
  SUM(ad.spend) AS total_spend

FROM {{ ref('ad_facebook') }} ad 
LEFT JOIN {{ref('ad_set_facebook')}} ads ON ad.account_id = ads.account_id
LEFT JOIN {{ref('campaign_facebook')}} c ON ads.campaign_name= c.name

GROUP BY ad.ad_id, ad.ad_name,ads.adset_name ,ad.account_id, ad.date, c.campaign_id, c.name, c.status
