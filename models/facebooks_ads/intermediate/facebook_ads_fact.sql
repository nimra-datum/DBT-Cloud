SELECT ad.ad_id, ad.ad_name, ad.account_id, ad.date, SUM(ad.impressions) AS total_impressions, ROUND(AVG(ad.cpc)::NUMERIC,2) AS CPC, ROUND(AVG(ad.cpm)::NUMERIC,2) AS CPM, ROUND(AVG(ad.ctr)::NUMERIC,2) AS CTR, SUM(ad.spend) AS total_spend, ROUND(AVG(ad.frequency)::NUMERIC,2) AS frequency,
       ads.adset_id, ads.adset_name, 
       c.campaign_id, c.campaign_name, 
       dga.age, dga.gender,
       dc.country, dr.region,
       mm.dma, mm.device_platform, mm.publisher_platform, mm.platform_position

FROM {{ ref('ad_facebook') }} ad 
LEFT JOIN {{ref('ad_set_facebook')}} ads ON ad.account_id::TEXT = ads.account_id::TEXT AND ad.date = ads.date 
LEFT JOIN {{ref('campaign_facebook')}} c ON ad.account_id::TEXT = c.account_id::TEXT AND ad.date = c.date
LEFT JOIN {{ref('age_and_gender')}} dga ON ad.account_id::TEXT = dga.account_id AND ad.date = dga.date
LEFT JOIN {{ref('country')}} dc ON ad.account_id::TEXT = dc.account_id::TEXT AND ad.date = dc.date
LEFT JOIN {{ref('region')}} dr ON ad.account_id::TEXT = dr.account_id::TEXT AND ad.date = dr.date
LEFT JOIN {{ref('marketing_mix')}} mm ON ad.account_id::TEXT = mm.account_id::TEXT AND ad.date = mm.date

GROUP BY ad.ad_id, ad.ad_name, ad.account_id, ad.date, ads.adset_id, ads.adset_name,
         c.campaign_id, c.campaign_name, dga.age, dga.gender,
         dc.country, dr.region,
         mm.dma, mm.device_platform, mm.publisher_platform, mm.platform_position
