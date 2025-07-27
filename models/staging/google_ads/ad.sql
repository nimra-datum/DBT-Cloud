WITH ranked AS (
  SELECT a.ad_id,s.search_term AS ad_name,a.clicks,a.impressions,a.date,a.campaign_id,a.cost_micros,
         ROW_NUMBER() OVER (
           PARTITION BY a.ad_id, a.campaign_id ,a.date
           ORDER BY a.cost_micros DESC
         ) AS row_num
  FROM {{ source('google_ads', 'ad_stats') }} a
  LEFT JOIN {{source('google_ads' , 'search_term_keyword_stats')}} s ON a.campaign_id = s.campaign_id
)

SELECT *
FROM ranked
WHERE row_num = 1
