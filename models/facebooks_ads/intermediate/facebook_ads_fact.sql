{{ config(materialized='view') }}

WITH ad AS (
    SELECT * FROM {{ ref('ad_facebook') }}
),
ads AS (
    SELECT * FROM {{ ref('ad_set_facebook') }}
),
c AS (
    SELECT * FROM {{ ref('campaign_facebook') }}
),
dga AS (
    SELECT * FROM {{ ref('age_and_gender') }}
),
dc AS (
    SELECT * FROM {{ ref('country') }}
),
dr AS (
    SELECT * FROM {{ ref('region') }}
),
mm AS (
    SELECT * FROM {{ ref('marketing_mix') }}
)

SELECT
    ad.ad_id, ad.ad_name, ad.account_id, ad.date,
    SUM(ad.impressions) AS total_impressions,
    ROUND(AVG(ad.cpc)::NUMERIC,2) AS cpc,
    ROUND(AVG(ad.cpm)::NUMERIC,2) AS cpm,
    ROUND(AVG(ad.ctr)::NUMERIC,2) AS ctr,
    SUM(ad.spend) AS total_spend,
    ROUND(AVG(ad.frequency)::NUMERIC,2) AS frequency,
    ads.adset_id, ads.adset_name,
    c.campaign_id, c.campaign_name,
    dga.age, dga.gender,
    dc.country, dr.region,
    mm.dma, mm.device_platform, mm.publisher_platform, mm.platform_position

FROM ad
LEFT JOIN ads ON ad.account_id::TEXT = ads.account_id::TEXT AND ad.date = ads.date
LEFT JOIN c ON ad.account_id::TEXT = c.account_id::TEXT AND ad.date = c.date
LEFT JOIN dga ON ad.account_id::TEXT = dga.account_id::TEXT AND ad.date = dga.date
LEFT JOIN dc ON ad.account_id::TEXT = dc.account_id::TEXT AND ad.date = dc.date
LEFT JOIN dr ON ad.account_id::TEXT = dr.account_id::TEXT AND ad.date = dr.date
LEFT JOIN mm ON ad.account_id::TEXT = mm.account_id::TEXT AND ad.date = mm.date


GROUP BY
    ad.ad_id, ad.ad_name, ad.account_id, ad.date,
    ads.adset_id, ads.adset_name,
    c.campaign_id, c.campaign_name,
    dga.age, dga.gender,
    dc.country, dr.region,
    mm.dma, mm.device_platform, mm.publisher_platform, mm.platform_position
