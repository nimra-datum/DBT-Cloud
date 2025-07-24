WITH creative_data AS (
    SELECT *
    FROM {{ ref('creative') }}
),

campaign_data AS (
    SELECT *
    FROM {{ ref('campaign_linkedin') }}
)

SELECT 
    cd.name AS creative_name, ccd.name as campaign_name, cd.month_,

    SUM(cd.cost_in_local_currency) AS total_cost_local,
    SUM(cd.clicks) AS total_clicks,
    SUM(cd.impressions) AS total_impressions,

    ROUND(SUM(cd.cost_in_usd) / NULLIF(SUM(cd.clicks), 0), 2) AS cpc,
    ROUND(SUM(cd.clicks)::NUMERIC / NULLIF(SUM(cd.impressions), 0), 4) AS ctr

FROM creative_data cd
JOIN campaign_data ccd ON cd.month_ = ccd.month_
GROUP BY cd.month_, cd.name , ccd.name
