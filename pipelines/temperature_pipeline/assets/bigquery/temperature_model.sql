/* @bruin
name: mart.fact_temperature
type: bq.sql
depends:
  - staging.load_temperature
@bruin */

-- pipelines/temperature_pipeline/assets/temperature_model.sql
-- -------------------------------------------------------
-- TRANSFORMATION: staging → mart for temperature anomaly data
-- -------------------------------------------------------
-- Transforms raw temperature anomaly staging data into an
-- analytics-ready fact table with a 5-year rolling average.
--
-- SOURCE FIELDS (stg_temperature):
--   entity, code, year, global_temperature_anomaly
--
-- KEY TRANSFORMATIONS:
--   1. Rename global_temperature_anomaly → temperature_anomaly
--      to match the mart schema
--   2. Compute anomaly_5yr_avg via AVG() window function
--      over a 4-preceding to current row frame, giving a
--      5-year rolling average (current year + 4 prior years)
--
-- 5-YEAR ROLLING AVERAGE:
--   AVG() with ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
--   within the same entity partition, ordered by year.
--   The first 4 years for each entity will have a partial
--   average (fewer than 5 data points) — this is correct
--   and preferable to NULL for early years on a dashboard.
--
-- NOTE ON ORDER BY:
--   ORDER BY is intentionally omitted from the final SELECT.
--   BigQuery does not allow ORDER BY in a CREATE TABLE AS SELECT
--   when PARTITION BY is used — it raises:
--   "Result of ORDER BY queries cannot be partitioned by field"
--   The table is physically organised by the PARTITION BY year
--   and CLUSTER BY entity definitions instead, which is more
--   efficient than ORDER BY for analytical queries.
-- -------------------------------------------------------

CREATE OR REPLACE TABLE `climate_mart.fact_temperature`
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(1850, 2100, 10))
CLUSTER BY entity
AS

-- Step 1: filter to rows with a valid measurement and
--         rename to mart column names
WITH base AS (
  SELECT
    year,
    entity,
    global_temperature_anomaly AS temperature_anomaly
  FROM `climate_staging.stg_temperature`
  WHERE year                       IS NOT NULL
    AND global_temperature_anomaly IS NOT NULL
),

-- Step 2: compute the 5-year rolling average per entity
with_rolling_avg AS (
  SELECT
    year,
    entity,
    temperature_anomaly,
    AVG(temperature_anomaly) OVER (
      PARTITION BY entity
      ORDER BY year
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS anomaly_5yr_avg
  FROM base
)

SELECT
  year,
  entity,
  ROUND(temperature_anomaly, 6) AS temperature_anomaly,
  ROUND(anomaly_5yr_avg,     6) AS anomaly_5yr_avg
FROM with_rolling_avg;
