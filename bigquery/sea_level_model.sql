-- bigquery/sea_level_model.sql
-- -------------------------------------------------------
-- TRANSFORMATION: staging → mart for sea level data
-- -------------------------------------------------------
-- Transforms raw sea level staging data into an
-- analytics-ready fact table with year-over-year change.
--
-- SOURCE FIELDS (stg_sea_level):
--   entity, code, day, church_and_white_2011,
--   uhslc, avg_church_white_and_uhslc
--
-- KEY TRANSFORMATIONS:
--   1. Extract year from day column
--   2. Take the last available day per entity per year
--      as the representative annual data point
--   3. Use avg_church_white_and_uhslc as sea_level_change,
--      falling back to church_and_white_2011 when UHSLC
--      is null (making the average unavailable)
--   4. Compute yoy_change_mm via LAG() window function
--
-- LAG() WINDOW FUNCTION:
--   LAG(sea_level_change, 1) looks back 1 row within the
--   same entity partition, ordered by year. Subtracting it
--   from the current value gives the year-over-year delta.
--   The first year for each entity will have NULL yoy_change
--   (no prior year to compare against) — this is correct.
-- -------------------------------------------------------

CREATE OR REPLACE TABLE `climate_mart.fact_sea_level`
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(1850, 2100, 10))
CLUSTER BY entity
AS

-- Step 1: extract year and pick the last recorded day per entity per year
WITH last_day_per_year AS (
  SELECT
    EXTRACT(YEAR FROM day)          AS year,
    entity,
    day,
    church_and_white_2011,
    uhslc,
    avg_church_white_and_uhslc,
    ROW_NUMBER() OVER (
      PARTITION BY entity, EXTRACT(YEAR FROM day)
      ORDER BY day DESC
    ) AS rn
  FROM `climate_staging.stg_sea_level`
  WHERE day IS NOT NULL
),

-- Step 2: keep only the last day row and resolve the sea level value
--   Priority: avg_church_white_and_uhslc → church_and_white_2011
--   This ensures we always have a value when UHSLC data is sparse,
--   while preferring the blended average when both sources are present.
base AS (
  SELECT
    year,
    entity,
    COALESCE(
      avg_church_white_and_uhslc,
      church_and_white_2011
    ) AS sea_level_change
  FROM last_day_per_year
  WHERE rn = 1
    AND COALESCE(avg_church_white_and_uhslc, church_and_white_2011) IS NOT NULL
),

-- Step 3: compute year-over-year change within each entity
with_yoy AS (
  SELECT
    year,
    entity,
    sea_level_change,
    sea_level_change - LAG(sea_level_change, 1) OVER (
      PARTITION BY entity
      ORDER BY year
    ) AS yoy_change_mm
  FROM base
)

SELECT
  year,
  entity,
  ROUND(sea_level_change, 4) AS sea_level_change,
  ROUND(yoy_change_mm,    4) AS yoy_change_mm
FROM with_yoy
ORDER BY entity, year;