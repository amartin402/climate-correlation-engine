/* @bruin
name: mart.fact_sea_level
type: bq.sql
depends:
  - staging.load_sea_level
@bruin */

-- pipelines/sea_level_pipeline/assets/sea_level_model.sql
-- -------------------------------------------------------
-- TRANSFORMATION: staging → mart for sea level data
-- -------------------------------------------------------
-- Transforms raw sea level staging data into an
-- analytics-ready fact table with year-over-year change.
--
-- SOURCE FIELDS (stg_sea_level):
--   entity, code, day,
--   sea_level_church_and_white_2011,
--   sea_level_uhslc,
--   sea_level_change   (the avg of both series from OWID)
--
-- KEY TRANSFORMATIONS:
--   1. Extract year from day column (source is daily, mart is annual)
--   2. Take the last available day per entity per year
--      as the representative annual data point
--   3. Use sea_level_change (avg series) as primary value,
--      falling back to sea_level_church_and_white_2011 when
--      the average is null (UHSLC gaps make it unavailable)
--   4. Compute yoy_change_mm via LAG() window function
--
-- WHY LAST DAY PER YEAR:
--   The source data is daily measurements. The mart is annual
--   for dashboard readability. Taking the last day of each year
--   gives the end-of-year cumulative sea level — the most
--   meaningful single annual data point for a trend chart.
--
-- LAG() WINDOW FUNCTION:
--   LAG(sea_level_change, 1) looks back 1 row within the
--   same entity partition, ordered by year. Subtracting it
--   from the current value gives the year-over-year delta.
--   The first year for each entity will have NULL yoy_change
--   (no prior year to compare against) — this is correct.
--
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
    sea_level_church_and_white_2011,
    sea_level_uhslc,
    sea_level_change,
    ROW_NUMBER() OVER (
      PARTITION BY entity, EXTRACT(YEAR FROM day)
      ORDER BY day DESC
    ) AS rn
  FROM `climate_staging.stg_sea_level`
  WHERE day IS NOT NULL
),

-- Step 2: keep only the last day row and resolve the sea level value.
--   Priority: sea_level_change (avg series) → sea_level_church_and_white_2011
--   This ensures we always have a value when UHSLC data is sparse,
--   while preferring the blended average when both sources are present.
base AS (
  SELECT
    year,
    entity,
    COALESCE(
      sea_level_change,
      sea_level_church_and_white_2011
    ) AS sea_level_change
  FROM last_day_per_year
  WHERE rn = 1
    AND COALESCE(sea_level_change, sea_level_church_and_white_2011) IS NOT NULL
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
FROM with_yoy;
