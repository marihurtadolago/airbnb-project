-- ============================================================================
-- Madrid Short-Term Rental Observatory: data warehouse schema (DuckDB)
-- Status: in progress. See docs/data_model.md for the design decisions.
--
-- Star schema: two fact tables (availability, reviews) surrounded by three
-- dimensions (date, neighbourhood, listing), plus one metadata table.
-- Every statement uses IF NOT EXISTS, so the script can be run more than once.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

-- One row per calendar day. Covers review dates and calendar dates.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,   -- YYYYMMDD, e.g. 20261224
    date         DATE    NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    quarter      TINYINT  NOT NULL,
    month        TINYINT  NOT NULL,
    month_name   VARCHAR  NOT NULL,
    day_of_week  TINYINT  NOT NULL,     -- 1 = Monday ... 7 = Sunday (ISO)
    day_name     VARCHAR  NOT NULL,
    is_weekend   BOOLEAN  NOT NULL,
    is_holiday   BOOLEAN  NOT NULL DEFAULT false  -- Madrid public holidays (reference list pending)
);

-- One row per neighbourhood of the Inside Airbnb reference table (128 rows).
CREATE TABLE IF NOT EXISTS dim_neighbourhood (
    neighbourhood_key SMALLINT PRIMARY KEY,  -- small surrogate key, cheap to repeat in the facts
    district          VARCHAR NOT NULL,      -- neighbourhood_group in the source
    neighbourhood     VARCHAR NOT NULL,
    UNIQUE (district, neighbourhood)
);

-- One row per listing in each snapshot. Attributes such as price can change
-- between snapshots, so each snapshot keeps its own version of the listing.
CREATE TABLE IF NOT EXISTS dim_listing (
    snapshot_date     DATE     NOT NULL,
    listing_id        BIGINT   NOT NULL,     -- Inside Airbnb id (verified unique per snapshot)
    neighbourhood_key SMALLINT NOT NULL,
    host_id           BIGINT,                -- privacy treatment pending (see docs/data_model.md)
    room_type         VARCHAR  NOT NULL,
    accommodates      INTEGER  NOT NULL,
    price             DECIMAL(10, 2),        -- NULL when the source has no price
    PRIMARY KEY (snapshot_date, listing_id)
);


-- ---------------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------------

-- Grain: one listing, one calendar day, as seen in one snapshot (~8.4 M rows per snapshot).
CREATE TABLE IF NOT EXISTS fact_availability (
    snapshot_date     DATE     NOT NULL,     -- which snapshot ("photo") the row comes from
    listing_id        BIGINT   NOT NULL,
    neighbourhood_key SMALLINT NOT NULL,     -- star schema: neighbourhood reachable in one join
    date_key          INTEGER  NOT NULL,     -- the day the availability refers to
    available         BOOLEAN  NOT NULL,
    minimum_nights    BIGINT   NOT NULL,
    maximum_nights    BIGINT   NOT NULL,
    PRIMARY KEY (snapshot_date, listing_id, date_key)
);

-- Grain: one review, as seen in one snapshot (~1.5 M rows per snapshot).
-- No reviewer name, reviewer id or comment is stored.
CREATE TABLE IF NOT EXISTS fact_review (
    snapshot_date     DATE     NOT NULL,
    listing_id        BIGINT   NOT NULL,
    review_id         BIGINT   NOT NULL,     -- unique only together with listing_id
    neighbourhood_key SMALLINT NOT NULL,
    date_key          INTEGER  NOT NULL,     -- review date
    PRIMARY KEY (snapshot_date, listing_id, review_id)
);


-- ---------------------------------------------------------------------------
-- Metadata
-- ---------------------------------------------------------------------------

-- Snapshots already loaded (used by check_new_snapshot and mark_processed).
CREATE TABLE IF NOT EXISTS processed_snapshots (
    snapshot_date DATE PRIMARY KEY,
    run_id        VARCHAR   NOT NULL,
    loaded_at     TIMESTAMP NOT NULL,
    status        VARCHAR   NOT NULL         -- 'success' once every task has finished
);


-- ---------------------------------------------------------------------------
-- Views for the dashboard
-- ---------------------------------------------------------------------------

-- One row per listing of the latest processed snapshot, with its availability
-- rate and review count. Facts are aggregated separately before the join, so
-- reviews are not multiplied by calendar days.
CREATE OR REPLACE VIEW vw_listing_summary AS
WITH latest AS (
    SELECT max(snapshot_date) AS snapshot_date
    FROM processed_snapshots WHERE status = 'success'
),
availability AS (
    SELECT listing_id, count(*) AS calendar_days,
           avg(CAST(available AS INTEGER)) AS availability_rate
    FROM fact_availability JOIN latest USING (snapshot_date)
    GROUP BY listing_id
),
review_activity AS (
    SELECT listing_id, count(*) AS review_count
    FROM fact_review JOIN latest USING (snapshot_date)
    GROUP BY listing_id
)
SELECT l.snapshot_date, l.listing_id, n.district, n.neighbourhood,
       l.room_type, l.accommodates, l.price,
       a.calendar_days, a.availability_rate,
       coalesce(r.review_count, 0) AS review_count
FROM dim_listing l
JOIN latest USING (snapshot_date)
JOIN dim_neighbourhood n USING (neighbourhood_key)
LEFT JOIN availability    a USING (listing_id)
LEFT JOIN review_activity r USING (listing_id);
