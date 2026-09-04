-- ============ silver_transcript_sf_joined ============
-- Joins calls with known sales opportunity in the target regions." Reproduces the original Python prep, step for step:
--   1. Join to the (synthetic) Salesforce table on the normalized phone to attach
--      the opportunity_id (the deal), its stage, region, and created date.
--   2. Number each call chronologically within its opportunity -> "Seg X of N".
--   3. Build `combined_key`, the human-readable traceability string the LLM sees.
--   4. Keep only calls in the two demo regions.
--
-- DOMAIN NOTES
--   opportunity  = one sales deal (Salesforce Opportunity ID).
--   segment      = one call. An opportunity can have several calls (segments).
--   "Seg X of N" = this call's chronological position among the opportunity's N calls.
--   This is a Materialized View (batch), not streaming, because the window functions below need to see all of an opportunity's calls at once.

-- Includes DQ checks for region and phone number
CREATE OR REFRESH MATERIALIZED VIEW silver_transcript_sf_joined (

    CONSTRAINT valid_phone10 EXPECT(length(clientPhoneNumber) = 10) ON VIOLATION DROP ROW,
    CONSTRAINT valid_region EXPECT(region IN ('${region_1}', '${region_2}')) ON VIOLATION DROP ROW
  ) AS
WITH normalized AS (SELECT
    *,
    right(regexp_replace(clientPhoneNumber, '[^0-9]', ''), 10) AS phone10
  FROM
    bronze_transcripts
),
windowed AS (SELECT
    t.id,
    t.phone10 AS clientPhoneNumber,
    sf.opportunity_id,
    sf.stage_name,
    sf.region,
    sf.created_datetime,
    t.recordingStartTime,
    t.interactionDurationSeconds,
    t.interwovenTranscript,
    t._source_file,
    count(*) OVER (PARTITION BY t.phone10) AS seg_count,
    row_number() OVER (PARTITION BY t.phone10 ORDER BY t.recordingStartTime) AS position
  FROM
    normalized t
      JOIN ${catalog}.${schema}.dim_salesforce_opportunity sf
        ON sf.contact_phone = t.phone10
)
SELECT
  *,
  concat(
    opportunity_id, ' | Created: ', created_datetime, ' | ',
    id, ' | Seg ', position, ' of ', seg_count,
    ' | Call date: ', recordingStartTime, ' | ',
    interactionDurationSeconds, 's'
  ) AS combined_key
FROM
  windowed;
