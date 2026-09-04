-- ============ [CORE — like-for-like with original/ingest_jsonl_to_delta.py] ============
-- WHAT THIS STEP DOES
--   Ingests raw call-transcript JSON files as they arrive and lands them in a
--   clean, de-duplicated bronze table. This replaces the original Python script,
--   which downloaded JSON from Azure Blob and MERGE'd it into Delta keyed on `id`.
--
-- HOW THE ORIGINAL TWO CHECKPOINTS ARE REPLACED
--   * Original blob-filename checkpoint (don't re-download a file) -> Auto Loader's
--     built-in file tracking. `STREAM read_files(...)` remembers which files it has
--     already processed, so re-running never re-ingests the same file.
--   * Original record-level `{id}.json` checkpoint + `MERGE ... ON id` (don't insert
--     a duplicate record) -> the AUTO CDC flow below, keyed on `id`. It keeps exactly
--     one row per `id` across every run (idempotent upsert).
--
-- NOTES
--   Each JSON record = one "segment" = one phone call. `id` is the unique call id.
--   `interwovenTranscript.transcriptBlock` is the turn-by-turn dialogue: an array of
--   {channelName: 'AGENT'|'CLIENT', text: '...'}.
-- Stage A -- Auto Loader raw ingest (append-only; if a file is ever re-landed, it will be deduped in the Stage B table below).
-- `${input_multiline}` = true because the sample file is a single JSON array; set it to false for line-delimited JSON (JSONL) in production.
--

CREATE OR REFRESH STREAMING TABLE bronze_transcripts_ingest AS
SELECT
  *,
  _metadata.file_path AS _source_file, -- which file this row came from
  current_timestamp() AS _ingest_ts -- when we ingested it
FROM
  STREAM read_files(
    '/Volumes/${catalog}/${schema}/landing/transcripts/',
    format => 'json',
    multiLine => ${input_multiline}, -- true = one JSON array per file
    inferColumnTypes => true,
    schemaEvolutionMode => 'addNewColumns', -- absorb new fields automatically
    rescuedDataColumn => '_rescued_data', -- anything off-schema is captured here
    -- Pin the handful of load-bearing fields so their types are stable; the other fields are inferred and allowed to evolve.
    schemaHints =>
      'id STRING, clientPhoneNumber STRING, clientAni STRING, recordingStartTime TIMESTAMP, interactionDurationSeconds INT, interwovenTranscript STRUCT<transcriptBlock: ARRAY<STRUCT<channelName:STRING, text:STRING>>>'
  );

-- Stage B -- de-duplicated bronze table (exactly one row per call `id`).
-- The target must be pre-created before the AUTO CDC flow populates it.
--
-- AUTO CDC = Native support for SCD Type 1 and Type 2 Upserts
--   KEYS (id)                    -> uniqueness key (the call id)
--   SEQUENCE BY recordingStartTime -> if the same id appears twice, keep the latest
--   STORED AS SCD TYPE 1         -> overwrite in place (no history), like the MERGE
CREATE OR REFRESH STREAMING TABLE bronze_transcripts;

CREATE FLOW bronze_dedup AS
  AUTO CDC INTO bronze_transcripts
    FROM STREAM(bronze_transcripts_ingest) KEYS (id)
      SEQUENCE BY recordingStartTime
      STORED AS SCD TYPE 1;