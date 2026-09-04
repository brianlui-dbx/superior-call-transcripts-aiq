-- ============ silver_opportunity_dialogue ============
-- WHAT THIS STEP DOES
--   Aggregates all calls of one opportunity into a single prompt-ready transcript
--   string (the original `format_transcripts()` logic).
--   DQ: Drops malformed opportunities (non-contiguous segment numbering, missing Salesforce opp).
--

CREATE OR REFRESH MATERIALIZED VIEW silver_opportunity_dialogue (

    CONSTRAINT contiguous_positions EXPECT(
      max_pos = seg_count
      AND distinct_pos = seg_count
    ) ON VIOLATION DROP ROW,
    CONSTRAINT has_opp_id EXPECT(opportunity_id IS NOT NULL) ON VIOLATION DROP ROW
  ) AS
WITH per_segment AS (SELECT
    opportunity_id,
    stage_name,
    position,
    seg_count,
    _source_file,
    concat(
      '\n============================================\n',
      'Segment ID: ',
      id,
      '\n',
      combined_key,
      '\n--------------------------------------------\n',
      concat_ws(
        '\n',
        transform(
          filter(
            interwovenTranscript.transcriptBlock,
            b ->
              b.text IS NOT NULL
              AND length(trim(b.text)) > 0
          ),
          b -> concat(b.channelName, ': ', b.text)
        )
      )
    ) AS segment_block
  FROM
    silver_transcript_sf_joined
)
SELECT
  opportunity_id,
  any_value(stage_name) AS stage_name,
  any_value(seg_count) AS seg_count,
  array_join(collect_set(_source_file), ', ') AS _source_file,
  max(position) AS max_pos,
  count(DISTINCT position) AS distinct_pos,
  array_join(
    transform(array_sort(collect_list(struct(position, segment_block))), x -> x.segment_block),
    '\n'
  ) AS transcript_text
FROM
  per_segment
GROUP BY
  opportunity_id;
