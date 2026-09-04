-- WHAT THIS STEP DOES
--   Explodes silver_opportunity_dialogue to one row per pre-identified candidate code per opportunitiy,
--   then uses ai_classify to determine:
--     * Disposition (hard_blocker, friction, mention_only, resolved, latent, insufficient_evidence)
--     * Qualifier (code-specific sub-reason, driven by lookup_qualifier_config lookup)
--   Evidence is extracted from the ai_classify rationale.
--   Opportunities with zero pre-identified codes are retained as a single row with null codes
--

CREATE OR REFRESH MATERIALIZED VIEW gold_offer_blockers (
  CONSTRAINT no_disp_error EXPECT(disp_error IS NULL),
  CONSTRAINT no_qual_error EXPECT(qual_error IS NULL)
) AS
WITH exploded AS (SELECT
    d.opportunity_id,
    d.stage_name,
    d.transcript_text,
    d.quoted_rates,
    d._source_file,
    c.code_name
  FROM
    gold_opportunity_enrichment d
    LATERAL VIEW explode_outer(d.code_classification_names) c AS code_name
),
classified AS (SELECT
    e.opportunity_id,
    e.stage_name,
    e.transcript_text,
    e.quoted_rates,
    e._source_file,
    e.code_name,
    -- Disposition: how much did this issue affect the deal?
    ai_classify(
      concat(
        '[',
        coalesce(e.code_name, 'No specific offer blocker code pre-identified'),
        ']',
        '\n\n',
        e.transcript_text
      ),
      '{
        "hard_blocker": "Deal declined, died, or made conditional on a change to this attribute that was not granted.",
        "friction": "Deal continued, but this attribute caused stall, escalation, demanded concession, or visible drag on progression.",
        "mention_only": "Attribute surfaced as a candidate issue but did not affect progression. Most common outcome.",
        "resolved": "Customer factual premise was WRONG, accurate explanation corrected it, and the concern dissolved entirely.",
        "latent": "Attribute objectively adverse for this customer, opportunity stalled or ended undetermined, no competing stated cause.",
        "insufficient_evidence": "Transcript quality prevents determination."
      }',
      map(
        'version', '2.1',
        'enableConfidenceScores', 'true',
        'enableRationales', 'true',
        'instructions',
        'You are a senior sales operations analyst at Superior Plus Propane analyzing a sales call transcript. Classify the disposition of the offer blocker code identified in brackets at the start of the text. This code was pre-identified as relevant to the transcript. Determine how much this specific issue affected the deal outcome. Key rules: If the concern dissolved once the rep gave an accurate explanation, classify as resolved. If the REP''s handling (not the offer itself) was the gap, classify as mention_only. Tiebreaker: any concession/waiver/credit granted after the concern was voiced -> friction. Customer''s factual premise was wrong and correction ended the issue -> resolved. Customer simply accepted the accurate answer with no concession or drag -> mention_only. In your rationale, include the relevant verbatim customer quote from the transcript.'
      )
    ) AS disp_result,
    -- Qualifier: what specific sub-type within this code?
    ai_classify(
      concat('INSTRUCTIONS:\n', cfg.instructions, '\n\nTRANSCRIPT:\n', e.transcript_text),
      cfg.labels_json,
      map(
        'version', '2.1',
        'enableConfidenceScores', 'true',
        'enableRationales', 'true',
        'instructions',
        'You are a senior sales operations analyst at Superior Plus Propane analyzing a sales call transcript. Classify the qualifier sub-type for the given offer blocker code based on the transcript content (under the header "TRANSCRIPT:")  and instructions (under the header "INSTRUCTIONS:") provided in the text.'
      )
    ) AS qual_result
  FROM
    exploded e
      LEFT JOIN lookup_qualifier_config cfg
        ON left(e.code_name, 2) = cfg.code_prefix
)
SELECT
  opportunity_id,
  stage_name,
  quoted_rates,
  _source_file,
  left(code_name, 2) AS code,
  code_name,
  disp_result:response[0]:value::STRING AS disposition,
  disp_result:response[0]:confidence_score::DOUBLE AS disposition_confidence,
  disp_result:response[0]:rationale::STRING AS disposition_evidence,
  qual_result:response[0]:value::STRING AS qualifier,
  qual_result:response[0]:confidence_score::DOUBLE AS qualifier_confidence,
  qual_result:response[0]:rationale::STRING AS qualifier_evidence,
  disp_result:error_message::STRING AS disp_error,
  qual_result:error_message::STRING AS qual_error
FROM
  classified;

-- WHAT THIS STEP DOES
--   Clean up the analysis to match the format of the manual workflow's "AI Output" spreadsheet.
--
-- COLUMN MEANINGS (one row = one evaluated candidate issue for an opportunity)
--   Version/Batch      : which prompt revision + transcript batch produced this (metadata).
--   Opp ID             : the Salesforce Opportunity ID (taken from our data, exact).
--   Salesforce Stage   : the deal's stage (Open / Closed Won / Closed Lost).
--   quoted_rate_detail  : ARRAY<STRUCT> — all structured rates from ai_extract; each element has {amount, rate_type, per_unit} with {value}.
--   final_quoted_rate   : STRUCT — the last (final) quoted rate from the array, as a single JSON object.
--   Code / Code Name   : the blocker code 4A-4F and its display name.
--   Primary            : "Yes" if this is the main blocker for the opportunity, else "—".
--   Qualifier          : sub-reason within the code (e.g. "current-supplier").
--   Disposition        : hard_blocker | friction | mention_only | resolved | latent |
--                        insufficient_evidence (how much it actually affected the deal).
--   Confidence         : High | Medium | Low (mapped from ai_classify confidence score).
--   Evidence           : justification from ai_classify rationale (best effort).
--
-- EXPECTATIONS (data-quality "warn" checks; rows are kept, violations reported)
CREATE OR REFRESH MATERIALIZED VIEW gold_offer_blocker_summary
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS
SELECT
  '${prompt_version}' AS Version,
  regexp_extract(_source_file, '[^/]+$', 0) AS Batch,
  opportunity_id AS `Opp ID`,
  stage_name AS `Salesforce Stage`,
  element_at(quoted_rates, -1) AS final_quoted_rate,
  code AS Code,
  code_name AS `Code Name`,
  -- Primary: highest-confidence blocker per opportunity gets "Yes".
  CASE
    WHEN disposition IN ('hard_blocker', 'friction')
      AND row_number() OVER (
        PARTITION BY opportunity_id
        ORDER BY
          CASE WHEN disposition = 'hard_blocker' THEN 0 ELSE 1 END,
          disposition_confidence DESC
      ) = 1
    THEN 'Yes'
  END AS Primary,
  nullif(trim(qualifier), '') AS Qualifier,
  disposition AS Disposition,
  CASE
    WHEN disposition = 'insufficient_evidence' THEN 'Low'
    WHEN disposition_confidence >= 0.75 THEN 'High'
    ELSE 'Medium'
  END AS Confidence,
  nullif(trim(disposition_evidence), '') AS Evidence,
  code AS Key
FROM
  gold_offer_blockers;