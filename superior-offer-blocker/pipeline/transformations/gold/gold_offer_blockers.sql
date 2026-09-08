-- WHAT THIS STEP DOES
--   Explodes silver_opportunity_dialogue to one row per pre-identified candidate code per opportunitiy,
--   then uses ai_query to determine:
--     * Disposition (hard_blocker, friction, mention_only, resolved, latent, insufficient_evidence)
--     * Qualifier (code-specific sub-reason, driven by lookup_qualifier_config lookup)
--   Evidence is extracted from the model's rationale/evidence.
--   Opportunities with zero pre-identified codes are retained as a single row with null codes
--

CREATE OR REFRESH MATERIALIZED VIEW gold_offer_blockers (
  CONSTRAINT no_disp_error EXPECT(disp_error IS NULL),
  CONSTRAINT no_qual_error EXPECT(qual_error IS NULL)
) AS
WITH exploded AS (
  SELECT
    d.opportunity_id,
    d.stage_name,
    d.transcript_text,
    d.quoted_rates,
    d._source_file,
    c.code_name
  FROM gold_opportunity_enrichment d
  LATERAL VIEW explode_outer(d.code_classification_names) c AS code_name
),
classified AS (
  SELECT
    e.opportunity_id,
    e.stage_name,
    e.transcript_text,
    e.quoted_rates,
    e._source_file,
    e.code_name,
    -- Disposition: how much did this issue affect the deal?
    ai_query(
      '${model_endpoint}',
      concat(
        p.prompt,
        '\n\nTASK:\nDetermine the disposition of the single code named under CANDIDATE CODE for the transcript below, applying the disposition definitions and tiebreaker in Section 6. If the code reflects only the rep\'s handling rather than the offer attribute itself, use mention_only. Provide evidence per Section 4 (a verbatim customer quote). Return only strict JSON matching the response schema. Do not add prose.',
        '\n\nTRANSCRIPT:\n', e.transcript_text
      ),
      responseFormat => '{"type":"json_schema","json_schema":{"name":"disposition","strict":true,"schema":{"type":"object","additionalProperties":false,"required":["disposition","confidence","evidence"],"properties":{"disposition":{"type":"string","enum":["hard_blocker","friction","mention_only","resolved","latent","insufficient_evidence"]},"confidence":{"type":"number","minimum":0,"maximum":1},"evidence":{"type":"string"}}}}}',
      failOnError => false,
      modelParameters => named_struct('temperature', CAST(0.0 AS DOUBLE), 'max_tokens', 1200)
    ) AS disp_resp,
    -- Qualifier: what specific sub-type within this code?
    ai_query(
      '${model_endpoint}',
      concat(
        p.prompt,
        '\n\nPOSSIBLE_LABELS_JSON:\n', coalesce(cfg.labels_json, '{}'),
        '\n\nTASK:\nChoose the single best qualifier label (string) from POSSIBLE_LABELS_JSON based on the transcript content, provide a brief evidence rationale and numeric confidence in [0,1]. Return only strict JSON matching the response schema. Do not add prose.',
        '\n\nTRANSCRIPT:\n', e.transcript_text
      ),
      responseFormat => '{"type":"json_schema","json_schema":{"name":"qualifier","strict":true,"schema":{"type":"object","additionalProperties":false,"required":["qualifier","confidence","evidence"],"properties":{"qualifier":{"type":"string"},"confidence":{"type":"number","minimum":0,"maximum":1},"evidence":{"type":"string"}}}}}',
      failOnError => false,
      modelParameters => named_struct('temperature', CAST(0.0 AS DOUBLE), 'max_tokens', 1200)
    ) AS qual_resp
  FROM exploded e
  LEFT JOIN lookup_qualifier_config cfg
    ON left(e.code_name, 2) = cfg.code_prefix
  CROSS JOIN (SELECT prompt FROM prompt_offer_blocker LIMIT 1) p
)
SELECT
  opportunity_id,
  stage_name,
  quoted_rates,
  _source_file,
  left(code_name, 2) AS code,
  code_name,
  from_json(disp_resp.result, 'STRUCT<disposition: STRING, confidence: DOUBLE, evidence: STRING>').disposition AS disposition,
  from_json(disp_resp.result, 'STRUCT<disposition: STRING, confidence: DOUBLE, evidence: STRING>').confidence AS disposition_confidence,
  from_json(disp_resp.result, 'STRUCT<disposition: STRING, confidence: DOUBLE, evidence: STRING>').evidence AS disposition_evidence,
  from_json(qual_resp.result, 'STRUCT<qualifier: STRING, confidence: DOUBLE, evidence: STRING>').qualifier AS qualifier,
  from_json(qual_resp.result, 'STRUCT<qualifier: STRING, confidence: DOUBLE, evidence: STRING>').confidence AS qualifier_confidence,
  from_json(qual_resp.result, 'STRUCT<qualifier: STRING, confidence: DOUBLE, evidence: STRING>').evidence AS qualifier_evidence,
  disp_resp.errorMessage::STRING AS disp_error,
  qual_resp.errorMessage::STRING AS qual_error
FROM classified;

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
