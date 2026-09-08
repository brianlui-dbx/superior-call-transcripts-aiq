-- ============ gold_opportunity_enrichment ============
-- WHAT THIS STEP DOES
--   Runs two task-specific AI queries per opportunity to produce classified codes and rates:
--     * ai_query -> candidate blocker codes (4A–4F), with confidence scores / rationale
--     * ai_query -> quoted and competitor per-unit price(s) as structured JSON (VARIANT)
--
--   Reads from silver_opportunity_dialogue (aggregated, DQ-validated transcripts).
CREATE OR REFRESH MATERIALIZED VIEW gold_opportunity_enrichment AS
WITH base AS (
  SELECT opportunity_id, stage_name, transcript_text, _source_file
  FROM silver_opportunity_dialogue
),
prompt_src AS (
  SELECT prompt FROM prompt_offer_blocker LIMIT 1
),
scored AS (
  SELECT
    b.opportunity_id,
    b.stage_name,
    b.transcript_text,
    b._source_file,
    -- Offer-blocker codes (multilabel across 4A–4F)
    ai_query(
      '${model_endpoint}',
      concat(
        p.prompt,
        '\n\nTASK:\nReturn only strict JSON matching the response schema. Select all applicable offer-blocker codes 4A-4F (multilabel). For each selected code, include a brief rationale citing the key customer quote or behaviour as evidence and a numeric confidence in [0,1].',
        '\n\nTRANSCRIPT:\n', b.transcript_text
      ),
      responseFormat => '{"type":"json_schema","json_schema":{"name":"offer_blocker_codes","strict":true,"schema":{"type":"object","additionalProperties":false,"required":["codes"],"properties":{"codes":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["name","confidence","rationale"],"properties":{"name":{"type":"string","enum":["4A. Rate/price uncompetitive","4B. Ancillary fees barrier","4C. Commercial model mismatch","4D. Contract/transaction mechanics","4E. Availability/serviceability gap","4F. Promotion ineligibility"]},"confidence":{"type":"number","minimum":0,"maximum":1},"rationale":{"type":"string"}}}}}}}}',
      failOnError => false,
      modelParameters => named_struct('max_tokens', 2000)
    ) AS cls_resp,
    -- Quoted and competitor rates
    ai_query(
      '${model_endpoint}',
      concat(
        p.prompt,
        '\n\nTASK:\nExtract the per-unit propane rate quoted by Superior and any competitor or current-supplier rate mentioned by the client, following the rate-extraction discipline in Section 5 (extraction only — never estimate, average, or infer; use null when no rate is explicitly stated). When fixed and variable rates are quoted together as a dual offer, record them as ONE item with rate_type dual and populate both fixed_amount and variable_amount; never duplicate a single rate across both. Return only strict JSON matching the response schema. Do not add prose.',
        '\n\nTRANSCRIPT:\n', b.transcript_text
      ),
      responseFormat => '{"type":"json_schema","json_schema":{"name":"rates_extract","strict":true,"schema":{"type":"object","additionalProperties":false,"required":["quoted_unit_rate","competitor_or_current_supplier_rate"],"properties":{"quoted_unit_rate":{"type":"array","items":{"type":"object","additionalProperties":false,"required":[],"properties":{"rate_type":{"type":"string","enum":["dual","fixed","variable"]},"per_unit":{"type":"string"},"fixed_amount":{"type":"number"},"variable_amount":{"type":"number"},"amount":{"type":"number"}}}},"competitor_or_current_supplier_rate":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["amount","label"],"properties":{"amount":{"type":"number"},"label":{"type":"string","enum":["current_supplier","competitor","generic"]},"per_unit":{"type":"string"}}}}}}}}',
      failOnError => false,
      modelParameters => named_struct('max_tokens', 2000)
    ) AS ext_resp
  FROM base b CROSS JOIN prompt_src p
)
SELECT
  opportunity_id,
  stage_name,
  transcript_text,
  -- Parse cls JSON -> arrays for names, confidences, rationales
  transform(
    from_json(cls_resp.result, 'STRUCT<codes: ARRAY<STRUCT<name: STRING, confidence: DOUBLE, rationale: STRING>>>').codes,
    x -> x.name
  ) AS code_classification_names,
  transform(
    from_json(cls_resp.result, 'STRUCT<codes: ARRAY<STRUCT<name: STRING, confidence: DOUBLE, rationale: STRING>>>').codes,
    x -> x.confidence
  ) AS code_classifications_confidence_scores,
  transform(
    from_json(cls_resp.result, 'STRUCT<codes: ARRAY<STRUCT<name: STRING, confidence: DOUBLE, rationale: STRING>>>').codes,
    x -> x.rationale
  ) AS code_classifications_rationale,
  cls_resp.errorMessage::STRING AS classification_error,
  -- Parse ext JSON -> typed arrays
  from_json(
    ext_resp.result,
    'STRUCT<quoted_unit_rate: ARRAY<STRUCT<rate_type: STRING, per_unit: STRING, fixed_amount: DOUBLE, variable_amount: DOUBLE, amount: DOUBLE>>, competitor_or_current_supplier_rate: ARRAY<STRUCT<amount: DOUBLE, label: STRING, per_unit: STRING>>>'
  ).quoted_unit_rate AS quoted_rates,
  from_json(
    ext_resp.result,
    'STRUCT<quoted_unit_rate: ARRAY<STRUCT<rate_type: STRING, per_unit: STRING, fixed_amount: DOUBLE, variable_amount: DOUBLE, amount: DOUBLE>>, competitor_or_current_supplier_rate: ARRAY<STRUCT<amount: DOUBLE, label: STRING, per_unit: STRING>>>'
  ).quoted_unit_rate AS quoted_rates_full,
  from_json(
    ext_resp.result,
    'STRUCT<quoted_unit_rate: ARRAY<STRUCT<rate_type: STRING, per_unit: STRING, fixed_amount: DOUBLE, variable_amount: DOUBLE, amount: DOUBLE>>, competitor_or_current_supplier_rate: ARRAY<STRUCT<amount: DOUBLE, label: STRING, per_unit: STRING>>>'
  ).competitor_or_current_supplier_rate AS competitor_rate,
  CAST(NULL AS ARRAY<STRING>) AS extract_citations,
  ext_resp.errorMessage::STRING AS extract_error,
  _source_file
FROM scored;
