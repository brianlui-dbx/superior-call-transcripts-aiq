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
        '\n\nINSTRUCTIONS:\n',
        'You are analyzing sales call transcripts for a propane/energy supplier (Superior) between Superior\'s agent and a client. Identify which offer-blocker codes apply based on what the client explicitly says or demonstrates through behaviour. Key rules: (1) Only assign a code if there is clear evidence in the dialogue — do not infer intent beyond what is stated. (2) If the customer\'s concern dissolved once the rep gave an accurate explanation, do NOT assign — the offer was not the barrier. (3) If the REP\'s handling was the gap (inaccurate, evasive), do NOT assign — that is a rep-handling issue, not an offer issue. (4) A customer with no genuine switch intent (benchmarking or price-checking only) has no offer blocker. (5) One-question litmus for ambiguous cases: complaint about the rate number -> 4A; about a bolt-on operating charge -> 4B; about the deal shape -> 4C; about what binds them to it -> 4D; about what Superior cannot physically provide -> 4E; about a deal they were advertised but cannot get -> 4F.',
        '\n\nCODE DEFINITIONS JSON:\n',
        '{\n        "4A. Rate/price uncompetitive": "The customer reacts to the quoted per-unit rate as too high, or compares it unfavourably to a competitor or current supplier. NOT a generic price concern before a specific rate is quoted. NOT a non-rate fee (that is 4B). NOT an objection to rate DESIGN like variability or Year-2 uncertainty (that is 4C rate-mechanism).",\n        "4B. Ancillary fees barrier": "Delivery, tank rental, installation, MUC, inspection, monitoring, or admin fees — charges paid WHILE a customer — break the economics or trigger pushback. NOT fees for LEAVING the contract like early-termination (that is 4D). NOT fee objections where the underlying want is tank ownership (that is 4C).",\n        "4C. Commercial model mismatch": "The customer wants a fundamentally different deal shape (pre-buy, tank ownership, fixed vs variable pricing, commitment length, delivery model) than Superior offers. Route on the customer WANT, not the complaint wording: wanted ownership but voiced as fees is still 4C. NOT binding mechanics like auto-renewal or exit terms (that is 4D).",\n        "4D. Contract/transaction mechanics": "A binding mechanic — how the customer is locked in, gets out, or must transact — is the barrier. Includes auto-renewal, exit/cancellation terms, payment/billing terms, and process barriers (e-signature only, prerequisite gating). Customer accepts the deal shape but resists the machinery. NOT commitment length (that is 4C). NOT fees during service (that is 4B).",\n        "4E. Availability/serviceability gap": "Superior cannot physically provide the required product, equipment, coverage area, or timeline. Must be a PHYSICAL or logistical gap. Includes timeline when the customer requires delivery/install by a date that cannot be met. NOT a commercial model preference (that is 4C). NOT when service exists but is gated behind prerequisites (that is 4D).",\n        "4F. Promotion ineligibility": "The customer wanted a specific promotion, credit, referral discount, or prior offer and was DENIED or found ineligible. Must be ineligibility or access gap — NOT a promo successfully applied. A Superior offer they cannot access is 4F; a competitor advertised rate is 4A."\n      }',
        '\n\nTASK:\nReturn only strict JSON matching the response schema. Select all applicable codes (multilabel). For each selected code, include a brief rationale citing the key customer quote or behaviour as evidence and a numeric confidence in [0,1].',
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
        '\n\nINSTRUCTIONS:\n',
        'You are analyzing sales call transcripts for a propane/energy supplier (Superior) between Superior\'s agent and a client. Extract the per-unit propane rate quoted by Superior and any competitor or current-supplier rate mentioned by the client. Extract only rates EXPLICITLY stated in the transcript — never estimate, average, or infer a rate. Return null for any field where no rate is explicitly mentioned. When fixed and variable rates are quoted together as a dual offer, record them as ONE item with rate_type dual and populate both fixed_amount and variable_amount. Never duplicate a single rate across both fixed_amount and variable_amount.',
        '\n\nTASK:\nReturn only strict JSON matching the response schema. Do not add prose.',
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
