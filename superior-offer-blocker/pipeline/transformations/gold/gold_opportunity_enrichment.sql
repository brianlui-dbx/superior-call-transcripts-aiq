-- ============ gold_opportunity_enrichment ============
-- WHAT THIS STEP DOES
--   Runs two task-specific AI Functions per opportunity to produce classified codes and rates:
--     * ai_classify -> candidate blocker codes (4A–4F), with confidence scores / rationale
--     * ai_extract  -> quoted and competitor per-unit price(s) as structured JSON (VARIANT)
--
--   Reads from silver_opportunity_dialogue (aggregated, DQ-validated transcripts).
CREATE OR REFRESH MATERIALIZED VIEW gold_opportunity_enrichment AS
WITH scored AS (SELECT
    opportunity_id,
    stage_name,
    transcript_text,
    _source_file,
    ai_classify(
      transcript_text,
      '{
        "4A. Rate/price uncompetitive": "The customer reacts to the quoted per-unit rate as too high, or compares it unfavourably to a competitor or current supplier. NOT a generic price concern before a specific rate is quoted. NOT a non-rate fee (that is 4B). NOT an objection to rate DESIGN like variability or Year-2 uncertainty (that is 4C rate-mechanism).",
        "4B. Ancillary fees barrier": "Delivery, tank rental, installation, MUC, inspection, monitoring, or admin fees — charges paid WHILE a customer — break the economics or trigger pushback. NOT fees for LEAVING the contract like early-termination (that is 4D). NOT fee objections where the underlying want is tank ownership (that is 4C).",
        "4C. Commercial model mismatch": "The customer wants a fundamentally different deal shape (pre-buy, tank ownership, fixed vs variable pricing, commitment length, delivery model) than Superior offers. Route on the customer WANT, not the complaint wording: wanted ownership but voiced as fees is still 4C. NOT binding mechanics like auto-renewal or exit terms (that is 4D).",
        "4D. Contract/transaction mechanics": "A binding mechanic — how the customer is locked in, gets out, or must transact — is the barrier. Includes auto-renewal, exit/cancellation terms, payment/billing terms, and process barriers (e-signature only, prerequisite gating). Customer accepts the deal shape but resists the machinery. NOT commitment length (that is 4C). NOT fees during service (that is 4B).",
        "4E. Availability/serviceability gap": "Superior cannot physically provide the required product, equipment, coverage area, or timeline. Must be a PHYSICAL or logistical gap. Includes timeline when the customer requires delivery/install by a date that cannot be met. NOT a commercial model preference (that is 4C). NOT when service exists but is gated behind prerequisites (that is 4D).",
        "4F. Promotion ineligibility": "The customer wanted a specific promotion, credit, referral discount, or prior offer and was DENIED or found ineligible. Must be ineligibility or access gap — NOT a promo successfully applied. A Superior offer they cannot access is 4F; a competitor advertised rate is 4A."
      }',
      map(
        'version','2.1',
        'multilabel','true',
        'enableConfidenceScores','true',
        'enableRationales','true',
        'instructions',
        'You are analyzing sales call transcripts for a propane/energy supplier (Superior) between Superior\'s agent and a client. Identify which offer-blocker codes apply based on what the client explicitly says or demonstrates through behaviour. Key rules: (1) Only assign a code if there is clear evidence in the dialogue — do not infer intent beyond what is stated. (2) If the customer\'s concern dissolved once the rep gave an accurate explanation, do NOT assign — the offer was not the barrier. (3) If the REP\'s handling was the gap (inaccurate, evasive), do NOT assign — that is a rep-handling issue, not an offer issue. (4) A customer with no genuine switch intent (benchmarking or price-checking only) has no offer blocker. (5) One-question litmus for ambiguous cases: complaint about the rate number -> 4A; about a bolt-on operating charge -> 4B; about the deal shape -> 4C; about what binds them to it -> 4D; about what Superior cannot physically provide -> 4E; about a deal they were advertised but cannot get -> 4F.'
      )
    ) AS cls,
    ai_extract(
      transcript_text,
      '{
        "quoted_unit_rate": {
          "type": "array",
          "description": "Every distinct rate offer quoted by Superior during the call. When fixed and variable rates are quoted together as a dual offer, group them as a single item with rate_type dual. Null if no Superior rate is explicitly stated.",
          "items": {
            "type": "object",
            "properties": {
              "rate_type": {"type": "enum", "labels": ["dual", "fixed", "variable"], "description": "The pricing structure of this offer. Use dual when both a fixed and variable rate are quoted together. Use fixed or variable for single-type offers. Null when the rate cannot be clearly classified as fixed or variable."},
              "per_unit": {"type": "string", "description": "Denominator unit of the rate exactly as stated (e.g. gal, kWh, therm). Null if no unit is explicitly mentioned."},
              "fixed_amount": {"type": "number", "description": "The fixed per-unit rate value exactly as stated. Populated when rate_type is dual or fixed. Null otherwise. If a promo intro rate and ongoing rate were both quoted, record only the ongoing rate."},
              "variable_amount": {"type": "number", "description": "The variable per-unit rate value exactly as stated. Populated when rate_type is dual or variable. Null otherwise. If volume tiers were cited, record the rate for the customer-stated tier."},
              "amount": {"type": "number", "description": "The per-unit rate value when rate_type is null (unclassified). Null when rate_type is dual, fixed, or variable — use fixed_amount or variable_amount instead."}
            }
          }
        },
        "competitor_or_current_supplier_rate": {
          "type": "array",
          "description": "Competitor or current-supplier rates mentioned by the client. Null if no competitor or currentw-supplier rate is mentioned.",
          "items": {
            "type": "object",
            "properties": {
              "amount": {"type": "number", "description": "The numeric per-unit rate value exactly as stated by the client. Do not estimate or average."},
              "label": {"type": "enum", "labels": ["current_supplier", "competitor", "generic"], "description": "Use current_supplier when the client references their existing provider rate. Use competitor when a named or implied competing offer is cited. Use generic only when the source is generic or unidentifiable. If multiple rates, include all ordered by emphasis."},
              "per_unit": {"type": "string", "description": "Denominator unit of the rate exactly as stated (e.g. gal, kWh, therm). Null if no unit is explicitly mentioned."}
            }
          }
        }
      }',
      map(
        'version','2.1',
        'mode','precision',
        'enableConfidenceScores','true',
        'enableCitations','true',
        'instructions',
        'You are analyzing sales call transcripts for a propane/energy supplier (Superior) between Superior\'s agent and a client. Extract the per-unit propane rate quoted by Superior and any competitor or current-supplier rate mentioned by the client. Extract only rates EXPLICITLY stated in the transcript — never estimate, average, or infer a rate. Return null for any field where no rate is explicitly mentioned. When fixed and variable rates are quoted together as a dual offer, record them as ONE item with rate_type dual and populate both fixed_amount and variable_amount. Never duplicate a single rate across both fixed_amount and variable_amount.'
      )
    ) AS ext
  FROM
    silver_opportunity_dialogue
)
SELECT
  opportunity_id,
  stage_name,
  transcript_text,
  transform(
    cls:response::ARRAY<STRUCT<value: STRING, confidence_score: DOUBLE, rationale: STRING>>,
    x -> x.value
  ) AS code_classification_names,
  transform(
    cls:response::ARRAY<STRUCT<value: STRING, confidence_score: DOUBLE, rationale: STRING>>,
    x -> x.confidence_score
  ) AS code_classifications_confidence_scores,
  transform(
    cls:response::ARRAY<STRUCT<value: STRING, confidence_score: DOUBLE, rationale: STRING>>,
    x -> x.rationale
  ) AS code_classifications_rationale,
  cls:error_message::STRING AS classification_error,
  transform(
    ext:response:quoted_unit_rate::ARRAY<STRUCT<
      rate_type: STRUCT<value: STRING>,
      per_unit: STRUCT<value: STRING>,
      fixed_amount: STRUCT<value: DOUBLE>,
      variable_amount: STRUCT<value: DOUBLE>,
      amount: STRUCT<value: DOUBLE>
    >>,
    x -> named_struct(
      'rate_type', x.rate_type.value,
      'per_unit', x.per_unit.value,
      'fixed_amount', x.fixed_amount.value,
      'variable_amount', x.variable_amount.value,
      'amount', x.amount.value
    )
  ) AS quoted_rates,
  ext:response:quoted_unit_rate AS quoted_rates_full,
  ext:response:competitor_or_current_supplier_rate AS competitor_rate,
  ext:metadata:citations AS extract_citations,
  ext:error_message::STRING AS extract_error,
  _source_file
FROM
  scored;