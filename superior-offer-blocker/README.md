# Offer-Blocker Transcript Analysis — Databricks POC

Converts a slow, manual "paste-transcripts-into-an-LLM-chat" workflow into an automated,
re-runnable Databricks pipeline that reads Superior Plus Propane sales-call transcripts and
produces a coded table of **offer blockers** — the reasons a customer did not accept an offer.

Built with **Auto Loader**, **Spark Declarative Pipelines (SDP)**, and **Databricks AI Functions**
(`ai_query`, `ai_classify`, `ai_extract`, `ai_analyze_sentiment`, `ai_summarize`, `ai_mask`),
all on **serverless** compute.

---

## Two tiers (every object is labelled)

| Tier | Meaning | Naming |
|------|---------|--------|
| **CORE** | Faithfully reproduces the two original hand-written Python scripts (`../original/*.py`). | no suffix |
| **ENHANCEMENT** | Net-new AI capability that did **not** exist in the original workflow. | suffix `_enh` |

Each SQL file opens with a `[CORE]` / `[ENHANCEMENT]` banner so a reviewer can tell at a glance
what reproduces today's workflow versus what is added value.

---

## What the pipeline does (bronze → silver → gold)

```
landing/*.json ──Auto Loader──▶ bronze_transcripts_ingest ──AUTO CDC (dedup on id)──▶ bronze_transcripts
                                                                                          │
dim_salesforce_opportunity (synthetic) ───────────────────────────────────────────────┐  │
                                                                                        ▼  ▼
                                        silver_transcript_sf_joined  (phone-join, Seg X of N, region filter)
                                                        │
                                                        ▼
                                        silver_opportunity_dialogue  (one dialogue string per opportunity)
                                          │                         │
   dim_code_categories (labels) ──▶ silver_issue_signals            │   [CORE task fns: ai_classify code, ai_extract rate]
                                          │                         │
   prompt_offer_blocker (prompt) ──▶ gold_findings_raw  (ai_query, hinted by the signals)
                                          │
                                          ▼
                       gold_offer_blocker_findings   ◀── THE deliverable ("AI Output" sheet)
                          ├─ gold_batch_inventory     (validation: "Detected N segments across M opportunities")
                          ├─ gold_primary_check       (validation: one Primary=Yes per blocker opp)
                          └─ gold_findings_quarantine (validation: suspect rows; normally empty)

── ENHANCEMENT (suffix _enh) ─────────────────────────────────────────────────────────────────
   silver_transcript_sf_joined ──▶ gold_call_enrichment_enh   (per call: sentiment · topic · NER · summary · PII-masked summary)
   silver_opportunity_dialogue + findings ──▶ gold_followup_email_enh   (per opportunity: blocker-aware follow-up email in JSON)
```

### The deliverable table: `gold_offer_blocker_findings`
Mirrors the manual workflow's "AI Output" spreadsheet, one row per coded candidate issue:
`Version · Batch · Opp ID · Salesforce Stage · Rate · Code · Code Name · Primary · Qualifier ·
Disposition · Confidence · Evidence · Key`. (The Pivot Summary and Backup sheets are intentionally
not reproduced.)

---

## Where each AI Function is used

| Function | CORE use | ENHANCEMENT use (`_enh`) |
|---|---|---|
| `ai_classify` | Blocker code 4A–4F (labels mirror `dim_code_categories`) — `silver_issue_signals` | Call **topic** routing — `gold_call_enrichment_enh` |
| `ai_extract` | Quoted **rate** (NER) — `silver_issue_signals` | Customer **CRM entities** (name, supplier, tank size, city…) — `gold_call_enrichment_enh` |
| `ai_query` | The compound offer-blocker findings — `gold_findings_raw` | Blocker-aware **follow-up email** (strict JSON) — `gold_followup_email_enh` |
| `ai_analyze_sentiment` | — | Customer **call tone** — `gold_call_enrichment_enh` |
| `ai_summarize` | — | Agent-facing **call summary** — `gold_call_enrichment_enh` |
| `ai_mask` | — | **PII redaction** of the summary for compliance — `gold_call_enrichment_enh` |

`ai_query` is the only function that can emit the variable-length, multi-field findings array, so it
stays the engine for the findings; `ai_classify` + `ai_extract` do real CORE work upstream (and are
cross-checked against the findings via a data-quality expectation).

---

## Project layout

```
databricks.yml                      Bundle + variables (catalog, schema, model_endpoint, regions, …)
resources/
  offer_blocker.pipeline.yml        The SDP pipeline (serverless)
  setup.job.yml                     One-time setup job
  offer_blocker.job.yml             Pipeline wrapped with a file-arrival trigger (incremental batches)
src/
  pipeline/*.sql                    One dataset per file (bronze/silver/gold + _enh)
  setup/setup.py                    Creates volumes, stages the sample, builds the dims + prompt table
seeds/
  code_categories.csv               The 4A–4F blocker-code catalog (-> dim_code_categories)
  offer_blocker_prompt_v6.txt       Verbatim v6 instruction prompt (-> prompt_offer_blocker)
data/
  batch_b_ny_001.json               Sample transcript batch (19 calls) for the demo
```

Everything is **parameterized** (see `variables:` in `databricks.yml`). Notably the model endpoint is
a parameter — swap models with `--var model_endpoint=…`, no code change. Note the endpoint must support
**batch inference** (e.g. `databricks-claude-sonnet-4`; some newest pay-per-token endpoints do not yet).

---

## How to run

```bash
PROFILE=dbw-brlui-stable

# 1. Validate + deploy
databricks bundle validate -t dev -p $PROFILE
databricks bundle deploy   -t dev -p $PROFILE

# 2. One-time setup (volumes, synthetic Salesforce dim, code catalog, prompt, sample data)
databricks bundle run setup_job -t dev -p $PROFILE

# 3. Run the pipeline
databricks bundle run offer_blocker_pipeline -t dev -p $PROFILE

# 4. Incremental: drop a new *.json into the landing Volume and the file-arrival
#    trigger on offer_blocker_ingest_job runs the pipeline automatically.
```

### Verify
```sql
SELECT inventory_line FROM <catalog>.<schema>.gold_batch_inventory;
SELECT * FROM <catalog>.<schema>.gold_offer_blocker_findings WHERE Code <> '—';
SELECT call_tone, call_topic, call_summary, call_summary_masked FROM <catalog>.<schema>.gold_call_enrichment_enh;
SELECT opportunity_id, email_json FROM <catalog>.<schema>.gold_followup_email_enh;
```

---

## Demo run result (sample batch)

`Detected 16 segments across 13 opportunities.` (3 out-of-region opportunities correctly filtered
out), findings coded across 4A/4B/4D/4E/4F with real transcript evidence, **0 rows quarantined**,
per-call sentiment/topic/entities/summary populated with PII masked, and a complete blocker-aware
follow-up email drafted per opportunity.

---

## Notes / deviations from the original

- **Model:** original used Azure OpenAI (GPT); this uses a Databricks-hosted Claude endpoint. Different
  model family → expect to re-calibrate the prompt; swap freely via the `model_endpoint` parameter.
- **`confidence`** is categorical (High/Medium/Low) to match the rubric + spreadsheet (the original
  Python used a 0–1 float).
- **Salesforce data is synthetic**, keyed to the sample's phone numbers, so findings are illustrative.
- **Chunking / inventory line / integrity trailer** from the manual chat are replaced by parallel
  per-opportunity inference plus pipeline Expectations and the validation views.
- **Input format:** the sample is one JSON array (`input_multiline=true`); set it to `false` for
  line-delimited JSON in production.
