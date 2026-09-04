# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # `ai_top_drivers` Exploration — Offer Blocker Analysis
# MAGIC
# MAGIC **Use Case:** Identify which dimensions (blocker code, deal stage, qualifier) contribute most to opportunities being classified as `hard_blocker` vs `mention_only`.
# MAGIC
# MAGIC `ai_top_drivers()` is a Beta table-valued function that performs contribution analysis between a control group and test group. It ranks dimension values that explain the most variance in a metric.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Azure Databricks workspace with the Beta preview enabled
# MAGIC - Pro or Serverless SQL Warehouse

# COMMAND ----------

# DBTITLE 1,Example 1: What drives hard_blocker vs mention_only dispositions?
# MAGIC %sql
# MAGIC -- Which dimensions (code, stage, qualifier) explain the most difference
# MAGIC -- between opportunities classified as hard_blocker vs mention_only?
# MAGIC --
# MAGIC -- is_test = TRUE  -> hard_blocker (the "changed" / concerning group)
# MAGIC -- is_test = FALSE -> mention_only (the baseline / expected group)
# MAGIC
# MAGIC WITH input AS (
# MAGIC   SELECT
# MAGIC     code_name,
# MAGIC     stage_name                                  AS deal_stage,
# MAGIC     coalesce(qualifier, 'unqualified')          AS qualifier,
# MAGIC     disposition_confidence,
# MAGIC     -- Metric: confidence score (numeric, required by ai_top_drivers)
# MAGIC     CAST(disposition_confidence AS DOUBLE)       AS confidence_metric,
# MAGIC     -- Test split: hard_blocker vs mention_only
# MAGIC     CASE
# MAGIC       WHEN disposition = 'hard_blocker'  THEN TRUE
# MAGIC       WHEN disposition = 'mention_only'  THEN FALSE
# MAGIC       ELSE NULL  -- other dispositions excluded (NULLs are dropped)
# MAGIC     END AS is_test
# MAGIC   FROM dbw_brlui_stable.call_transcripts_poc.gold_offer_blockers
# MAGIC   WHERE disposition IN ('hard_blocker', 'mention_only')
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ai_top_drivers(
# MAGIC   input      => TABLE(input),
# MAGIC   metric     => 'confidence_metric',
# MAGIC   is_test    => 'is_test',
# MAGIC   dimensions => ARRAY('code_name', 'deal_stage', 'qualifier'),
# MAGIC   aggregation => 'avg'
# MAGIC )
# MAGIC ORDER BY ABS(change) DESC;

# COMMAND ----------

# DBTITLE 1,Example 2: What drives friction (count of occurrences)?
# MAGIC %sql
# MAGIC -- Alternative use case: which code/stage combinations have disproportionately
# MAGIC -- MORE friction findings compared to resolved findings?
# MAGIC --
# MAGIC -- is_test = TRUE  -> friction (deal stalled or required concession)
# MAGIC -- is_test = FALSE -> resolved (concern dissolved after explanation)
# MAGIC
# MAGIC WITH input AS (
# MAGIC   SELECT
# MAGIC     code_name,
# MAGIC     stage_name                           AS deal_stage,
# MAGIC     coalesce(qualifier, 'unqualified')   AS qualifier,
# MAGIC     1                                    AS finding_count,  -- count metric
# MAGIC     CASE
# MAGIC       WHEN disposition = 'friction' THEN TRUE
# MAGIC       WHEN disposition = 'resolved' THEN FALSE
# MAGIC       ELSE NULL
# MAGIC     END AS is_test
# MAGIC   FROM dbw_brlui_stable.call_transcripts_poc.gold_offer_blockers
# MAGIC   WHERE disposition IN ('friction', 'resolved')
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ai_top_drivers(
# MAGIC   input       => TABLE(input),
# MAGIC   metric      => 'finding_count',
# MAGIC   is_test     => 'is_test',
# MAGIC   dimensions  => ARRAY('code_name', 'deal_stage', 'qualifier'),
# MAGIC   aggregation => 'count'
# MAGIC )
# MAGIC ORDER BY ABS(change) DESC;

# COMMAND ----------

# DBTITLE 1,Interpreting results
# MAGIC %md
# MAGIC ## How to Read the Output
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC | --- | --- |
# MAGIC | `dimension` | The column name being segmented |
# MAGIC | `segment` | The specific value or range within that dimension |
# MAGIC | `change` | Contribution to the overall metric change (positive = drives test group higher) |
# MAGIC | `support` | Fraction of rows in this segment |
# MAGIC
# MAGIC **Actionable insight:** Segments with the largest `|change|` are the deal attributes that most explain *why* some objections kill deals while others don't. Use these to prioritize:
# MAGIC * Rep coaching ("when you see code 4A + Closed Lost stage, escalate immediately")
# MAGIC * Offer design changes ("4C/tank-ownership is disproportionately a hard blocker in New Jersey")
# MAGIC * Qualifier refinement ("current-supplier qualifier under 4A is mostly mention_only — deprioritize")