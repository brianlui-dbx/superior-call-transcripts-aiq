# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Setup — supporting objects for the Offer-Blocker POC
# MAGIC
# MAGIC This notebook runs **once, before the pipeline**. It creates the things the
# MAGIC pipeline reads but does not own. None of this is "AI" — it is plumbing and
# MAGIC synthetic reference data so the demo is self-contained.
# MAGIC
# MAGIC It creates:
# MAGIC 1. A **Volume** — `landing` (where transcript files arrive for Auto Loader).
# MAGIC 2. Copies the **sample transcript batch** into the landing Volume.
# MAGIC 3. `dim_salesforce_opportunity` — **synthetic** CRM data. The real workflow joins
# MAGIC    transcripts to Salesforce on phone number to learn each call's *opportunity*
# MAGIC    (a sales deal) and its *stage*/*region*. Our sample transcripts have no such
# MAGIC    fields, so we fabricate a small, deterministic Salesforce table keyed to the
# MAGIC    phone numbers actually present in the sample.
# MAGIC 4. `lookup_qualifier_config` — per-code-prefix ai_classify labels & instructions for
# MAGIC    the qualifier step in `gold_findings_raw`.

# COMMAND ----------

# Parameters are injected by the job (see resources/setup.job.yml). Defaults let
# you run the notebook interactively too.
dbutils.widgets.text("catalog", "dbw_brlui_stable")
dbutils.widgets.text("schema", "call_transcripts_poc")
dbutils.widgets.text("files_path", "")  # bundle's synced workspace files root

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FILES_PATH = dbutils.widgets.get("files_path")

# When run interactively (no files_path), fall back to the current notebook folder.
if not FILES_PATH:
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    FILES_PATH = "/Workspace" + "/".join(nb.split("/")[:-2])  # .../files (src/setup/setup -> files)

print(f"CATALOG={CATALOG}  SCHEMA={SCHEMA}  FILES_PATH={FILES_PATH}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %md ## 1. Volume — landing zone (Auto Loader source)

# COMMAND ----------

# DBTITLE 1,Cell 4
# A Volume is a Unity-Catalog-governed folder for files. `landing/transcripts` is
# where new transcript batches are dropped; the pipeline's Auto Loader watches it.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")

LANDING_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/landing/transcripts"
import os
os.makedirs(LANDING_DIR, exist_ok=True)
print("landing dir:", LANDING_DIR)

# COMMAND ----------

# MAGIC %md ## 2. Copy the sample transcript batch into the landing Volume
# MAGIC The sample (`data/batch_b_ny_001.json`) is a JSON **array** of 19 call
# MAGIC transcript records. Copying it here gives Auto Loader a file to ingest.

# COMMAND ----------

import shutil
src_json = f"{FILES_PATH}/data/batch_b_ny_001.json"
dst_json = f"{LANDING_DIR}/batch_b_ny_001.json"
shutil.copyfile(src_json, dst_json)
print(f"copied {src_json} -> {dst_json}  ({os.path.getsize(dst_json)} bytes)")

# COMMAND ----------

# MAGIC %md ## 3. Synthetic `dim_salesforce_opportunity`
# MAGIC One row per phone number found in the sample. Each phone maps to exactly ONE
# MAGIC opportunity (kept 1:1 so the phone-join doesn't multiply rows). We deliberately
# MAGIC place a few opportunities OUT of the demo regions so you can see the region
# MAGIC filter drop them. All values are deterministic (seeded by the phone) so re-runs
# MAGIC are stable.

# COMMAND ----------

import json, re, hashlib
from datetime import datetime, timedelta

def normalize_phone(p: str) -> str:
    """Same rule the pipeline uses: strip non-digits; if >=11 digits keep last 10
    (drops the US country code '1'). Returns '' if not a usable 10-digit number."""
    if p is None:
        return ""
    digits = re.sub(r"\D", "", str(p))
    if len(digits) >= 11:
        digits = digits[-10:]
    return digits if len(digits) == 10 else ""

# Read the raw sample directly in Python (small file) to collect the phone numbers.
with open(src_json, "r", encoding="utf-8") as f:
    records = json.load(f)

phones = sorted({normalize_phone(r.get("clientPhoneNumber")) for r in records} - {""})
print(f"{len(phones)} distinct usable phone numbers in the sample")

# Deterministic attribute assignment.
STAGES = ["Open", "Closed Won", "Closed Lost"]
IN_REGIONS = ["New York", "New Jersey"]
OUT_REGIONS = ["Ontario", "Massachusetts", "Connecticut"]  # excluded by the region filter

def sf_opp_id(phone: str) -> str:
    """Fabricate a realistic-looking 18-char Salesforce Opportunity ID from the phone."""
    h = hashlib.md5(phone.encode()).hexdigest().upper()
    body = "".join(c for c in h if c.isalnum())[:8]
    return ("006Rg00000" + body)[:18].ljust(18, "0")

rows = []
n = len(phones)
for i, phone in enumerate(phones):
    # Put the LAST 3 phones out-of-region to prove the filter works; the rest in NY/NJ.
    if i >= n - 3:
        region = OUT_REGIONS[i % len(OUT_REGIONS)]
    else:
        region = IN_REGIONS[i % len(IN_REGIONS)]
    stage = STAGES[i % len(STAGES)] if i % 4 != 3 else "Closed Won"  # bias toward Open/Closed Won
    created = (datetime(2026, 3, 1) + timedelta(days=(i * 3) % 40)).strftime("%Y-%m-%d %H:%M:%S")
    rows.append((phone, sf_opp_id(phone), stage, region, created))

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F

sf_schema = StructType([
    StructField("contact_phone", StringType(), False),   # 10-digit normalized join key
    StructField("opportunity_id", StringType(), False),  # Salesforce Opp ID (the deal)
    StructField("stage_name", StringType(), True),        # Open / Closed Won / Closed Lost
    StructField("region", StringType(), True),            # sales region
    StructField("created_datetime", StringType(), True),  # when the opportunity was opened
])
sf_df = (spark.createDataFrame(rows, sf_schema)
         .withColumn("created_datetime", F.to_timestamp("created_datetime")))
sf_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA}.dim_salesforce_opportunity")
print(f"wrote dim_salesforce_opportunity ({sf_df.count()} rows)")
display(sf_df.orderBy("region", "opportunity_id"))

# COMMAND ----------

# DBTITLE 1,Cell 13
# MAGIC %md ## 4. `lookup_qualifier_config` — ai_classify labels & instructions per code prefix
# MAGIC Used by `gold_findings_raw` to dynamically route the qualifier classification
# MAGIC without a large CASE statement. One row per 4x code prefix.

# COMMAND ----------

from pyspark.sql import Row

qualifier_rows = [
    Row(
        code_prefix="4A",
        labels_json='{"current-supplier": "Comparison to what customer says their current supplier charges.", "competitor-rate": "A rate or quote from another supplier being shopped (not their current one).", "advertised-rate": "A competitor published or advertised rate cited by the customer.", "high-rate": "Rate too high in absolute terms; no external comparison invoked.", "rate-increase": "Existing or returning customer reacting to a rate increase or renewal price.", "price-match": "Customer demanded a concession: match, discount, or revised quote as condition of proceeding.", "other": "A rate-pressure source no defined value fits."}',
        instructions="Classify the source of rate pressure for code 4A (Rate Competitiveness) in this propane sales call. Precedence: if the customer demanded a concession (match/discount), use price-match and note the comparison source."
    ),
    Row(
        code_prefix="4B",
        labels_json='{"rental/objection": "Tank or equipment rental fee — customer pushed back and it affected progression.", "rental/economics": "Tank or equipment rental fee — structurally uneconomical for customer usage.", "delivery/objection": "Delivery fee — customer pushed back.", "delivery/economics": "Delivery fee — structurally uneconomical for customer usage.", "install/objection": "Installation or setup charge — customer pushed back.", "MUC/objection": "Minimum usage charge — customer pushed back.", "MUC/economics": "Minimum usage charge — structurally uneconomical for customer usage.", "inspection/objection": "Safety or system inspection fee — customer pushed back.", "monitoring/objection": "Tank monitoring or auto-delivery service fee — customer pushed back.", "admin/objection": "Service, admin, or account fee — customer pushed back.", "stack/objection": "Fee stack rejected as a whole.", "other": "Other fee type or ground not covered above."}',
        instructions="Classify the specific fee type and ground (objection = customer pushed back; economics = structurally uneconomical for their usage) for code 4B (Ancillary Fees) in this propane sales call."
    ),
    Row(
        code_prefix="4C",
        labels_json='{"prebuy": "Customer wants pre-buy (fixed volume upfront at locked rate); not offered.", "ownership": "Customer wanted to own their tank and could not: unavailable or conditions made it unviable.", "rate-mechanism": "Rejects the pricing MODEL itself (fixed vs variable, capped, indexed, unknown future pricing).", "commitment-length": "The length of commitment was rejected or required (e.g., 5-year vs 3-year).", "supply-terms": "Exclusivity or minimum volume resisted as a structural requirement.", "delivery-model": "Customer wanted a different delivery model (will-call vs automatic) and was blocked.", "other": "A structural want or rejection no defined pattern fits."}',
        instructions="Classify the specific commercial model mismatch for code 4C in this propane sales call. Route on the customer WANT, not the complaint wording: wanted ownership but blocked -> ownership, even if voiced as fees."
    ),
    Row(
        code_prefix="4D",
        labels_json='{"auto-renewal": "Automatic renewal resisted by the customer.", "exit-terms": "Early-termination charges, cancellation conditions, notice periods, equipment removal, or transfer-on-sale.", "payment-billing": "COD, autopay-required-for-rate, budget-plan gates, or payment window issues.", "escalation-clause": "Resisted adjustment clauses, liability, maintenance responsibility, or property-access rights.", "process": "A required transaction step treated as barrier: e-signature only, prerequisite gating, documentation demands.", "complexity": "Contract as artifact rejected: too long, too much fine print, too complicated.", "other": "A binding mechanic no defined pattern fits."}',
        instructions="Classify the specific contract or transaction mechanic for code 4D in this propane sales call. Fees DURING service -> 4B not here. Fees for LEAVING -> exit-terms. Service gated behind prerequisite -> process."
    ),
    Row(
        code_prefix="4E",
        labels_json='{"service-area": "Location outside serviceable territory or delivery coverage.", "product": "Product type or grade not offered (e.g., cylinder fills, oil in propane-only market).", "equipment": "Tank size, equipment type, or configuration unavailable.", "site": "Access, safety, regulatory, or installation-feasibility limits at the property.", "timeline": "Required delivery or install timeframe could not be met (structural or capacity).", "capability": "A service capability Superior does not offer.", "other": "A supply gap no defined form fits."}',
        instructions="Classify the specific availability or serviceability gap for code 4E in this propane sales call. Must be a PHYSICAL/logistical gap, not commercial model (that is 4C). Timeline includes both structural and capacity-driven delays."
    ),
    Row(
        code_prefix="4F",
        labels_json='{"referral": "Expected referral credit as referred party; only the referrer gets it.", "threshold": "Projected usage below the qualifying volume for promo, waiver, or credit tier.", "ownership": "Owns their tank so leased-tank promos do not apply.", "prior-offer": "References a company rate or offer they saw, had, or heard that cannot be honored today.", "stacking": "Wanted to combine promotions and the stack was denied.", "policy-gate": "Chose a path the promo requires forgoing (refused credit check, chose COD, declined auto-delivery).", "timing": "Promo expired or not yet active in their region.", "other": "An ineligibility ground no defined value fits."}',
        instructions="Classify the specific promotion ineligibility ground for code 4F in this propane sales call. Must be DENIED or INELIGIBLE — not a promo that was successfully applied."
    ),
]

cfg_df = spark.createDataFrame(qualifier_rows)
cfg_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA}.lookup_qualifier_config"
)
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.lookup_qualifier_config CLUSTER BY (code_prefix)")
print(f"wrote lookup_qualifier_config ({cfg_df.count()} rows, clustered by code_prefix)")
display(cfg_df)

# COMMAND ----------

# MAGIC %md ## Done
# MAGIC Supporting objects are ready. Now run the pipeline:
# MAGIC `databricks bundle run offer_blocker_pipeline -t dev -p dbw-brlui-stable`