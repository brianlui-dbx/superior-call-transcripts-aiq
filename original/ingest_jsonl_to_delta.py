"""
JSON Ingestion to Delta Pipeline
================================================
Downloads JSON files from Azure Blob Storage and persists them to a
Delta table for downstream analytics.

Pipeline Steps:
    1. Connect to Azure Blob Storage using credentials from config.yaml.
    2. List all blobs under a given prefix.
    3. Load checkpoint from Delta table to identify already-processed blobs.
    4. Download only new blobs concurrently (asyncio semaphore).
    5. Persist to Delta table:
       - MERGE INTO on 'id' to deduplicate against existing records.
    6. Update checkpoint Delta table with newly processed blob names.

Concurrency Model:
    asyncio.Semaphore controls concurrent blob downloads.
    Each blob is downloaded as a single chunk (max_concurrency=1).
    Progress logged every 10,000 files.

Checkpoint System:
    Delta table stores blob names of all previously downloaded files
    to enable incremental/resumable runs.

Configuration (config/config.yaml -> azure):
    - blob_account: Azure Storage account name
    - blob_key: Storage account key
    - blob_container: Container name
"""


import os
import sys
root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, root)

import json
import asyncio
import time
import logging
from pathlib import Path

import yaml
import pandas as pd
import nest_asyncio
nest_asyncio.apply()

from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ClientAuthenticationError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, schema_of_json

from <package> import setup_logging

logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

# --- Configuration ---
script_dir = Path.cwd()
project_root = script_dir.parent

config_path = project_root / "config" / "config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)["<config_section>"]

log = setup_logging("jsonl_ingestion_to_delta", project_root)

CONCURRENCY = 100
BATCH_SIZE = 50  # Process this many blobs per batch to bound memory
BLOB_PREFIX = "<blob_prefix>/"  # e.g. "folder_name/"

TABLE_NAME = "<catalog>.<schema>.<table_name>"  # target Delta table
RECORD_CHECKPOINT_TABLE = "<catalog>.<schema>.<checkpoint_records>"  # record-level checkpoint
BLOB_CHECKPOINT_TABLE = "<catalog>.<schema>.<checkpoint_blobs>"  # blob-level checkpoint

bytes_downloaded = 0
files_downloaded = 0
semaphore = asyncio.Semaphore(CONCURRENCY)

# --- Blob Storage Access ---
async def create_blob_client(config):
    """Create and return an async BlobServiceClient and container client."""
    blob_account = config['blob_account']
    blob_key = config['blob_key']
    blob_container = config['blob_container']
    blob_service_client = BlobServiceClient(
        account_url=f"https://{blob_account}.blob.core.windows.net",
        credential=blob_key
    )
    container_client = blob_service_client.get_container_client(blob_container)
    return blob_service_client, container_client

# --- Blob Download ---
MAX_RETRIES = 3
RETRY_DELAY = 5

async def process_blob(blob, container_client, total_files, start_time):
    """Download a single blob and return parsed JSON content.
    Retries on ClientAuthenticationError (stale Date header)."""
    global files_downloaded, bytes_downloaded
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                blob_client = container_client.get_blob_client(blob.name)
                stream = await blob_client.download_blob(max_concurrency=1)
                data = await stream.readall()
                break  # Success
            except ClientAuthenticationError:
                if attempt == MAX_RETRIES:
                    log.error(f"Auth failed after {MAX_RETRIES} retries: {blob.name}")
                    return []  # Skip this blob rather than crash the pipeline
                log.warning(f"Auth error on {blob.name}, retrying in {RETRY_DELAY}s (attempt {attempt}/{MAX_RETRIES})")
                await asyncio.sleep(RETRY_DELAY)

        blob_bytes = len(data)
        # Parse JSONL: each line is a separate JSON object
        lines = data.decode("utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line.strip()]

        bytes_downloaded += blob_bytes
        files_downloaded += 1

        if (files_downloaded % 10000 == 0) or (files_downloaded == total_files):
            elapsed = time.time() - start_time
            log.info(
                f"Progress: {files_downloaded}/{total_files} files | "
                f"{(bytes_downloaded / (1024 * 1024)):.2f} MB in {elapsed:.1f}s"
            )
        return records


def get_new_blob_list(all_blobs, blob_checkpoint: set):
    """Filter blob list against blob checkpoint. Returns list of new blob objects."""
    new_blobs = [b for b in all_blobs if os.path.basename(b.name) not in blob_checkpoint]
    log.info(f"Found {len(all_blobs)} total blobs, {len(new_blobs)} new (skipping {len(all_blobs) - len(new_blobs)} already downloaded)")
    return new_blobs


async def download_batch(blobs_batch, container_client, total_files, start_time):
    """
    Download a single batch of blobs concurrently.
    
    Returns:
        Tuple of (list of parsed records, list of blob filenames in this batch)
    """
    tasks = [process_blob(blob, container_client, total_files, start_time) for blob in blobs_batch]

    results = []
    for coro in asyncio.as_completed(tasks):
        records = await coro
        if records:
            results.extend(records)

    blob_names = [os.path.basename(b.name) for b in blobs_batch]
    return results, blob_names


# --- Checkpoint Management ---
def load_blob_checkpoint():
    """Load already-downloaded JSONL blob filenames to skip re-downloading."""
    if not spark.catalog.tableExists(BLOB_CHECKPOINT_TABLE):
        log.info("Blob checkpoint table does not exist yet — first run")
        return set()
    try:
        rows = spark.table(BLOB_CHECKPOINT_TABLE).select("blob_name").collect()
        checkpoint = {row.blob_name for row in rows}
    except Exception as e:
        log.warning(f"Could not read blob checkpoint table: {e}")
        return set()
    log.info(f"Loaded {len(checkpoint)} blob-level checkpoint entries")
    return checkpoint


def load_record_checkpoint():
    """Load already-processed record IDs ('{id}.json') to skip duplicates."""
    if not spark.catalog.tableExists(RECORD_CHECKPOINT_TABLE):
        log.info("Record checkpoint table does not exist yet — first run")
        return set()
    try:
        rows = spark.table(RECORD_CHECKPOINT_TABLE).select("json_record_name").collect()
        checkpoint = {row.json_record_name for row in rows}
    except Exception as e:
        log.warning(f"Could not read record checkpoint table: {e}")
        return set()
    log.info(f"Loaded {len(checkpoint)} record-level checkpoint entries")
    return checkpoint


def save_blob_checkpoint(blob_names):
    """Append downloaded JSONL blob filenames to the blob checkpoint table."""
    if not blob_names:
        return
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BLOB_CHECKPOINT_TABLE} (
            blob_name STRING
        ) USING DELTA
    """)
    pdf = pd.DataFrame({"blob_name": blob_names})
    checkpoint_schema = StructType([StructField("blob_name", StringType(), True)])
    spark.createDataFrame(pdf, schema=checkpoint_schema) \
        .write.mode("append") \
        .format("delta") \
        .saveAsTable(BLOB_CHECKPOINT_TABLE)
    log.info(f"Blob checkpoint: {len(blob_names)} blob names saved")


def save_record_checkpoint(record_ids):
    """Append newly processed record IDs to the record checkpoint table."""
    if not record_ids:
        return
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RECORD_CHECKPOINT_TABLE} (
            json_record_name STRING
        ) USING DELTA
    """)
    pdf = pd.DataFrame({"json_record_name": record_ids})
    checkpoint_schema = StructType([StructField("json_record_name", StringType(), True)])
    spark.createDataFrame(pdf, schema=checkpoint_schema) \
        .write.mode("append") \
        .format("delta") \
        .saveAsTable(RECORD_CHECKPOINT_TABLE)
    log.info(f"Record checkpoint: {len(record_ids)} record IDs saved")


# --- Delta Table Write ---
def persist_to_delta(json_records):
    """
    Persist parsed JSON records to the Delta table using MERGE
    to deduplicate against existing rows. Supports schema evolution —
    new fields in the source JSON are automatically added as columns.

    Approach:
        1. Serialize records as JSON strings into a single-column DataFrame.
        2. Use schema_of_json() to infer schema, then from_json() to parse.
            Avoids writing temp files
           No file I/O — avoids FUSE/dbfs path issues on serverless.
        3. MERGE INTO the Delta table with schema evolution.
    """
    import uuid

    # Serialize records to JSON strings and create a single-column DataFrame
    log.info(f"Creating DataFrame from {len(json_records)} records...")
    json_lines = [json.dumps(record) for record in json_records]
    pdf = pd.DataFrame({"value": json_lines})
    string_df = spark.createDataFrame(pdf)

    table_exists = spark.catalog.tableExists(TABLE_NAME)

    if table_exists:
        # Use existing table schema for type compatibility (avoids merge field conflicts)
        existing_schema = spark.table(TABLE_NAME).schema
        existing_field_names = {f.name for f in existing_schema.fields}

        # Detect new columns in this batch that aren't in the table yet
        all_keys = set()
        for record in json_records:
            all_keys.update(record.keys())
        new_field_names = all_keys - existing_field_names

        if new_field_names:
            # Merge: existing types + new fields as STRING
            from pyspark.sql.types import StructType
            merged_schema = StructType(
                list(existing_schema.fields) +
                [StructField(name, StringType(), True) for name in new_field_names]
            )
            log.info(f"Schema: {len(existing_field_names)} existing + {len(new_field_names)} new columns")
            df = string_df.select(from_json(col("value"), merged_schema).alias("data")).select("data.*")
        else:
            df = string_df.select(from_json(col("value"), existing_schema).alias("data")).select("data.*")
    else:
        # First run — infer full schema from the longest record
        sample_idx = max(range(len(json_lines)), key=lambda i: len(json_lines[i]))
        schema_ddl = spark.createDataFrame([(json_lines[sample_idx],)], ["value"]) \
            .select(schema_of_json(col("value"))) \
            .first()[0]
        df = string_df.select(from_json(col("value"), schema_ddl).alias("data")).select("data.*")

    df_schema = df.schema  # Cache schema to avoid repeated Analyze RPCs
    record_count = df.count()
    log.info(f"Created DataFrame with {record_count} records, {len(df_schema.fields)} columns")

    table_exists = spark.catalog.tableExists(TABLE_NAME)

    if not table_exists:
        # First run — create table with dedup within batch
        log.info(f"Creating new table {TABLE_NAME}")
        df.dropDuplicates(['id']) \
            .write.mode("overwrite") \
            .option("mergeSchema", "true") \
            .format("delta") \
            .saveAsTable(TABLE_NAME)
    else:
        # Subsequent runs — MERGE with schema evolution to handle new fields
        log.info(f"Merging {record_count} records into {TABLE_NAME}")
        view_name = f"incoming_{uuid.uuid4().hex[:8]}"
        df.createOrReplaceTempView(view_name)
        spark.sql(f"""
            MERGE WITH SCHEMA EVOLUTION
            INTO {TABLE_NAME} AS target
            USING {view_name} AS source
            ON target.id = source.id
            WHEN NOT MATCHED THEN INSERT *
        """)
        spark.catalog.dropTempView(view_name)

    final_count = spark.table(TABLE_NAME).count()
    log.info(f"Table {TABLE_NAME} now has {final_count} total records")
    return final_count


# --- Main Pipeline ---
async def main():
    start_time = time.time()
    log.info("=" * 60)
    log.info("Blob -> Delta Ingestion")
    log.info("=" * 60)

    # Load both checkpoints
    blob_checkpoint = load_blob_checkpoint()      # JSONL blob filenames (skip re-download)
    record_checkpoint = load_record_checkpoint()  # Individual record IDs (skip duplicates)

    # Connect to blob storage
    log.info("Connecting to Azure Blob Storage...")
    blob_service_client, container_client = await create_blob_client(config)

    # List and filter blobs
    async with blob_service_client:
        all_blobs = [blob async for blob in container_client.list_blobs(name_starts_with=BLOB_PREFIX)]
        new_blobs = get_new_blob_list(all_blobs, blob_checkpoint)

        if not new_blobs:
            log.info("=" * 60)
            log.info("No new blobs to download")
            log.info("=" * 60)
            return

        # Process in batches to bound memory
        total_files = len(new_blobs)
        total_new_records = 0
        batch_start_time = time.time()

        for i in range(0, total_files, BATCH_SIZE):
            batch = new_blobs[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE
            log.info(f"--- Batch {batch_num}/{total_batches} ({len(batch)} blobs) ---")

            # Download this batch
            batch_records, batch_blob_names = await download_batch(
                batch, container_client, total_files, batch_start_time
            )

            if not batch_records:
                save_blob_checkpoint(batch_blob_names)
                continue

            # Filter at record level — record checkpoint stores "{id}.json"
            new_records = [r for r in batch_records if f"{r['id']}.json" not in record_checkpoint]
            log.info(f"Batch records: {len(batch_records)} downloaded, {len(new_records)} new")

            if new_records:
                # Persist to Delta table
                persist_to_delta(new_records)
                total_new_records += len(new_records)

                # Update record checkpoint (so next batch can skip these too)
                new_record_ids = [f"{r['id']}.json" for r in new_records]
                save_record_checkpoint(new_record_ids)
                record_checkpoint.update(new_record_ids)

            # save blob checkpoint to prevent re-downloading this batch
            save_blob_checkpoint(batch_blob_names)
            blob_checkpoint.update(batch_blob_names)

    # Summary
    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("Blob to Delta Ingestion Complete!")
    log.info(f"Files downloaded: {files_downloaded} ({bytes_downloaded / (1024 * 1024):.2f} MB)")
    log.info(f"New records persisted: {total_new_records}")
    log.info(f"Elapsed time: {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())