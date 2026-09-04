"""
Offer Blocker Transcript Analysis Pipeline
============================================

This module processes inside sales call transcripts to identify offer blockers
(reasons why a customer did not accept an offer) using Azure OpenAI with
structured (Pydantic) output parsing via LangChain.

Workflow:
---------
1. Loads transcript data and Salesforce opportunity data from Spark tables.
2. Cleans and normalizes phone numbers, then joins the two datasets on
   `clientPhoneNumber == contact_phone`.
3. Filters to configured regions, ranks segments chronologically
   per phone number, and builds a combined key for traceability.
4. Groups transcript segments by `opportunity_id` and formats the
   interwoven transcript blocks (agent/customer dialogue) into a prompt-ready
   string.
5. Sends each opportunity's concatenated transcripts to Azure OpenAI
   asynchronously (up to 100 concurrent requests) for structured extraction.
6. The LLM returns a list of `TranscriptItem` findings per opportunity,
   each containing: opp_id, rate, code, primary flag, qualifier,
   disposition, confidence score, and supporting evidence.
7. Retries failed requests with exponential backoff (max 3 attempts).
8. Writes results to both a JSONL text file and an Excel spreadsheet in
   the `gpt_EDA_results/` subdirectory. - Future expectation is to move the results to the data warehouse

Key Components:
---------------
- TranscriptItem / TranscriptResponse: Pydantic models enforcing the
  structured output schema from the LLM.
- extract_essential_fields(): Strips each transcript record to
  only the fields needed for analysis.
- format_transcripts(): Renders a list of transcript dicts
  into a human-readable prompt string.
- process_transcripts(): Async function handling a single
  opportunity's LLM call with retries.
- run_eda(): Orchestrates parallel async calls
  across all opportunity batches.
- main(): Entry point — loads data from Spark,
  joins, filters, and kicks off the
  async analysis pipeline.

Dependencies:
-------------
- Azure OpenAI (via LangChain's AzureChatOpenAI)
- PySpark (data loading and preparation)
- Pydantic (structured output validation)
- nest_asyncio (allows asyncio.run inside environments with an existing loop)
- httpx, pandas, openpyxl

Configuration:
--------------
Reads LLM credentials and model settings from a YAML config file.
"""
import os
import sys
from pathlib import Path
import yaml
import time
from typing import Dict, Optional
import pandas as pd
import json
import logging
import asyncio
import nest_asyncio
nest_asyncio.apply()
import warnings

from pydantic import BaseModel, Field

from langchain_openai import AzureChatOpenAI
import httpx

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType

root = os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), "..")))
sys.path.insert(0, root)
from  <package> import setup_logging
from <package> import _prompt  # project-specific prompt template

script_dir = Path.cwd()
project_root = script_dir.parent.parent

log = setup_logging("offer_blocker", script_dir.parent)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")

CONCURRENCY = 100
semaphore = asyncio.Semaphore(CONCURRENCY)
MAX_WORKERS = 20
total_processed = 0
total_transcripts = 0

config_path = project_root / "config" / "config.yaml"
try:
    with open(config_path, 'r') as f:
        cfg = yaml.safe_load(f)["<config_section>"]
except FileNotFoundError:
    log.error(f"Config file not found: {config_path}")
    raise SystemExit(1)
except (yaml.YAMLError, KeyError) as e:
    log.error(f"Invalid config file or missing config section: {e}")
    raise SystemExit(1)

try:
    endpoint = cfg['endpoint']
    model_name = cfg['model_name']
    deployment = cfg['deployment']
    subscription_key = cfg['<api_key>']
    api_version = cfg['api_version']
except KeyError as e:
    log.error(f"Missing required config key: {e}")
    raise SystemExit(1)

llm = AzureChatOpenAI(model=model_name, api_key=subscription_key, api_version=api_version, azure_endpoint=endpoint,  # noqa: credentials loaded from config
                      timeout=300,
                      max_retries=0,
                      http_client=httpx.Client(
                          http2=False,
                          limits=httpx.Limits(
                              max_keepalive_connections=0,
                              max_connections=MAX_WORKERS + 2
                          )
                      ))

MAX_RETRIES = 3
RETRY_DELAY = 5


class TranscriptItem(BaseModel):
    opp_id: str
    #salesforce_stage: str
    rate: str
    code: str
    #Code_Name
    primary: bool
    qualifier: str
    disposition: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: str

class TranscriptResponse(BaseModel):
    findings: list[TranscriptItem] = Field(description="All findings extracted from the transcript")

pydantic_llm = llm.with_structured_output(TranscriptResponse) 
chain = _prompt | pydantic_llm 


def extract_essential_fields(transcript_obj: Dict) -> Optional[Dict]:
    """
    Extract only the essential fields (id and interwovenTranscript) from a transcript object.
    
    Args:
        transcript_obj: Full transcript JSON object from JSONL file
        
    Returns:
        Dictionary with only 'id' and 'interwovenTranscript' fields, or None if:
        - 'id' field is missing
        - 'interwovenTranscript' field is missing
        - 'interwovenTranscript' doesn't have 'transcriptBlock' array
        
    Note:
        Preserves the exact structure: {"id": "...", "interwovenTranscript": {"transcriptBlock": [...]}}
        Each transcriptBlock item only contains channelName and text.
    """
    if 'id' not in transcript_obj or 'interwovenTranscript' not in transcript_obj:
        return None
    interwoven = transcript_obj['interwovenTranscript']
    if not isinstance(interwoven, dict) or 'transcriptBlock' not in interwoven:
        return None
    
    # Strip transcriptBlock to only channelName and text
    transcript_blocks = []
    for block in interwoven['transcriptBlock']:
        if isinstance(block, dict) and 'channelName' in block and 'text' in block:
            transcript_blocks.append({
                'channelName': block['channelName'],
                'text': block['text']
            })

    try:
        return {
            'id': transcript_obj['id'],
            'acdContactId': transcript_obj['acdContactId'],
            'segmentContactId': transcript_obj['segmentContactId'],
            'interactionId': transcript_obj['interactionId'],
            'agent': transcript_obj['agentName'],
            'agentId': transcript_obj['agentId'],
            'startTime': transcript_obj['startTime'],
            'endTime': transcript_obj['endTime'],
            'recordingStartTime': transcript_obj['recordingStartTime'],
            'recordingEndTime': transcript_obj['recordingEndTime'],
            'totalDurationSeconds': transcript_obj['totalDurationSeconds'],
            'interactionDurationSeconds': transcript_obj['interactionDurationSeconds'],
            'outbound': transcript_obj['outbound'],
            'clientAni': transcript_obj['clientAni'],
            'clientPhoneNumber': transcript_obj['clientPhoneNumber'],
            'clientDialedIn': transcript_obj['clientDialedIn'],
            'openReasonType': transcript_obj['openReasonType'],
            'closeReasonType': transcript_obj['closeReasonType'],
            'combined_key': transcript_obj['combined_key'],
            'opportunity_id': transcript_obj['opportunity_id'],
            'stage_name': transcript_obj['stage_name'],
            'interwovenTranscript': {
                'transcriptBlock': transcript_blocks
            }
        }
    except KeyError as e:
        log.warning(f"Missing field {e} in transcript {transcript_obj.get('id', 'unknown')}")
        return None

def format_transcripts(data):
    full_string = []
    for conversation in data:
        blocks = conversation.get('interwovenTranscript', {}).get("transcriptBlock", [])
        header = f"""
        ============================================
        Segment ID: {conversation["opportunity_id"]}
        {conversation["combined_key"]}
        --------------------------------------------
        """

        lines = []
        for b in blocks:
            role = b.get("channelName")
            text = b.get('text')
            if text:
                lines.append(f'{role}: {text}')
        full_string.append(f'{header}\n {"\n".join(lines)}')

    return "\n".join(full_string)

async def process_transcripts(row, filtered_comments, i):
    global total_processed
    async with semaphore:
        formatted_transcripts = format_transcripts(row) # it returns a string
        
        #log.info(formatted_transcripts)

        for attempt in range(MAX_RETRIES):
            try:
                response = await asyncio.wait_for(chain.ainvoke(
                    {"transcripts": formatted_transcripts}), timeout=180)
                total_processed += 1
                log.info(f"Total findings found for {row[0].get('opportunity_id')} = {len(response.findings)}")

                if (total_processed % 1000 == 0) or (total_processed == total_transcripts):
                    log.info(f"Total transcripts processed = {total_processed}")
                return [
                    { 
                     "Opp_ID": str(item.opp_id),
                        "Salesforce_Stage": row[0].get('stage_name'),
                        "Rate": str(item.rate),
                        "Code": str(item.code),
                        #Code_Name
                        "Primary": str(item.primary),
                        "Qualifier": str(item.qualifier),
                        "Disposition": str(item.disposition),
                        "Confidence": str(item.confidence),
                        "Evidence": str(item.evidence)
                        }
                        for item in response.findings]

            except asyncio.TimeoutError:
                if attempt == MAX_RETRIES - 1:
                    filtered_comments.append((i, formatted_transcripts))
                    log.info(f"{i}, {row[0].get('segmentContactId')}: timed out on attempt {attempt}")
                    return None

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                else:
                    log.warning(f'Failed batch {i}, {row[0].get('segmentContactId')}: {e}')
                    filtered_comments.append((i, formatted_transcripts))
                    return None

async def run_eda(batches):
    filtered_comments = []
    results = []
    log.info("Processing batches")
    
    call_purpose_tasks = []

    for i, batch in batches.items():
        call_purpose_tasks.append(process_transcripts(batch, filtered_comments, i))
    result = await asyncio.gather(*call_purpose_tasks) # use asyncio.gather for parallelism
    log.info(type(result))

    for isr_list in result:
        if isr_list is None:
            continue
        for isr in isr_list:
            if not isinstance(isr, dict):
                continue
            results.append(isr)
    return (results, filtered_comments)


def write_to_folder(file: list, file_name: str):
    path = script_dir / "gpt_EDA_results"
    path.mkdir(parents=True, exist_ok=True)
    log.info(f"The row size of output = {len(file)}")

    if not file:
        log.warning("No results to write — skipping file output")
        return

    try:
        df = pd.DataFrame(file)
        file_path = path / file_name
        df.to_excel(file_path, index=False)
        log.info(f"Results written to {file_path}")
    except (OSError, PermissionError) as e:
        log.error(f"Failed to write output file {file_name}: {e}")
    except Exception as e:
        log.error(f"Unexpected error writing {file_name}: {e}")  

async def main():
    global total_transcripts
    start_time = time.time()


    SALESFORCE_TABLE = "<catalog>.<schema>.table"
    TRANSCRIPT_TABLE = "<catalog>.<schema>.table"

    if not (spark.catalog.tableExists(SALESFORCE_TABLE) and spark.catalog.tableExists(TRANSCRIPT_TABLE)):
        log.error(f"Required tables {SALESFORCE_TABLE} and/or {TRANSCRIPT_TABLE} do not exist")
        return

    try:
        transcript_spark = spark.table(TRANSCRIPT_TABLE)

        cleaned_ani = F.regexp_replace(F.col('clientAni'), r'\D', '').cast(StringType())
        cleaned_phone_num = F.regexp_replace(F.col('clientPhoneNumber'), r'\D', "").cast(StringType())

        transcript_spark = transcript_spark \
            .withColumn('clientAni', F.when(F.length(cleaned_ani) >= 11, F.substring(cleaned_ani, -10, 10)).otherwise(cleaned_ani)) \
            .withColumn('clientPhoneNumber', F.when(F.length(cleaned_phone_num) >= 11, F.substring(cleaned_phone_num, -10, 10)).otherwise(cleaned_phone_num)) \
            .filter(F.length(F.col("clientPhoneNumber")) == 10)

        salesforce_360_spark = spark.table(SALESFORCE_TABLE)

        merged_df = salesforce_360_spark.join(transcript_spark, on=salesforce_360_spark['contact_phone']==transcript_spark['clientPhoneNumber'], how='left')

        total_segments = merged_df.groupBy(F.col("clientPhoneNumber").alias("seg_clientPhoneNumber")).agg(F.count("*").alias("count"))

        window_spec = Window.partitionBy("clientPhoneNumber").orderBy(F.col("recordingStartTime"))
        merged_df = merged_df \
            .withColumn("match", F.when(F.col("clientPhoneNumber") == F.col("contact_phone"), 1).otherwise(0)) \
            .filter(F.col("match") == 1)

        merged_df = merged_df \
            .join(total_segments, on=merged_df["clientPhoneNumber"]==total_segments["seg_clientPhoneNumber"], how="left") \
            .withColumn("position", F.row_number().over(window_spec)) \
            .withColumn("combined_key", F.concat(F.col("opportunity_id"), F.lit(" | Created: "), F.col("created_datetime"), F.lit(" | "), F.col("id"), F.lit(" | Seg "), F.col("position"), F.lit(" of "), F.col("count"), F.lit(" | Call date: "), F.col("recordingStartTime"), F.lit(" | "), F.col("interactionDurationSeconds"), F.lit("s"))) \
            .filter(F.col("region").isin(["<region_1>", "<region_2>"])) \
            .drop("match", "seg_clientPhoneNumber")
        log.info(f'Total number of matched rows = {merged_df.count()}')

        try:
            json_files = [row.asDict(recursive=True) for row in merged_df.collect()]
        except Exception as e:
            log.error(f"Failed to collect merged DataFrame (possible OOM): {e}")
            return

        sf_ids = set()
        transcript_dict = {}
        for transcript_obj in json_files:
            transcripts = extract_essential_fields(transcript_obj)

            if transcripts:
                key = transcripts.get('opportunity_id')

                if key not in sf_ids:
                    transcript_dict[key] = []
                    sf_ids.add(key)
                transcript_dict[key].append(transcripts)

        length = len(transcript_dict)
        total_transcripts = length
        log.info(f'Transcripts with interwoven data extracted = {length}')
        
        if not transcript_dict:
            log.warning("No transcripts with interwoven data found — nothing to process")
            return

        transcript_sample = dict(list(transcript_dict.items())[:100])
        log.info("GPT transcript analysis...")
        gpt_start_time = time.time()
        results, errors = await run_eda(transcript_sample)

        if errors:
            log.warning(f"{len(errors)} batches failed after retries")

        log.info("Saving File >>>>")
        # Future development - move to warehouse
        output_dir = script_dir / "gpt_EDA_results"
        output_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = output_dir / "offer_blocker_isr_transcript.txt"

        try:
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for line in results:
                    f.write(json.dumps(line, ensure_ascii=False) + "\n")
        except (OSError, PermissionError) as e:
            log.error(f"Failed to write JSONL output: {e}")

        write_to_folder(results, "offer_blocker_ISR_transcript_analysis.xlsx")

        end_time = time.time()
        log.info("GPT Processing complete!")
        log.info(f"Total time: {(end_time - start_time):.2f}s ({(end_time - start_time)/60:.2f} minutes)")
        log.info(f"Results: {len(results)} findings from {len(transcript_sample)} opportunities")

    except Exception as e:
        log.error(f"Pipeline failed: {type(e).__name__}: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())