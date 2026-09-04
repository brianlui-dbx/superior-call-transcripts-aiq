# Databricks notebook source
# MAGIC %md
# MAGIC # Provision the Genie space (idempotent)
# MAGIC
# MAGIC Databricks Asset Bundles do not (yet) have a native Genie-space resource type, so
# MAGIC this notebook — run as a task in the bundle's setup job — provisions the Genie
# MAGIC space from the version-controlled `genie/genie_agent.json`. It is idempotent:
# MAGIC it finds the space by title and **updates it in place** (keeping the same space
# MAGIC id, so the dashboard's Genie link stays valid), or creates it if missing.
# MAGIC
# MAGIC Endpoints (confirmed from the CLI): list `GET /api/2.0/genie/spaces`,
# MAGIC create `POST /api/2.0/genie/spaces`, update `PATCH /api/2.0/genie/spaces/{id}`.

# COMMAND ----------

dbutils.widgets.text("files_path", "")     # bundle's synced workspace files root
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("title", "Offer Blocker Analytics")

FILES_PATH = dbutils.widgets.get("files_path")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
TITLE = dbutils.widgets.get("title")

if not FILES_PATH:
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    FILES_PATH = "/Workspace" + "/".join(nb.split("/")[:-2])  # .../files (src/setup/x -> files)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# The serialized_space field is a STRING containing the space JSON.
with open(f"{FILES_PATH}/genie/genie_agent.json", "r", encoding="utf-8") as f:
    serialized_space = f.read()

me = w.current_user.me().user_name
parent_path = f"/Workspace/Users/{me}/genie_spaces"
w.workspace.mkdirs(parent_path)  # create parent folder (no-op if it exists)

# COMMAND ----------

# Find an existing space with this title (paginate through the list).
existing_id = None
page_token = None
while True:
    params = {"page_token": page_token} if page_token else {}
    resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query=params) or {}
    for s in resp.get("spaces", []):
        if s.get("title") == TITLE:
            existing_id = s.get("space_id")
            break
    page_token = resp.get("next_page_token")
    if existing_id or not page_token:
        break

# COMMAND ----------

description = "Ask natural-language questions about propane sales-call offer blockers, call tone/topics, and drafted follow-up emails."

if existing_id:
    # Update in place — keeps the space id stable (so the dashboard's Genie link holds).
    w.api_client.do(
        "PATCH", f"/api/2.0/genie/spaces/{existing_id}",
        body={"serialized_space": serialized_space, "title": TITLE,
              "description": description, "warehouse_id": WAREHOUSE_ID},
    )
    print(f"Updated existing Genie space: {existing_id}")
    space_id = existing_id
else:
    created = w.api_client.do(
        "POST", "/api/2.0/genie/spaces",
        body={"warehouse_id": WAREHOUSE_ID, "title": TITLE, "description": description,
              "parent_path": parent_path, "serialized_space": serialized_space},
    )
    space_id = created.get("space_id")
    print(f"Created Genie space: {space_id}")

print("GENIE_SPACE_ID:", space_id)
