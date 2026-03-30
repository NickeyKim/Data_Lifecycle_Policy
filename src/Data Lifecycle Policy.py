# Databricks notebook source
# MAGIC %md
# MAGIC # Data Lifecycle Policy Notebook
# MAGIC ## This is for deleting saved data in the workspace which is longer than 3 months old.
# MAGIC
# MAGIC Input: RUN_MODE, TARGET_CATALOGS
# MAGIC

# COMMAND ----------

# DBTITLE 1,initial settings
from pyspark.sql import functions as F
from pyspark.sql import Row

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, BooleanType
)
from datetime import datetime, timedelta

# Databricks widget
dbutils.widgets.text("TARGET_CATALOGS", "", "TARGET_CATALOGS")

# Get TARGET_CATALOGS
TARGET_CATALOGS = dbutils.widgets.get("TARGET_CATALOGS")


if not TARGET_CATALOGS:
    raise Exception("Please input TARGET_CATALOGS.")


dbutils.widgets.text("RUN_MODE", "", "RUN_MODE")
RUN_MODE = dbutils.widgets.get("RUN_MODE")


#TARGET_CATALOGS = dbutils.widgets.get("TARGET_CATALOGS")
#EXCLUDE_SCHEMAS = dbutils.widgets.get("EXCLUDE_SCHEMAS")
#TARGET_CATALOGS = ["users"]     # it is moved to the parameter
EXCLUDE_SCHEMAS = ["information_schema", "default", "_data_classification"]  # add more schemas if you need

STALE_DAYS = 90 # days

CANDIDATE_TABLE = "users.nakhoe_kim.lifecycle_candidates"
EXCLUSION_TABLE = "users.nakhoe_kim.lifecycle_exclusions"

#RUN_MODE = "DRY_RUN"   # "DRY_RUN" or "TRUNCATE"
#RUN_MODE = 'TRUNCATE' # it is moved to the parameter
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
NOW_TS = datetime.now()
STALE_CUTOFF_TS = NOW_TS - timedelta(days=STALE_DAYS)

print("TARGET_CATALOGS:", TARGET_CATALOGS)
print("RUN_ID:", RUN_ID)
print("STALE_CUTOFF_TS:", STALE_CUTOFF_TS)
print("RUN_MODE:", RUN_MODE)
print("EXCLUDE_SCHEMAS:", EXCLUDE_SCHEMAS)

# COMMAND ----------

# DBTITLE 1,Create governance tables one time
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS users.nakhoe_kim;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS users.nakhoe_kim.lifecycle_candidates (
# MAGIC   run_id STRING,
# MAGIC   scanned_at TIMESTAMP,
# MAGIC   table_catalog STRING,
# MAGIC   table_schema STRING,
# MAGIC   table_name STRING,
# MAGIC   fqtn STRING,
# MAGIC   last_write_ts TIMESTAMP,
# MAGIC   last_op STRING,
# MAGIC   is_stale_90d BOOLEAN,
# MAGIC   action_planned STRING,   -- DRY_RUN / TRUNCATE / SKIP
# MAGIC   action_executed STRING,  -- SUCCESS / FAILED / SKIPPED
# MAGIC   message STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS users.nakhoe_kim.lifecycle_exclusions (
# MAGIC   table_catalog STRING,
# MAGIC   table_schema STRING,
# MAGIC   table_name STRING,
# MAGIC   reason STRING,
# MAGIC   updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS users.nakhoe_kim.lifecycle_exclusions_schema (
# MAGIC   table_catalog STRING,
# MAGIC   table_schema  STRING,
# MAGIC   reason        STRING,
# MAGIC   updated_at    TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# DBTITLE 1,select the list of tables in the catalog
tables_df = spark.sql(f"""
SELECT table_catalog, table_schema, table_name, table_type
FROM system.information_schema.tables
WHERE table_type = 'MANAGED'
  AND table_catalog IN ('{TARGET_CATALOGS}')
  AND table_schema NOT IN ({",".join([f"'{s}'" for s in EXCLUDE_SCHEMAS])})
   AND substr(table_name, 1, 2) <> '__'   -- tables starting with __ are excluded
""")

tables = tables_df.select("table_catalog","table_schema","table_name").collect()
print("table count:", len(tables))
display(tables_df)


# COMMAND ----------

# DBTITLE 1,schema exception
rows = [
    (None, schema, "exclude schema (bootstrap)", NOW_TS)
    for schema in EXCLUDE_SCHEMAS
]

schema = "table_catalog STRING, table_schema STRING, reason STRING, updated_at TIMESTAMP"

df = spark.createDataFrame(rows, schema=schema)

df.createOrReplaceTempView("new_schema_exclusions")

spark.sql("""
MERGE INTO users.nakhoe_kim.lifecycle_exclusions_schema tgt
USING new_schema_exclusions src
ON  tgt.table_catalog = src.table_catalog
AND tgt.table_schema  = src.table_schema
WHEN NOT MATCHED THEN
  INSERT (table_catalog, table_schema, reason, updated_at)
  VALUES (src.table_catalog, src.table_schema, src.reason, src.updated_at)
""")

display(df)

# COMMAND ----------

# DBTITLE 1,Insert the exceptions to not be deleted
# %sql
# INSERT INTO users.nakhoe_kim.lifecycle_exclusions
# (table_catalog, table_schema, table_name, reason, updated_at)
# VALUES
# ('users', 'nakhoe_kim', 'et_oz1020_dtl', 'test', current_timestamp()),
# ('users', 'nakhoe_kim', 'uc_grants_snapshot', 'test', current_timestamp()),
# ('users', 'nakhoe_kim', 'llm_detection_results', 'test', current_timestamp()),
# ('users', 'nakhoe_kim', 'rule_detection_results', 'test', current_timestamp()),
# ('users', 'nakhoe_kim', 'uc_grants_drift', 'test', current_timestamp());

# COMMAND ----------

# DBTITLE 1,DESCRIBE History function
WRITE_OPS = {"WRITE", "MERGE", "UPDATE", "DELETE", "TRUNCATE", "REPLACE TABLE AS SELECT"}

def get_last_write_from_history(fqtn: str):
    try:
        hist = spark.sql(f"DESCRIBE HISTORY {fqtn}")
        w = (
            hist
            .where(F.col("operation").isin(list(WRITE_OPS)))
            .agg(F.max("timestamp").alias("last_write_ts"))
            .collect()[0]["last_write_ts"]
        )
        if w is None:
            latest = hist.agg(F.max("timestamp").alias("ts")).collect()[0]["ts"]
            op = (
                hist.orderBy(F.col("timestamp").desc())
                    .select("operation")
                    .limit(1)
                    .collect()[0]["operation"]
            )
            return latest, f"FALLBACK({op})", "No matching write op found; used latest commit timestamp."
        
        last_op = (
            hist.where((F.col("operation").isin(list(WRITE_OPS))) & (F.col("timestamp") == F.lit(w)))
                .select("operation")
                .limit(1)
                .collect()[0]["operation"]
        )
        return w, last_op, "OK"
    except Exception as e:
        return None, None, f"FAILED: {str(e)[:500]}"


# COMMAND ----------

# DBTITLE 1,select the list of exclusion
# 제외목록
excl = spark.table(EXCLUSION_TABLE).select(
    F.concat_ws(".", "table_catalog", "table_schema", "table_name").alias("fqtn_excl")
)
excl_set = set([r["fqtn_excl"] for r in excl.collect()])
excl_set

# COMMAND ----------

# DBTITLE 1,Truncate the tables
rows = []

def truncate_table(fqtn: str):
    spark.sql(f"TRUNCATE TABLE {fqtn}")

for t in tables:
    catalog, schema, name = t["table_catalog"], t["table_schema"], t["table_name"]
    fqtn = f"`{catalog}`.`{schema}`.`{name}`"
    key = f"{catalog}.{schema}.{name}"

    if key in excl_set:
        rows.append(Row(
            run_id=RUN_ID, scanned_at=NOW_TS,
            table_catalog=catalog, table_schema=schema, table_name=name, fqtn=fqtn,
            last_write_ts=None, last_op=None,
            is_stale_90d=False,
            action_planned="SKIP", action_executed="SKIPPED",
            message="Excluded by nakhoe_kim.lifecycle_exclusions"
        ))
        continue

    last_ts, last_op, msg = get_last_write_from_history(fqtn)
    is_stale = (last_ts is not None and last_ts < STALE_CUTOFF_TS)

    planned = "DRY_RUN"
    executed = None
    exec_msg = msg

    if is_stale and RUN_MODE == "TRUNCATE":
        planned = "TRUNCATE"
        try:
            truncate_table(fqtn)
            executed = "SUCCESS"
            exec_msg = "TRUNCATED"
        except Exception as e:
            executed = "FAILED"
            exec_msg = f"TRUNCATE FAILED: {str(e)[:500]}"
    elif is_stale:
        planned = "TRUNCATE"  # it is planned to truncate (but not executed because of DRY_RUN)
        executed = "SKIPPED"
        exec_msg = "DRY_RUN (not executed)"

    rows.append(Row(
        run_id=RUN_ID, scanned_at=NOW_TS,
        table_catalog=catalog, table_schema=schema, table_name=name, fqtn=fqtn,
        last_write_ts=last_ts, last_op=last_op,
        is_stale_90d=is_stale,
        action_planned=planned, action_executed=executed,
        message=exec_msg
    ))


result_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("scanned_at", TimestampType(), False),

    StructField("table_catalog", StringType(), False),
    StructField("table_schema", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("fqtn", StringType(), False),

    StructField("last_write_ts", TimestampType(), True),
    StructField("last_op", StringType(), True),

    StructField("is_stale_90d", BooleanType(), False),

    StructField("action_planned", StringType(), False),
    StructField("action_executed", StringType(), True),

    StructField("message", StringType(), True),
])

# rows are kept as is
result_df = spark.createDataFrame(rows, schema=result_schema)

if rows:
    result_df = spark.createDataFrame(rows)
    (result_df.write.format("delta").mode("append").saveAsTable(CANDIDATE_TABLE))
    display(result_df.orderBy(F.col("is_stale_90d").desc(), F.col("last_write_ts").asc()))
else:
    print("There is no tables to delete.")
