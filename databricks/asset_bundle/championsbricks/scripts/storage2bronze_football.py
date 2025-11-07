# ======================================
# ChampionsBricks - Bronze ingestion script
# ======================================
# Reads raw data from GitHub (public repo) and loads it into Delta tables (bronze layer)
# using configuration from a YAML file.

import yaml
import pandas as pd
import requests
from io import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# ------------------------------------
# 🔧 Spark initialization
# ------------------------------------
spark = SparkSession.builder.appName("ChampionsBricks Bronze Ingestion").getOrCreate()

# ------------------------------------
# ⚙️ Load YAML configuration
# ------------------------------------
config_path = "./config.yaml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Databricks settings
catalog = config["databricks"]["catalog"]
bronze_schema = config["databricks"]["schemas"]["bronze"]

# Storage configs (dictionary of storage accounts)
storage_configs = config["storage"]

print(f"✅ Loaded config for catalog: {catalog}, bronze schema: {bronze_schema}")

# ------------------------------------
# 🧩 Process each defined source
# ------------------------------------
for src in config["sources"]:
    name = src["name"]
    storage_account = src["storage_account"]
    storage_path = src["storage_path"]
    fmt = src.get("format", "csv").lower()
    sep = src.get("sep", ",")
    target_table = src["target_table"]

    # Resolve GitHub base URL from storage section
    if storage_account not in storage_configs:
        print(f"⚠️ Storage account '{storage_account}' not found in config.")
        continue

    base_url = storage_configs[storage_account]["url_prefix"]
    # Ensure URL is well formed (avoid double slashes)
    if base_url.endswith("/") and storage_path.startswith("/"):
        full_url = base_url[:-1] + storage_path
    else:
        full_url = base_url + storage_path

    print(f"\n🚀 Ingesting source: {name}")
    print(f"   → Source URL: {full_url}")
    print(f"   → Format: {fmt} | Separator: '{sep}'")

    # ------------------------------------
    # 📥 Step 1 - Download from GitHub
    # ------------------------------------
    try:
        resp = requests.get(full_url)
        resp.raise_for_status()
        content = resp.text
    except Exception as e:
        print(f"❌ Failed to download {full_url}: {e}")
        continue

    # ------------------------------------
    # 🧮 Step 2 - Load with pandas
    # ------------------------------------
    try:
        if fmt == "csv":
            pdf = pd.read_csv(StringIO(content), sep=sep)
        elif fmt == "json":
            pdf = pd.read_json(StringIO(content))
        else:
            print(f"⚠️ Unsupported format '{fmt}' for source '{name}'. Skipping.")
            continue
    except Exception as e:
        print(f"❌ Failed to parse file from {full_url}: {e}")
        continue

    print(f"   → Loaded {len(pdf)} rows from source.")

    # ------------------------------------
    # 🧱 Step 3 - Convert to Spark DataFrame
    # ------------------------------------
    df = spark.createDataFrame(pdf)
    df = (
        df.withColumn("ingestion_ts", current_timestamp())
          .withColumn("source_url", lit(full_url))
    )

    # ------------------------------------
    # 💾 Step 4 - Write to Delta (Bronze)
    # ------------------------------------
    delta_table = f"{catalog}.{bronze_schema}.{target_table}"

    (
        df.write
        .format("delta")
        .mode("overwrite")  # switch to "append" for incremental ingestion
        .option("overwriteSchema", "true")
        .saveAsTable(delta_table)
    )

    print(f"✅ Data written successfully to Delta table: {delta_table}")

print("\n🏁 Bronze ingestion completed for all configured sources.")
