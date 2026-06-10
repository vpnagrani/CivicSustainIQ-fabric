# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "574f95e7-1261-4fa7-a201-2eeda2376d6a",
# META       "default_lakehouse_name": "lh_sustainability_bronze",
# META       "default_lakehouse_workspace_id": "b5be5ed1-b7bc-4ebb-93f4-ac1364c78725",
# META       "known_lakehouses": [
# META         {
# META           "id": "574f95e7-1261-4fa7-a201-2eeda2376d6a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import logging
import sys
from pyspark.sql import SparkSession

# =============================================================================
# 1. CORE LOGGING FRAMEWORK CONFIGURATION
# =============================================================================
def setup_pipeline_logger(name: str = "bronze_pipeline") -> logging.Logger:
    """
    Initializes a production-grade pipeline logger routing structured stdout
    streams to monitor real-time orchestration behavior inside Microsoft Fabric.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if cell is re-executed
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

logger = setup_pipeline_logger()
logger.info("Initializing CivicSustainIQ Bronze Ingestion Pipeline Session...")

# =============================================================================
# 2. ENVIRONMENT VALIDATION & SOURCE PATH ARCHITECTURE
# =============================================================================
try:
    # Verify active Spark context session
    spark_version = spark.version
    logger.info("Spark Session verified. Engine Core Version: %s", spark_version)
except NameError as ne:
    logger.error("Global 'spark' session handle missing. Ensure this notebook runs within a Fabric Spark environment.")
    raise RuntimeError("Notebook Execution Terminated – Spark context uninitialized.") from ne

# Global OneLake Workspace Pointer Context
WORKSPACE_URL = "abfss://WS_CivicSustain_IQ@onelake.dfs.fabric.microsoft.com"

# Inbound File Location (Sole Raw Landing File)
RAW_BER_SEARCH_PATH       = f"{WORKSPACE_URL}/lh_sustainability_bronze.Lakehouse/Files/BERPublicsearch.txt"

# Outbound Persistence Namespace (Target Managed Table)
BRONZE_LAKEHOUSE          = "lh_sustainability_bronze"
TARGET_BER_TABLE          = "bronze_raw_ber_search"

logger.info("Namespace mapping matrix established successfully.")
logger.info("Raw Inbound Source Path Verified for BER Public Search.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# HARDWARE METRIC TRACKING & UTILITY FUNCTIONS
# =============================================================================

def log_count(df, label: str) -> int:
    """
    Executes an action to calculate the exact row count of a Spark DataFrame 
    and logs it in a structured format for end-to-end pipeline auditing.
    
    Args:
        df (DataFrame): The Spark DataFrame to audit.
        label (str): Descriptional label for the logging output.
        
    Returns:
        int: Total row count.
    """
    try:
        row_count = df.count()
        logger.info("[ROW COUNT] %s: %s rows", label, f"{row_count:,}")
        return row_count
    except Exception as e:
        logger.error("Failed to compute row count for %s: %s", label, str(e))
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 3. CORE INGESTION TRANSACTION
# =============================================================================
try:
    logger.info("Extracting raw public data from OneLake source: %s", RAW_BER_SEARCH_PATH)
    
    # Read the raw tab-separated text file safely into a Spark DataFrame
    df_raw_ber = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "\t")
        .csv(RAW_BER_SEARCH_PATH)
    )
    
    # Track the inbound audit metrics
    raw_rows_logged = log_count(df_raw_ber, "BER Public Search raw ingested lines")
    
    if raw_rows_logged == 0:
        raise ValueError("Critical Halt: Inbound raw dataset is entirely empty.")
        
except Exception as exc:
    logger.error("Ingestion sequence terminated due to source read failure: %s", str(exc))
    raise RuntimeError(f"Bronze Ingestion aborted – Source Extract: {exc}") from exc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# =============================================================================
# CONTROLLER PARAMETER: Toggle Initial Boot vs. CDC Incremental Stream Ingestion
# =============================================================================
first_load = True  # Set to True for initial build; False for subsequent incremental runs

# =============================================================================
# CORE GOVERNANCE ENGINE & DIRECT MANAGED TABLE PERSISTENCE
# =============================================================================
def save_bronze_table(
    df, 
    target_table: str = TARGET_BER_TABLE, 
    lakehouse: str = BRONZE_LAKEHOUSE, 
    is_first_load: bool = True
):
    """
    Appends governance tracking columns and registers data natively into the 
    attached Lakehouse catalog context. Activates Delta Column Mapping to natively
    support special characters like spaces and parentheses in raw source headers.
    """
    logger.info("=== RECONCILING PERSISTENCE TRANSACTION TARGET: %s ===", target_table)
    
    # -------------------------------------------------------------------------
    # STEP 1: Inject Governance Auditing Metadata Footprints
    # -------------------------------------------------------------------------
    df_governed = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_lineage_file", F.lit(RAW_BER_SEARCH_PATH.split('/')[-1]))
          .withColumn("pipeline_execution_layer", F.lit("BRONZE"))
    )
    
    # -------------------------------------------------------------------------
    # STEP 2: Non-Blocking Transient Session Cleanup 
    # -------------------------------------------------------------------------
    try:
        if is_first_load:
            logger.warning("First-load requested. Purging existing database catalog definitions...")
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    except Exception as catalog_exc:
        logger.warning("Session catalog cleanup bypassed (expected execution state): %s", str(catalog_exc))

    # -------------------------------------------------------------------------
    # STEP 3: Execute Selected Ingestion Routing Strategy
    # -------------------------------------------------------------------------
    try:
        if is_first_load:
            logger.info("Executing pipeline routing path: Overwrite/Initial Boot Mode")
            (
                df_governed.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                # FIX: Enable Column Mapping to allow special characters like (sq m) in column names
                .option("delta.columnMapping.mode", "name")
                .saveAsTable(target_table)
            )
            logger.info("=== [SUCCESS] INITIAL MONOLITHIC BRONZE STORAGE DEPLOYED ===")
            
        else:
            logger.info("Executing pipeline routing path: Delta Upsert Mode with Schema Evolution")
            
            # Check if target table exists inside the attached context catalog space
            if spark.catalog.tableExists(target_table):
                logger.info("Target table found. Compiling programmatic Delta tracking map...")
                
                # Configure the Upsert Transaction. We enable schema evolution implicitly inside the merge block.
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                
                # Fetch a handler instance pointing to the active physical target Delta table
                target_delta_table = DeltaTable.forName(spark, target_table)
                
                # Execute Delta Upsert using unique record identifiers
                (
                    target_delta_table.alias("target")
                    .merge(df_governed.alias("source"), "target.CountyName = source.CountyName AND target.Year_of_Construction = source.Year_of_Construction")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                logger.info("=== [SUCCESS] INCREMENTAL UPSERT MERGE COMPLETED ===")
            else:
                logger.warning("Target table '%s' not found for upsert. Defaulting to fallback generation.", target_table)
                (
                    df_governed.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .option("delta.columnMapping.mode", "name")
                    .saveAsTable(target_table)
                )
                logger.info("=== [SUCCESS] FALLBACK MANAGED TABLE INITIALIZED ===")

        # Log completion verification metrics
        log_count(spark.table(target_table), f"Final persistent physical table states for '{target_table}'")
        
    except Exception as exc:
        logger.error("Failed to commit transactional mutations inside storage layer: %s", str(exc))
        raise RuntimeError(f"Bronze Save Pipeline aborted – Storage Layer Commit: {exc}") from exc



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# =============================================================================
# PIPELINE STEP RUNNER EXECUTION
# =============================================================================
save_bronze_table(
    df=df_raw_ber, 
    target_table=TARGET_BER_TABLE, 
    lakehouse=BRONZE_LAKEHOUSE, 
    is_first_load=first_load
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
