# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7dc1b5d2-685d-41c5-96ab-c933451f2994",
# META       "default_lakehouse_name": "lh_sustainability_silver",
# META       "default_lakehouse_workspace_id": "b5be5ed1-b7bc-4ebb-93f4-ac1364c78725",
# META       "known_lakehouses": [
# META         {
# META           "id": "7dc1b5d2-685d-41c5-96ab-c933451f2994"
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
# 1. SILVER LAYER LOGGING FRAMEWORK CONFIGURATION
# =============================================================================
def setup_pipeline_logger(name: str = "silver_pipeline") -> logging.Logger:
    """
    Initializes a production-grade pipeline logger routing structured stdout
    streams to monitor real-time orchestration behavior inside Microsoft Fabric.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
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
logger.info("Initializing CivicSustainIQ Silver Transformation Pipeline Session...")

# =============================================================================
# 2. ENVIRONMENT VALIDATION & BOUNDARY ARCHITECTURE
# =============================================================================
try:
    spark_version = spark.version
    logger.info("Spark Session verified. Engine Core Version: %s", spark_version)
except NameError as ne:
    logger.error("Global 'spark' session handle missing. Ensure this notebook runs within a Fabric Spark environment.")
    raise RuntimeError("Notebook Execution Terminated – Spark context uninitialized.") from ne

# Source Lakehouse Namespaces (Bronze Input)
BRONZE_LAKEHOUSE         = "lh_sustainability_bronze"
SOURCE_BRONZE_BER_TABLE  = "bronze_raw_ber_search"

# Target Lakehouse Namespaces (Silver Output)
SILVER_LAKEHOUSE         = "lh_sustainability_silver"
TARGET_SILVER_BER_TABLE  = "silver_clean_ber_search"

logger.info("Silver layer workspace mapping established successfully.")
logger.info("Source Table: %s.%s -> Target Table: %s.%s", 
            BRONZE_LAKEHOUSE, SOURCE_BRONZE_BER_TABLE, SILVER_LAKEHOUSE, TARGET_SILVER_BER_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# METRIC TRACKING & SYSTEM UTILITY FUNCTIONS
# =============================================================================

def log_count(df, label: str) -> int:
    """
    Executes an action to calculate the exact row count of a Spark DataFrame 
    and logs it in a structured format for end-to-end pipeline auditing.
    """
    try:
        row_count = df.count()
        logger.info("[ROW COUNT] %s: %s rows", label, f"{row_count:,}")
        return row_count
    except Exception as e:
        logger.error("Failed to compute row count for %s: %s", label, str(e))
        raise

def align_database_context(lakehouse_name: str):
    """
    Safely aligns the active Spark session context to the target database.
    """
    try:
        logger.info("Aligning Spark SQL session database context to: %s", lakehouse_name)
        spark.sql(f"USE {lakehouse_name}")
    except Exception as e:
        logger.warning("Could not set active database context via USE command: %s", str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 3: Extract Source Data from Bronze Layer

# CELL ********************

# =============================================================================
# 3. SOURCE LAYER DATA EXTRACTION (VERIFIED PRODUCTION PATH RESOLUTION)
# =============================================================================
try:
    WORKSPACE_NAME = "WS_CivicSustain_IQ"
    
    # FIX: Injected the physical '/dbo/' sub-folder discovered by mssparkutils
    BRONZE_TABLE_PATH = f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE}.Lakehouse/Tables/dbo/{SOURCE_BRONZE_BER_TABLE}"
    
    logger.info("Connecting to Production Workspace Context: %s", WORKSPACE_NAME)
    logger.info("Extracting data from Bronze Layer path: %s", BRONZE_TABLE_PATH)
    
    # Read the Delta format data directly from the production Bronze path
    df_bronze_raw = (
        spark.read
        .format("delta")
        .load(BRONZE_TABLE_PATH)
    )
    
    # Audit the inbound baseline rows
    bronze_count = log_count(df_bronze_raw, "Bronze Layer baseline source lines")
    
    if bronze_count == 0:
        raise ValueError("Critical Halt: Target Bronze source data table is empty.")
        
except Exception as exc:
    logger.error("Failed to read dataset from Bronze repository path: %s", str(exc))
    raise RuntimeError(f"Silver Transformation aborted – Source Extract: {exc}") from exc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 4: Silver Cleansing and Normalization Transformations

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql import DataFrame

def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Applies production cleansing rules to raw Bronze BER data:
    1. Standardizes column names by removing special characters and spaces.
    2. Enforces structural null handling on core relational keys.
    3. Trims and normalizes Irish County strings for seamless Gold joins.
    """
    logger.info("Starting Silver transformation and column normalization rules...")

    # -------------------------------------------------------------------------
    # STEP 1: Standardize Column Names (Sanitize Special Characters)
    # -------------------------------------------------------------------------
    # We replace spaces, parentheses, hyphens, and slashes with clean underscores
    sanitized_cols = []
    for col_name in df.columns:
        clean_name = (
            col_name.replace(" ", "_")
                    .replace("(", "_")
                    .replace(")", "")
                    .replace("-", "_")
                    .replace("/", "_")
        )
        sanitized_cols.append(F.col(f"`{col_name}`").alias(clean_name))
    
    df_sanitized = df.select(sanitized_cols)

    # -------------------------------------------------------------------------
    # STEP 2: Enforce Strict Structural Null Handling & Data Type Casting
    # -------------------------------------------------------------------------
    # Ensure our vital tracking keys don't contain breaking nulls or malformed text
    df_casted = (
        df_sanitized
        .withColumn("CountyName", F.coalesce(F.col("CountyName").cast(StringType()), F.lit("UNKNOWN")))
        .withColumn("Year_of_Construction", F.coalesce(F.col("Year_of_Construction").cast(IntegerType()), F.lit(-1)))
        .withColumn("GroundFloorArea_sq_m", F.coalesce(F.col("GroundFloorArea_sq_m").cast(DoubleType()), F.lit(0.0)))
        .withColumn("BerRating", F.coalesce(F.col("BerRating").cast(DoubleType()), F.lit(0.0)))
        .withColumn("CO2Rating", F.coalesce(F.col("CO2Rating").cast(DoubleType()), F.lit(0.0)))
    )

    # -------------------------------------------------------------------------
    # STEP 3: Normalize Irish County Regional Strings
    # -------------------------------------------------------------------------
    # Cleans variations in county names so they map cleanly to geospatial indexes
    df_normalized = df_casted.withColumn(
        "CountyName",
        F.trim(
            F.when(F.col("CountyName").rlike("(?i)Dublin|Laoghaire"), "Dublin")
             .when(F.col("CountyName").rlike("(?i)Tipperary"), "Tipperary")
             .when(F.col("CountyName").endswith(" City"), F.regexp_replace(F.col("CountyName"), " City", ""))
             .when(F.col("CountyName").startswith("County "), F.regexp_replace(F.col("CountyName"), "County ", ""))
             .otherwise(F.col("CountyName"))
        )
    )

    logger.info("Silver transformation rule compilation complete.")
    return df_normalized

# =============================================================================
# RUN TRANSFORMATIONS
# =============================================================================
df_silver_clean = transform_bronze_to_silver(df_bronze_raw)

# Audit row counts post-transformation to verify no records were dropped
_ = log_count(df_silver_clean, "Silver Transformation parsed output records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 5: Data Persistence into Silver Lakehouse Managed Tables

# MARKDOWN ********************


# CELL ********************

from delta.tables import DeltaTable

# =============================================================================
# CONTROLLER PARAMETER: Toggle Initial Boot vs. CDC Incremental Stream Ingestion
# =============================================================================
first_load = False  # Set to True for initial build; False for subsequent incremental runs

# =============================================================================
# PERSISTENCE ENGINE FOR SILVER MANAGED QUANTITATIVE STORAGE
# =============================================================================
def save_silver_table(
    df, 
    target_table: str = TARGET_SILVER_BER_TABLE, 
    is_first_load: bool = True
):
    """
    Persists transformed Spark DataFrame directly into the attached default Silver Lakehouse
    as a clean, optimized Delta table. Relies on the default notebook context alignment 
    to prevent double-qualification schema resolution errors.
    """
    logger.info("=== RECONCILING SILVER STORAGE TRANSACTION TARGET: %s ===", target_table)
    
    # -------------------------------------------------------------------------
    # STEP 1: Purge Session State Metadata on Full Overwrite Reboots
    # -------------------------------------------------------------------------
    try:
        if is_first_load:
            logger.warning("First-load flag active. Purging legacy schema table from catalog...")
            # FIX: Dropping table using its direct, un-nested logical name
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    except Exception as catalog_exc:
        logger.warning("Session catalog cleanup bypassed (expected execution state): %s", str(catalog_exc))

    # -------------------------------------------------------------------------
    # STEP 2: Execute Persistence Pathway Routing Logic
    # -------------------------------------------------------------------------
    try:
        if is_first_load:
            logger.info("Executing pipeline routing path: Overwrite/Initial Boot Mode")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                # FIX: Writing straight to the plain table name inside the attached catalog space
                .saveAsTable(target_table)
            )
            logger.info("=== [SUCCESS] INITIAL MONOLITHIC SILVER STORAGE DEPLOYED ===")
            
        else:
            logger.info("Executing pipeline routing path: Delta Upsert Mode with Schema Evolution")
            
            # Check if target table exists inside the catalog metastore space
            if spark.catalog.tableExists(target_table):
                logger.info("Target table found. Compiling programmatic Delta tracking map...")
                
                # Fetch a handler instance pointing to the active physical target Delta table
                target_delta_table = DeltaTable.forName(spark, target_table)
                
                # Enable schema evolution implicitly inside the merge block
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                
                # Execute Delta Upsert using clean, standardized composite key fields
                (
                    target_delta_table.alias("target")
                    .merge(
                        df.alias("source"), 
                        "target.CountyName = source.CountyName AND target.Year_of_Construction = source.Year_of_Construction"
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                logger.info("=== [SUCCESS] INCREMENTAL UPSERT MERGE COMPLETED ===")
            else:
                logger.warning("Target table '%s' not found for upsert. Defaulting to fallback generation.", target_table)
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(target_table)
                )
                logger.info("=== [SUCCESS] FALLBACK MANAGED TABLE INITIALIZED ===")

        # Log completion verification metrics out to the monitor console
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
save_silver_table(
    df=df_silver_clean, 
    target_table=TARGET_SILVER_BER_TABLE, 
    is_first_load=first_load
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
