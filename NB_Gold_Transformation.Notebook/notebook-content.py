# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bca5943b-dc33-4df9-b0bd-99e6ed9036d2",
# META       "default_lakehouse_name": "lh_sustainability_gold",
# META       "default_lakehouse_workspace_id": "b5be5ed1-b7bc-4ebb-93f4-ac1364c78725",
# META       "known_lakehouses": [
# META         {
# META           "id": "bca5943b-dc33-4df9-b0bd-99e6ed9036d2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Cell 1: Global Setup, Logger Initialization & Star Schema Boundary Configurations

# CELL ********************

import logging
import sys
from pyspark.sql import SparkSession

# =============================================================================
# 1. GOLD LAYER LOGGING FRAMEWORK CONFIGURATION
# =============================================================================
def setup_pipeline_logger(name: str = "gold_pipeline") -> logging.Logger:
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
logger.info("Initializing CivicSustainIQ Gold Dimensional Transformation Pipeline Session...")

# =============================================================================
# 2. ENVIRONMENT VALIDATION & STAR SCHEMA BOUNDARY ARCHITECTURE
# =============================================================================
try:
    spark_version = spark.version
    logger.info("Spark Session verified. Engine Core Version: %s", spark_version)
except NameError as ne:
    logger.error("Global 'spark' session handle missing. Ensure this notebook runs within a Fabric Spark environment.")
    raise RuntimeError("Notebook Execution Terminated – Spark context uninitialized.") from ne

# Source Workspace Context Parameters (Explicit for Path-Based Isolation)
WORKSPACE_NAME           = "WS_CivicSustain_IQ"

# Source Lakehouse Namespace (Silver Input)
SILVER_LAKEHOUSE         = "lh_sustainability_silver"
SOURCE_SILVER_BER_TABLE  = "silver_clean_ber_search"

# Target Lakehouse Namespace (Gold Output)
GOLD_LAKEHOUSE           = "lh_sustainability_gold"
TARGET_FACT_TABLE        = "fact_sustainability_assessments"
TARGET_DIM_GEOGRAPHY     = "dim_geography"
TARGET_DIM_AGE           = "dim_construction_era"
TARGET_DIM_DEPRIVATION   = "dim_deprivation_index"
TARGET_DIM_SMALL_AREA    = "dim_small_area_mapping"

logger.info("Gold layer warehouse mapping established successfully.")
logger.info("Source Path: %s.%s", SILVER_LAKEHOUSE, SOURCE_SILVER_BER_TABLE)
logger.info("Target Star Schema Tables: [%s, %s, %s]", 
            TARGET_FACT_TABLE, TARGET_DIM_GEOGRAPHY, TARGET_DIM_AGE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 2: Metric Auditing & Path-Based Silver Extraction

# CELL ********************

# =============================================================================
# METRIC TRACKING & PATH-BASED SILVER LAYER EXTRACTION
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

try:
    # Construct the explicit, native Fabric path to the clean Silver table folder
    SILVER_TABLE_PATH = f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE}.Lakehouse/Tables/dbo/{SOURCE_SILVER_BER_TABLE}"
    
    logger.info("Extracting clean data from Silver Layer path: %s", SILVER_TABLE_PATH)
    
    # Read the clean Delta data directly into the Gold notebook memory space
    df_silver_source = (
        spark.read
        .format("delta")
        .load(SILVER_TABLE_PATH)
    )
    
    # Audit baseline row metrics
    silver_count = log_count(df_silver_source, "Silver Layer baseline source records")
    
    if silver_count == 0:
        raise ValueError("Critical Halt: Target Silver source dataset is empty.")
        
except Exception as exc:
    logger.error("Failed to read dataset from Silver repository path: %s", str(exc))
    raise RuntimeError(f"Gold Dimensional Generation aborted – Source Extract: {exc}") from exc
# =============================================================================
# 2B. DEPRIVATION INDEX DATA EXTRACTION & SANITIZATION (FIXED)
# =============================================================================
# Import the missing PySpark relational function library explicitly
from pyspark.sql import functions as F

try:
    # Construct the explicit path to the CSV file sitting in the Gold Files section
    DEPRIVATION_CSV_PATH = f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Files/deprivation_index.csv"
    
    logger.info("Extracting raw socio-economic data from Gold Files path: %s", DEPRIVATION_CSV_PATH)
    
    # Read the CSV with schema inference enabled
    df_deprivation_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(DEPRIVATION_CSV_PATH)
    )
    
    # Standardize column names to be 100% compliant with Delta Lake storage format
    sanitized_dep_cols = []
    for col_name in df_deprivation_raw.columns:
        clean_name = col_name.replace(" ", "_").replace("(", "_").replace(")", "").replace(".", "_")
        sanitized_dep_cols.append(F.col(f"`{col_name}`").alias(clean_name))
        
    df_dim_deprivation = df_deprivation_raw.select(sanitized_dep_cols)
    
    # Audit row count metrics
    dep_count = log_count(df_dim_deprivation, "Deprivation Index dimension records loaded")
    
    if dep_count == 0:
        raise ValueError("Critical Halt: Deprivation Index source file loaded with 0 rows.")
        
except Exception as exc:
    logger.error("Failed to process Deprivation Index file: %s", str(exc))
    raise RuntimeError(f"Gold Dimensional Generation aborted – Deprivation Step: {exc}") from exc

# =============================================================================
# 2C. SMALL AREA CSO MAPPING EXTRACTION & SANITIZATION
# =============================================================================
from pyspark.sql import functions as F

try:
    # Construct the explicit path to the CSV file sitting in the Gold Files section
    SMALL_AREA_CSV_PATH = f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Files/small_areas_cso.csv"
    
    logger.info("Extracting raw census geographic structures from Gold Files path: %s", SMALL_AREA_CSV_PATH)
    
    # Read the CSO mapping file with schema inference enabled
    df_small_area_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(SMALL_AREA_CSV_PATH)
    )
    
    # Standardize column names to remove spaces or invalid characters for Delta storage
    sanitized_sa_cols = []
    for col_name in df_small_area_raw.columns:
        clean_name = col_name.replace(" ", "_").replace("(", "_").replace(")", "").replace(".", "_")
        sanitized_sa_cols.append(F.col(f"`{col_name}`").alias(clean_name))
        
    df_dim_small_area = df_small_area_raw.select(sanitized_sa_cols)
    
    # Audit row count metrics
    sa_count = log_count(df_dim_small_area, "Small Area lookup dimension records loaded")
    
    if sa_count == 0:
        raise ValueError("Critical Halt: Small Area mapping file loaded with 0 rows.")
        
except Exception as exc:
    logger.error("Failed to process Small Area mapping file: %s", str(exc))
    raise RuntimeError(f"Gold Dimensional Generation aborted – Small Area Step: {exc}") from exc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 3: Dimensional Model Processing (Dim_Geography & Dim_Construction_Era)

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# 3. DIMENSIONAL TABLE GENERATION (STAR SCHEMA LOOKUPS)
# =============================================================================
try:
    logger.info("Starting dimensional table decomposition and key mapping...")

    # -------------------------------------------------------------------------
    # SUB-STEP A: Generate Dim_Geography
    # -------------------------------------------------------------------------
    logger.info("Compiling unique geospatial footprints for Dim_Geography...")
    
    # Extract unique counties and assign a deterministic surrogate key
    df_dim_geo_pre = (
        df_silver_source
        .select("CountyName")
        .distinct()
        .filter(F.col("CountyName").isNotNull())
    )
    
    # Create an ordered surrogate key using MD5 hashing for cross-workspace stability
    df_dim_geography = df_dim_geo_pre.withColumn(
        "GeographyKey", 
        F.md5(F.upper(F.trim(F.col("CountyName"))))
    ).select("GeographyKey", "CountyName")

    # -------------------------------------------------------------------------
    # SUB-STEP B: Generate Dim_Construction_Era
    # -------------------------------------------------------------------------
    logger.info("Compiling architectural timelines for Dim_Construction_Era...")
    
    # Extract distinct construction years to build our time-age bucket matrix
    df_dim_age_pre = (
        df_silver_source
        .select("Year_of_Construction")
        .distinct()
        .filter(F.col("Year_of_Construction") > 0)
    )
    
    # Segment years into historical and structural building standard categories
    df_dim_construction_era = df_dim_age_pre.withColumn(
        "AgeKey", 
        F.md5(F.col("Year_of_Construction").cast("string"))
    ).withColumn(
        "Construction_Era",
        F.when(F.col("Year_of_Construction") < 1900, "Pre-Modern (Before 1900)")
         .when((F.col("Year_of_Construction") >= 1900) & (F.col("Year_of_Construction") < 1970), "Mid-Century (1900-1969)")
         .when((F.col("Year_of_Construction") >= 1970) & (F.col("Year_of_Construction") < 2000), "Late-Century Development (1970-1999)")
         .when((F.col("Year_of_Construction") >= 2000) & (F.col("Year_of_Construction") < 2015), "Early Eco-Standards (2000-2014)")
         .otherwise("Modern High-Efficiency (2015-Present)")
    ).select("AgeKey", "Year_of_Construction", "Construction_Era")

    # -------------------------------------------------------------------------
    # AUDIT PIPELINE METRICS
    # -------------------------------------------------------------------------
    geo_count = log_count(df_dim_geography, "Total Regional Records compiled for Dim_Geography")
    age_count = log_count(df_dim_construction_era, "Total Era Records compiled for Dim_Construction_Era")
    
except Exception as exc:
    logger.error("Failed to compile dimensional lookup structures: %s", str(exc))
    raise RuntimeError(f"Gold Dimensional Generation aborted – Dimension Step: {exc}") from exc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 4: Central Fact Table Ingestion and Key Mapping

# CELL ********************

# =============================================================================
# 4. CENTRAL FACT TABLE GENERATION (COMPLETE LAYER PRESERVATION)
# =============================================================================
try:
    logger.info("Assembling central metrics matrix for Fact_Sustainability_Assessments...")

    # Link the raw silver records to our newly generated dimensions to inherit keys
    df_fact_mapped = (
        df_silver_source.alias("src")
        # Step A: Map and inherit the Geography Surrogate Key
        .join(
            df_dim_geography.alias("geo"),
            F.upper(F.trim(F.col("src.CountyName"))) == F.upper(F.trim(F.col("geo.CountyName"))),
            "left"
        )
        # Step B: Map and inherit the Construction Era Surrogate Key
        .join(
            df_dim_construction_era.alias("age"),
            F.col("src.Year_of_Construction") == F.col("age.Year_of_Construction"),
            "left"
        )
    )

    # -------------------------------------------------------------------------
    # STEP C: Project ALL Source Columns + Append Star Schema Extensions
    # -------------------------------------------------------------------------
    df_fact_sustainability = (
        df_fact_mapped
        .select(
            # 1. Grab every single column from the clean Silver source dataset dynamically
            F.col("src.*"),
            
            # 2. Append the Structural Relationship Keys for Power BI Modeling
            F.coalesce(F.col("geo.GeographyKey"), F.lit("UNKNOWN_GEO")).alias("GeographyKey"),
            F.coalesce(F.col("age.AgeKey"), F.lit("UNKNOWN_AGE")).alias("AgeKey"),
            
            # 3. Append the High-Value Calculated Metric Indicator
            F.round(
                F.when(F.col("src.GroundFloorArea_sq_m") > 0, 
                       F.col("src.BerRating") / F.col("src.GroundFloorArea_sq_m"))
                .otherwise(0.0), 
                4
            ).alias("Energy_Intensity_Per_SqM"),
            # 4. New Column: Normalized Energy Rating Tiers (Extracting root letter A, B, C, etc.)
            F.when(F.col("src.EnergyRating").rlike("(?i)A"), "A")
             .when(F.col("src.EnergyRating").rlike("(?i)B"), "B")
             .when(F.col("src.EnergyRating").rlike("(?i)C"), "C")
             .when(F.col("src.EnergyRating").rlike("(?i)D"), "D")
             .when(F.col("src.EnergyRating").rlike("(?i)E"), "E")
             .when(F.col("src.EnergyRating").rlike("(?i)F"), "F")
             .when(F.col("src.EnergyRating").rlike("(?i)G"), "G")
             .otherwise("UNKNOWN").alias("Updated_Energy_Rating")
        )
    )

    # Audit row counts to verify integrity against the inbound Silver source baseline
    fact_count = log_count(df_fact_sustainability, "Total rows successfully compiled for the Fact Table")
    
    if fact_count == 0:
        raise ValueError("Critical Halt: Constructed Fact Table layout resulted in 0 output rows.")
        
except Exception as exc:
    logger.error("Failed to compile central Star Schema Fact Table layout: %s", str(exc))
    raise RuntimeError(f"Gold Dimensional Generation aborted – Fact Step: {exc}") from exc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 5: Transactional Commit into Gold Lakehouse Star Schema Metastore

# CELL ********************

# =============================================================================
# 5. DATA PERSISTENCE ENGINE FOR GOLD DIMENSIONAL MODEL WAREHOUSE
# =============================================================================

# Controller parameter: True for monolithic fresh boot; False for continuous ingestion upserts
first_load = True  

def commit_gold_table(df, target_table: str, is_first_load: bool = True):
    """
    Persists Gold model components straight into the default attached 
    lh_sustainability_gold metastore catalog space as clean Delta asset tables.
    """
    logger.info("=== COMMITTING TRANSACTIONS TO GOLD SEMANTIC ASSET: %s ===", target_table)
    
    # -------------------------------------------------------------------------
    # STEP A: Drop Existing Tables on First Load Refresh
    # -------------------------------------------------------------------------
    try:
        if is_first_load:
            logger.warning("First-load flag enabled. Evicting table '%s' from active catalog...", target_table)
            spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    except Exception as catalog_exc:
        logger.warning("Metastore catalog sync bypassed for '%s': %s", target_table, str(catalog_exc))

    # -------------------------------------------------------------------------
    # STEP B: Commit Current Dataframe Payload to Storage
    # -------------------------------------------------------------------------
    try:
        logger.info("Writing payload to table '%s' using optimized Delta persistence...", target_table)
        (
            df.write
            .format("delta")
            .mode("overwrite" if is_first_load else "append")
            .option("overwriteSchema", "true" if is_first_load else "false")
            .saveAsTable(target_table)
        )
        logger.info("=== [SUCCESS] TRANSACTION COMMITTED FOR: %s ===", target_table)
        
        # Verify and audit written states
        log_count(spark.table(target_table), f"Active production state row audit for '{target_table}'")
        
    except Exception as exc:
        logger.error("Failed to commit transactional mutation to Gold warehouse table '%s': %s", target_table, str(exc))
        raise RuntimeError(f"Gold Persistence aborted – Metastore Commit Failure: {exc}") from exc


# =============================================================================
# STAR SCHEMA EXECUTION RUNNER
# =============================================================================

# 1. Commit Lookup Dimensions
commit_gold_table(df=df_dim_geography, target_table=TARGET_DIM_GEOGRAPHY, is_first_load=first_load)
commit_gold_table(df=df_dim_construction_era, target_table=TARGET_DIM_AGE, is_first_load=first_load)

commit_gold_table(df=df_dim_deprivation, target_table=TARGET_DIM_DEPRIVATION, is_first_load=first_load)
commit_gold_table(df=df_dim_small_area, target_table=TARGET_DIM_SMALL_AREA, is_first_load=first_load)

# 2. Commit Central Metric Fact Table
commit_gold_table(df=df_fact_sustainability, target_table=TARGET_FACT_TABLE, is_first_load=first_load)

logger.info("🚀 [PIPELINE COMPLETE] End-to-End CivicSustainIQ Lakehouse Architecture Deployed Successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Audit Cell: Programmatic Inner-Join Key Profiler

# CELL ********************

# =============================================================================
# MAXIMUM MATCH COUNT PROFILER FOR SA_CODE
# =============================================================================
from pyspark.sql import functions as F

logger.info("Evaluating maximum join intersections for `SA_CODE`...")

# Isolate the unique, clean list of SA_CODEs from your Fact table
df_fact_sa = df_fact_sustainability.select("SA_CODE").filter(F.col("SA_CODE").isNotNull())

cso_columns = ["SA_PUB2011", "SA_PUB2016", "SA_PUB2022"]

for cso_col in cso_columns:
    try:
        # Calculate the exact number of matching rows across the entire dataset
        total_matches = (
            df_fact_sa.join(
                df_dim_small_area.select(cso_col),
                F.trim(df_fact_sa["SA_CODE"]) == F.trim(df_dim_small_area[cso_col]),
                "inner"
            )
            .count()
        )
        
        logger.info("📊 Column Interaction: [SA_CODE] <--> [%s] | Total Row Matches = %s", 
                    cso_col, f"{total_matches:,}")
                    
    except Exception as e:
        logger.error("Failed to calculate matches for column %s: %s", cso_col, str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# MAXIMUM MATCH COUNT EVALUATOR FOR SA_CODE JOIN SELECTION
# =============================================================================
from pyspark.sql import functions as F

logger.info("Evaluating maximum join intersections for `SA_CODE`...")

# Isolate non-null, unique SA_CODEs from your Fact table to optimize the operation
df_fact_sa = df_fact_sustainability.select("SA_CODE").filter(F.col("SA_CODE").isNotNull())

cso_columns = ["SA_PUB2011", "SA_PUB2016", "SA_PUB2022"]
match_results = {}

print("\n--- JOIN MATCH COUNT SUMMARY ---")
for cso_col in cso_columns:
    try:
        # Calculate the absolute total of overlapping rows across the full dataset
        total_matches = (
            df_fact_sa.join(
                df_dim_small_area.select(cso_col),
                F.trim(df_fact_sa["SA_CODE"]) == F.trim(df_dim_small_area[cso_col]),
                "inner"
            )
            .count()
        )
        
        match_results[cso_col] = total_matches
        logger.info("Column Target: %s | Match Count: %s rows", cso_col, f"{total_matches:,}")
                    
    except Exception as e:
        logger.error("Failed to run profile for target column %s: %s", cso_col, str(e))

# -------------------------------------------------------------------------
# IDENTIFY AND PRINT THE HIGHEST MATCH WINNER
# -------------------------------------------------------------------------
if match_results:
    winner_column = max(match_results, key=match_results.get)
    max_value = match_results[winner_column]
    
    print("\n=================================================================")
    print(f"🥇 THE WINNER IS: {winner_column}")
    print(f"👉 Use '{winner_column}' to connect your Fact table to the Small Area Mapping table.")
    print(f"📊 Maximum structural coverage: {max_value:,} row matches achieved.")
    print("=================================================================\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
