import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType
)


# ---------------------------------------------------------
# 1. Initialize Glue Job
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BATCH_ID"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# 2. Configuration
# ---------------------------------------------------------

batch_id = args["BATCH_ID"]

source_path = (
    f"s3://f1-data-engineering-bucket/"
    f"landing/{batch_id}/races.csv"
)

target_path = (
    f"s3://f1-data-engineering-bucket/"
    f"processed/{batch_id}/races/"
)


# ---------------------------------------------------------
# 3. Define Source Schema
# ---------------------------------------------------------

races_schema = StructType([
    StructField("season", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("date", DateType(), True),
    StructField("circuitId", StringType(), True)
])


# ---------------------------------------------------------
# 4. Read races.csv
# ---------------------------------------------------------

races_df = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(races_schema)
        .load(source_path)
)


# ---------------------------------------------------------
# 5. Add Ingestion Metadata
# ---------------------------------------------------------

races_final_df = (
    races_df
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "source_file",
            F.input_file_name()
        )
        .withColumn(
            "batch_id",
            F.lit(batch_id)
        )
)


# ---------------------------------------------------------
# 6. Write Processed Data to S3
# ---------------------------------------------------------

(
    races_final_df
        .write
        .mode("overwrite")
        .option("header", "true")
        .parquet(target_path)
)


# ---------------------------------------------------------
# 7. Commit Glue Job
# ---------------------------------------------------------

job.commit()