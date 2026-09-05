import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType
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
    f"landing/{batch_id}/sprints/"
)

target_path = (
    f"s3://f1-data-engineering-bucket/"
    f"processed/{batch_id}/sprints/"
)


# ---------------------------------------------------------
# 3. Define Source Schema
# ---------------------------------------------------------

sprints_schema = StructType([
    StructField("date", StringType(), True),
    StructField("raceName", StringType(), True),
    StructField("round", IntegerType(), True),
    StructField("season", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("constructorId", StringType(), True),
    StructField("driverId", StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("status", StringType(), True)
])


# ---------------------------------------------------------
# 4. Read all multiline JSON files
# ---------------------------------------------------------

sprints_df = (
    spark.read
        .format("json")
        .schema(sprints_schema)
        .option("mode", "FAILFAST")
        .option("multiLine", "true")
        .load(source_path)
)


# ---------------------------------------------------------
# 5. Add Ingestion Metadata
# ---------------------------------------------------------

sprints_final_df = (
    sprints_df
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
    sprints_final_df
        .write
        .mode("overwrite")
        .parquet(target_path)
)


# ---------------------------------------------------------
# 7. Commit Glue Job
# ---------------------------------------------------------

job.commit()