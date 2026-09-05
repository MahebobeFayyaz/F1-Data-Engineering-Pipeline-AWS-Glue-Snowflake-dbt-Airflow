import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
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
    f"landing/{batch_id}/constructors.json"
)

target_path = (
    f"s3://f1-data-engineering-bucket/"
    f"processed/{batch_id}/constructors/"
)


# ---------------------------------------------------------
# 3. Define Source Schema
# ---------------------------------------------------------

constructors_schema = StructType([
    StructField("constructorId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])


# ---------------------------------------------------------
# 4. Read constructors.json
# ---------------------------------------------------------

constructors_df = (
    spark.read
        .format("json")
        .option("mode", "FAILFAST")
        .schema(constructors_schema)
        .load(source_path)
)


# ---------------------------------------------------------
# 5. Add Ingestion Metadata
# ---------------------------------------------------------

constructors_final_df = (
    constructors_df
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
    constructors_final_df
        .write
        .mode("overwrite")
        .option("header", "true")
        .parquet(target_path)
)


# ---------------------------------------------------------
# 7. Commit Glue Job
# ---------------------------------------------------------

job.commit()