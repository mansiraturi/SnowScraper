import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

# Set up Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get input files from Lambda
args = getResolvedOptions(sys.argv, ["INPUT_FILES"])
file_paths = args["INPUT_FILES"].split(",")

# Read CSV from S3
df = spark.read.option("header", "true").csv(file_paths)

# Transform Data: Convert price to float
df_transformed = df.withColumn("price", regexp_replace(col("price"), "£", "").cast("double"))

# Write Transformed Data to S3 (Publish Layer)
output_path = "s3://mansi-etl-bucket/publish/"
df_transformed.write.mode("overwrite").parquet(output_path)

print(f"Transformed data written to {output_path}")
