import boto3
import json

# AWS Services
s3 = boto3.client("s3")
glue = boto3.client("glue")

# Glue Job Name
glue_job_name = "webscrape_etljob"

def lambda_handler(event, context):
    for record in event["Records"]:
        file_key = record["s3"]["object"]["key"]
        bucket_name = record["s3"]["bucket"]["name"]

        print(f"Processing file: {file_key} from bucket: {bucket_name}")

        # List all files in the raw/ folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="raw/")
        print("Available files in raw/:", [obj["Key"] for obj in response.get("Contents", [])])

        # Check if the file actually exists before copying
        if not any(obj["Key"] == file_key for obj in response.get("Contents", [])):
            print(f"ERROR: File {file_key} does not exist in S3!")
            return {"statusCode": 400, "body": f"File {file_key} not found in S3"}

        # Move File to Curated Layer
        new_key = file_key.replace("raw/", "curated/")
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": file_key},
            Key=new_key
        )
        print(f"File moved to Curated Layer: {new_key}")

        # Step 2: Trigger AWS Glue Job
        response = glue.start_job_run(
            JobName=glue_job_name,
            Arguments={"--INPUT_FILES": f"s3://{bucket_name}/{new_key}"}
        )
        print("Glue Job Triggered:", response)

        return {"statusCode": 200, "body": json.dumps("ETL Pipeline Triggered")}
