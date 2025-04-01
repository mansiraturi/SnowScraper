-- Create Database
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS webscrape_etl;
USE DATABASE webscrape_etl;

CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account-id>:role/<role-name>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mansi-etl-bucket/');

  DESC INTEGRATION s3_integration;



-- Create Table for Books Data
CREATE OR REPLACE TABLE webscrape_etl.public.books_data (
    title STRING,
    price FLOAT,
    stock STRING
);

-- Create File Format for Parquet
CREATE OR REPLACE FILE FORMAT webscrape_etl.public.parquet_format
    TYPE = PARQUET;

-- Create External Stage (Linked to S3)
CREATE OR REPLACE STAGE webscrape_etl.public.s3_stage
    URL = 's3://mansi-etl-bucket/publish/'
    CREDENTIALS = (AWS_KEY_ID='<your-access-key-id>' AWS_SECRET_KEY='<your-secret-access-key>')
    FILE_FORMAT = webscrape_etl.public.parquet_format;

LIST @s3_stage;

-- Create Snowpipe to Auto-ingest Data

  CREATE OR REPLACE PIPE my_snowpipe
    AUTO_INGEST = TRUE
    AS 
    COPY INTO books_data
    FROM @s3_stage
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';


    --to check the most recent upload by snowpipe 

    SELECT *
    FROM INFORMATION_SCHEMA.LOAD_HISTORY
