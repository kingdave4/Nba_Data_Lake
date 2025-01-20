import logging
import boto3
import json
import os
import requests
from dotenv import load_dotenv
from watchtower import CloudWatchLogHandler

# Load environment variables
load_dotenv()

# AWS Configurations
REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
GLUE_DATABASE_NAME = "glue_nba_data_lake"
ATHENA_OUTPUT_LOCATION = f"s3://{BUCKET_NAME}/athena-results/"
NBA_ENDPOINT = os.getenv("NBA_ENDPOINT")
SPORTS_API_KEY = os.getenv("SPORTS_DATA_API_KEY")

# AWS Clients
s3_client = boto3.client("s3", region_name=REGION)
glue_client = boto3.client("glue", region_name=REGION)
athena_client = boto3.client("athena", region_name=REGION)

# Logger setup with CloudWatch
logger = logging.getLogger("NBADataPipeline")
logger.setLevel(logging.INFO)

cloudwatch_handler = CloudWatchLogHandler(
    log_group="nba-data-lake-logs",
    boto3_client=boto3.client("logs", region_name=REGION)
)
logger.addHandler(cloudwatch_handler)


def create_s3_bucket():
    """Create the S3 bucket."""
    try:
        if REGION == "us-east-1":  # create this if the region is us-east-1
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        logger.info(f"S3 bucket '{BUCKET_NAME}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating S3 bucket: {e}")


def create_glue_database():
    """Create the Glue database."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE_NAME,
                "Description": "Glue database for NBA sports analytics.",
            }
        )
        logger.info(f"Glue database '{GLUE_DATABASE_NAME}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating Glue database: {e}")


def fetch_nba_data():
    """Fetch NBA player data from the API."""
    try:
        headers = {"Ocp-Apim-Subscription-Key": SPORTS_API_KEY}
        response = requests.get(NBA_ENDPOINT, headers=headers)
        response.raise_for_status()
        logger.info("Fetched NBA data successfully.")
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching NBA data: {e}")
        return []


def upload_data_to_s3(data):
    """Upload the fetched data to S3."""
    try:
        file_key = "raw-data/nba_player_data.jsonl"
        line_delimited_data = "\n".join([json.dumps(record) for record in data])
        s3_client.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=line_delimited_data)
        logger.info(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}")


def create_glue_table():
    """Create the Glue table."""
    try:
        glue_client.create_table(
            DatabaseName=GLUE_DATABASE_NAME,
            TableInput={
                "Name": "nba_players",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "PlayerID", "Type": "int"},
                        {"Name": "FirstName", "Type": "string"},
                        {"Name": "LastName", "Type": "string"},
                        {"Name": "Team", "Type": "string"},
                        {"Name": "Position", "Type": "string"},
                        {"Name": "Points", "Type": "int"}
                    ],
                    "Location": f"s3://{BUCKET_NAME}/raw-data/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"
                    },
                },
                "TableType": "EXTERNAL_TABLE",
            },
        )
        logger.info("Glue table 'nba_players' created successfully.")
    except Exception as e:
        logger.error(f"Error creating Glue table: {e}")


def configure_athena():
    """Configure Athena output location."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": GLUE_DATABASE_NAME},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        )
        logger.info("Athena output location configured successfully.")
    except Exception as e:
        logger.error(f"Error configuring Athena: {e}")


def main():
    """Main workflow."""
    logger.info("Starting NBA Data Lake Pipeline...")
    create_s3_bucket()
    create_glue_database()
    data = fetch_nba_data()
    if data:
        upload_data_to_s3(data)
    create_glue_table()
    configure_athena()
    logger.info("NBA Data Lake Pipeline completed.")


if __name__ == "__main__":
    main()
