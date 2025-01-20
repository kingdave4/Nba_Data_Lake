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
region = os.getenv("AWS_REGION", "us-east-1")  # Default to us-east-1 if not set
bucket_name = os.getenv("AWS_BUCKET_NAME")
glue_database_name = os.getenv("GLUE_DATABASE_NAME", "glue_nba_data_lake")
athena_output_location = f"s3://{bucket_name}/athena-results/"
nba_endpoint = os.getenv("NBA_ENDPOINT")
sports_api_key = os.getenv("SPORTS_DATA_API_KEY")

# Validate critical environment variables
if not bucket_name or not bucket_name.islower():
    raise ValueError("AWS_BUCKET_NAME must be defined and all lowercase.")
if not nba_endpoint:
    raise ValueError("NBA_ENDPOINT is not defined in the .env file.")
if not sports_api_key:
    raise ValueError("SPORTS_DATA_API_KEY is not defined in the .env file.")

# AWS Clients
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)

# Logger setup with CloudWatch
logger = logging.getLogger("NBADataPipeline")
logger.setLevel(logging.INFO)

cloudwatch_handler = CloudWatchLogHandler(
    log_group="nba-data-lake-logs",
    boto3_client=boto3.client("logs", region_name=region)
)
logger.addHandler(cloudwatch_handler)


def create_s3_bucket():
    """Create the S3 bucket."""
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info(f"S3 bucket '{bucket_name}' created successfully.")
    except s3_client.exceptions.BucketAlreadyExists as e:
        logger.warning(f"S3 bucket '{bucket_name}' already exists. {e}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou as e:
        logger.info(f"S3 bucket '{bucket_name}' is already owned by you. {e}")
    except Exception as e:
        logger.error(f"Error creating S3 bucket: {e}")


def create_glue_database():
    """Create the Glue database."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for NBA sports analytics.",
            }
        )
        logger.info(f"Glue database '{glue_database_name}' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"Glue database '{glue_database_name}' already exists.")
    except Exception as e:
        logger.error(f"Error creating Glue database: {e}")


def fetch_nba_data():
    """Fetch NBA player data from the API."""
    try:
        headers = {"Ocp-Apim-Subscription-Key": sports_api_key}
        response = requests.get(nba_endpoint, headers=headers)
        response.raise_for_status()
        logger.info("Fetched NBA data successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching NBA data: {e}")
        return []


def upload_data_to_s3(data):
    """Upload the fetched data to S3."""
    try:
        file_key = "raw-data/nba_player_data.jsonl"
        line_delimited_data = "\n".join([json.dumps(record) for record in data])
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=line_delimited_data)
        logger.info(f"Uploaded data to S3: {file_key}")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}")


def create_glue_table():
    """Create the Glue table."""
    try:
        glue_client.create_table(
            DatabaseName=glue_database_name,
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
                    "Location": f"s3://{bucket_name}/raw-data/",
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
    except glue_client.exceptions.AlreadyExistsException:
        logger.info("Glue table 'nba_players' already exists.")
    except Exception as e:
        logger.error(f"Error creating Glue table: {e}")


def configure_athena():
    """Configure Athena output location."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
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
