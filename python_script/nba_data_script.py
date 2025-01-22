import boto3
import json
import time
import requests
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# AWS configurations
region = "us-east-1"  # Replace with your preferred AWS region
bucket_name = "sports-analytics-data-lake-2144"  # Change to a unique S3 bucket name
glue_database_name = "glue_nba_data_lake"
athena_output_location = f"s3://{bucket_name}/athena-results/"

# Sportsdata.io configurations (loaded from .env)
api_key = os.getenv("SPORTS_DATA_API_KEY")  # Get API key from .env
nba_endpoint = os.getenv("NBA_ENDPOINT")  # Get NBA endpoint from .env

# Create AWS clients
s3_client = boto3.client("s3", region_name=region)
glue_client = boto3.client("glue", region_name=region)
athena_client = boto3.client("athena", region_name=region)
logs_client = boto3.client("logs", region_name=region)

# CloudWatch log group configurations
log_group_name = "NBAAnalyticsLogGroup"

def initialize_cloudwatch_logging():
    """Set up CloudWatch log group."""
    try:
        # Create log group if it doesn't exist
        logs_client.create_log_group(logGroupName=log_group_name)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass  # Log group already exists

def log_to_cloudwatch(message):
    """Log a message to CloudWatch."""
    try:
        timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds
        logs_client.put_log_events(
            logGroupName=log_group_name,
            logEvents=[{"timestamp": timestamp, "message": message}],
        )
    except Exception as e:
        print(f"Error logging to CloudWatch: {e}")

def create_s3_bucket():
    """Create an S3 bucket for storing sports data."""
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        message = f"S3 bucket '{bucket_name}' created successfully."
        print(message)
        log_to_cloudwatch(message)
    except Exception as e:
        message = f"Error creating S3 bucket: {e}"
        print(message)
        log_to_cloudwatch(message)

def create_glue_database():
    """Create a Glue database for the data lake."""
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "Glue database for NBA sports analytics.",
            }
        )
        message = f"Glue database '{glue_database_name}' created successfully."
        print(message)
        log_to_cloudwatch(message)
    except Exception as e:
        message = f"Error creating Glue database: {e}"
        print(message)
        log_to_cloudwatch(message)

def fetch_nba_data():
    """Fetch NBA player data from sportsdata.io."""
    try:
        headers = {"Ocp-Apim-Subscription-Key": api_key}
        response = requests.get(nba_endpoint, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
        message = "Fetched NBA data successfully."
        print(message)
        log_to_cloudwatch(message)
        return response.json()  # Return JSON response
    except Exception as e:
        message = f"Error fetching NBA data: {e}"
        print(message)
        log_to_cloudwatch(message)
        return []

def convert_to_line_delimited_json(data):
    """Convert data to line-delimited JSON format."""
    message = "Converting data to line-delimited JSON format..."
    print(message)
    log_to_cloudwatch(message)
    return "\n".join([json.dumps(record) for record in data])

def upload_data_to_s3(data):
    """Upload NBA data to the S3 bucket."""
    try:
        # Convert data to line-delimited JSON
        line_delimited_data = convert_to_line_delimited_json(data)

        # Define S3 object key
        file_key = "raw-data/nba_player_data.jsonl"

        # Upload JSON data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=line_delimited_data
        )
        message = f"Uploaded data to S3: {file_key}"
        print(message)
        log_to_cloudwatch(message)
    except Exception as e:
        message = f"Error uploading data to S3: {e}"
        print(message)
        log_to_cloudwatch(message)

def create_glue_table():
    """Create a Glue table for the data."""
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
        message = "Glue table 'nba_players' created successfully."
        print(message)
        log_to_cloudwatch(message)
    except Exception as e:
        message = f"Error creating Glue table: {e}"
        print(message)
        log_to_cloudwatch(message)

def configure_athena():
    """Set up Athena output location."""
    try:
        athena_client.start_query_execution(
            QueryString="CREATE DATABASE IF NOT EXISTS nba_analytics",
            QueryExecutionContext={"Database": glue_database_name},
            ResultConfiguration={"OutputLocation": athena_output_location},
        )
        message = "Athena output location configured successfully."
        print(message)
        log_to_cloudwatch(message)
    except Exception as e:
        message = f"Error configuring Athena: {e}"
        print(message)
        log_to_cloudwatch(message)

# Main workflow
def main():
    print("Setting up data lake for NBA sports analytics...")
    initialize_cloudwatch_logging()
    create_s3_bucket()
    time.sleep(5)  # Ensure bucket creation propagates
    create_glue_database()
    nba_data = fetch_nba_data()
    if nba_data:  # Only proceed if data was fetched successfully
        upload_data_to_s3(nba_data)
    create_glue_table()
    configure_athena()
    print("Data lake setup complete.")

if __name__ == "__main__":
    main()
