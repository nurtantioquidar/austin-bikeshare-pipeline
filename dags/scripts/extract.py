import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, hour
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime, timedelta
import logging
import argparse
import pandas as pd
import tempfile
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_spark_session():
    try:
        spark = (
            SparkSession.builder.appName("BigQueryToGCS")
            .master("local[*]")  # Run Spark locally
            .getOrCreate()
        )
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise


def fetch_data_from_bigquery(query, max_results=None):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        )
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
        logger.info("Executing BigQuery query")
        query_job = client.query(query)

        # Fetch results
        results = query_job.result(max_results=max_results)

        # Convert to dataframe
        df = results.to_dataframe()

        logger.info(f"Fetched {len(df)} rows from BigQuery")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data from BigQuery: {str(e)}")
        raise


def upload_to_gcs(local_path, bucket_name, gcs_path):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        )
        storage_client = storage.Client(
            credentials=credentials, project=credentials.project_id
        )
        bucket = storage_client.bucket(bucket_name)

        for root, dirs, files in os.walk(local_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_path)
                blob = bucket.blob(os.path.join(gcs_path, relative_path))
                blob.upload_from_filename(local_file)
        logger.info(f"Uploaded files to gs://{bucket_name}/{gcs_path}")
    except Exception as e:
        logger.error(f"Failed to upload to GCS: {str(e)}")
        raise


def main(config):
    spark = None
    try:
        logger.info("Starting main function")
        logger.info(config)
        bucket_name = config["bucket_name"]
        days_ago = config.get("days_ago", 1)
        limit = config.get("limit", 1000)

        # Create Spark session
        spark = create_spark_session()

        # Calculate the date for the previous day
        # yesterday = datetime.utcnow() - timedelta(days=days_ago)
        # date_str = yesterday.strftime("%Y-%m-%d")
        date_str = datetime.strptime(config["date"], "%Y-%m-%d").date()
        logger.info(f"Fetching data for date: {date_str}")

        # Query to extract data
        query = f"""
        SELECT
          *,    
          DATE(start_time) AS date,
          LPAD(CAST(EXTRACT(hour FROM start_time) AS string),2,'0') AS hour
        FROM
          `bigquery-public-data.austin_bikeshare.bikeshare_trips`
        WHERE DATE(start_time) = '{date_str}'
        LIMIT {limit}
        """

        # Fetch data
        df = fetch_data_from_bigquery(query, max_results=limit)

        if df.empty:
            logger.info("No data available for the specified date.")
            return

        logger.info("Converting pandas DataFrame to Spark DataFrame")
        spark_df = spark.createDataFrame(df)

        # Save the data in Parquet format to a temporary local directory
        with tempfile.TemporaryDirectory() as tmpdir:
            local_output_path = os.path.join(tmpdir, f"bikeshare/{date_str}/")
            logger.info(f"Saving data to temporary location: {local_output_path}")
            spark_df.write.partitionBy("date", "hour").mode("overwrite").parquet(
                local_output_path
            )

            # Upload to GCS
            gcs_output_path = f"bikeshare/"
            logger.info(f"Uploading data to GCS: gs://{bucket_name}/{gcs_output_path}")
            upload_to_gcs(local_output_path, bucket_name, gcs_output_path)

        logger.info(
            f"Data extraction and storage completed. Data saved to gs://{bucket_name}/{gcs_output_path}"
        )

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.error("Exception traceback:", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BigQuery to GCS ETL job")
    parser.add_argument("--bucket_name", required=True, help="GCS bucket name")
    parser.add_argument(
        "--days_ago", type=int, default=1, help="Number of days ago to fetch data for"
    )
    parser.add_argument(
        "--limit", type=int, default=1000, help="Maximum number of rows to fetch"
    )

    args = parser.parse_args()

    config = vars(args)

    main(config)
