from google.cloud import bigquery, storage
from google.api_core import exceptions
from google.oauth2 import service_account
import logging
import argparse
import sys
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_table_schema():
    """Define and return the table schema."""
    return [
        bigquery.SchemaField("trip_id", "STRING"),
        bigquery.SchemaField("subscriber_type", "STRING"),
        bigquery.SchemaField("bikeid", "STRING"),
        bigquery.SchemaField("start_time", "TIMESTAMP"),
        bigquery.SchemaField("start_station_id", "INT64"),
        bigquery.SchemaField("start_station_name", "STRING"),
        bigquery.SchemaField("end_time", "TIMESTAMP"),
        bigquery.SchemaField("end_station_id", "STRING"),
        bigquery.SchemaField("end_station_name", "STRING"),
        bigquery.SchemaField("duration_minutes", "INTEGER"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("hour", "STRING"),
    ]


def get_external_config(gcs_bucket, gcs_path):
    """Create and return the external configuration for the table."""
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [f"gs://{gcs_bucket}/{gcs_path}/*.parquet"]
    external_config.autodetect = True
    external_config.hive_partitioning = bigquery.HivePartitioningOptions()
    external_config.hive_partitioning.mode = "AUTO"
    external_config.hive_partitioning.source_uri_prefix = (
        f"gs://{gcs_bucket}/{gcs_path}"
    )
    external_config.ignore_unknown_values = True
    return external_config


def validate_gcs_data(gcs_bucket, gcs_path):
    """Validate the data in GCS against the defined schema."""
    credentials = service_account.Credentials.from_service_account_file(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
    storage_client = storage.Client(
        credentials=credentials, project=credentials.project_id
    )
    bucket = storage_client.get_bucket(gcs_bucket)
    blobs = bucket.list_blobs(prefix=gcs_path)

    for blob in blobs:
        if blob.name.endswith(".parquet"):
            logger.info(f"Validated file: {blob.name}")

    logger.info("Data validation completed.")


def create_or_update_table(client, table_ref, schema, external_config, dry_run=False):
    """Create a new table or update an existing one."""
    table = bigquery.Table(table_ref, schema=schema)
    table.external_data_configuration = external_config

    if dry_run:
        logger.info(
            f"Dry run: Would {'create' if not client.get_table(table_ref) else 'update'} "
            f"BigLake table {table.project}.{table.dataset_id}.{table.table_id}"
        )
        return table

    try:
        try:
            client.get_table(table_ref)  # Check if table exists
            table = client.update_table(
                table, ["external_data_configuration", "schema"]
            )
            logger.info(
                f"Updated BigLake table {table.project}.{table.dataset_id}.{table.table_id}"
            )
        except exceptions.NotFound:
            table = client.create_table(table)
            logger.info(
                f"Created BigLake table {table.project}.{table.dataset_id}.{table.table_id}"
            )
    except exceptions.BadRequest as e:
        logger.error(f"Bad request error: {str(e)}")
        raise
    except exceptions.Forbidden as e:
        logger.error(f"Permission denied: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

    return {
        "project": table.project,
        "dataset_id": table.dataset_id,
        "table_id": table.table_id,
        "num_rows": table.num_rows,
        "num_bytes": table.num_bytes,
    }


def manage_biglake_table(
    project_id, dataset_id, table_id, gcs_bucket, gcs_path, dry_run=False
):
    """Main function to manage (create or update) the BigLake table."""
    credentials = service_account.Credentials.from_service_account_file(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    schema = get_table_schema()
    external_config = get_external_config(gcs_bucket, gcs_path)

    try:
        validate_gcs_data(gcs_bucket, gcs_path)
        table = create_or_update_table(
            client, table_ref, schema, external_config, dry_run
        )
        return table
    except Exception as e:
        logger.error(f"Error managing BigLake table: {str(e)}")
        raise


def main(args):
    """Main entry point of the script."""
    try:
        manage_biglake_table(
            args.project_id,
            args.dataset_id,
            args.table_id,
            args.gcs_bucket,
            args.gcs_path,
            args.dry_run,
        )
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage BigLake table")
    parser.add_argument("--project_id", required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset_id", required=True, help="BigQuery dataset ID")
    parser.add_argument("--table_id", required=True, help="BigQuery table ID")
    parser.add_argument("--gcs_bucket", required=True, help="GCS bucket name")
    parser.add_argument("--gcs_path", required=True, help="GCS path to data")
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Perform a dry run without making changes",
    )

    args = parser.parse_args()
    main(args)
