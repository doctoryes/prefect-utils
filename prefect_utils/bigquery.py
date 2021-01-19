"""
Utility methods and tasks for working with BigQuery/Google Cloud Storage from a Prefect flow.
"""

import os
from urllib.parse import urlparse

import backoff
import google.api_core.exceptions
from google.cloud import bigquery
from prefect import task
from prefect.utilities.gcp import get_bigquery_client, get_storage_client


@task
def cleanup_gcs_files(gcp_credentials: dict, url: str, project: str):
    """
    Task to delete files from a GCS prefix.

    Arguments:
      gcp_credentials (dict): GCP credentials in a format required by prefect.utilities.gcp.get_storage_client.
      url (str): Pointer to a GCS prefix containing one or more objects to delete.
      project (str): Name of the project which contains the target objects.
    """
    gcs_client = get_storage_client(credentials=gcp_credentials, project=project)
    parsed_url = urlparse(url)
    bucket = gcs_client.get_bucket(parsed_url.netloc)
    prefix = parsed_url.path.lstrip("/")
    blobs = bucket.list_blobs(prefix=prefix)
    bucket.delete_blobs(blobs)
    return blobs


@task
@backoff.on_exception(backoff.expo,
                      google.api_core.exceptions.NotFound,
                      max_time=60*60*2)
def extract_ga_table(project: str, gcp_credentials: dict, dataset: str, date: str, output_root: str):
    """
    Runs a BigQuery extraction job, extracting the google analytics' `ga_sessions` table for a
    given date to a location in GCS in gzipped compressed JSON format.
    """
    table_name = f"ga_sessions_{date}"
    dest_filename = f"{table_name}_*.json.gz"
    base_extraction_path = os.path.join(output_root, dataset, date)
    destination_uri = os.path.join(base_extraction_path, dest_filename)

    client = get_bigquery_client(credentials=gcp_credentials, project=project)

    dataset = client.dataset(dataset, project=project)
    table = dataset.table(table_name)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    job_config.compression = "GZIP"
    extract_job = client.extract_table(table, destination_uri, job_config=job_config)
    extract_job.result()

    return base_extraction_path
