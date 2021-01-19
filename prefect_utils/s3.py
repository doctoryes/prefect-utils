"""
S3 related common methods and tasks for Prefect
"""

import prefect
from prefect import task
from prefect.tasks.aws import s3
from prefect.utilities.aws import get_boto_client


@task
def delete_object_from_s3(key: str = None, bucket: str = None, credentials: dict = None, ):
    """
    Delete an object from S3.

    key (str): Name of the object within the S3 bucket (/foo/bar/baz.json)
    bucket (str): Name of the S3 bucket to delete from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    s3_client = get_boto_client("s3", credentials=credentials)
    s3_client.delete_object(Bucket=bucket, Key=key)


@task
def list_object_keys_from_s3(bucket: str = None, prefix: str = '', credentials: dict = None, ):
    """
    List objects key names from an S3 bucket that match the given prefix.

    prefix (str): Prefix path to search (ex: /foo/bar will match /foo/bar/baz and /foo/bar/baz/bing ...)
    bucket (str): Name of the S3 bucket to search from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    s3_client = get_boto_client("s3", credentials=credentials)
    found_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    logger = prefect.context.get("logger")
    logger.info(found_objects)

    if found_objects['KeyCount']:
        return [
            o['Key'] for o in found_objects['Contents']
        ]
    else:
        return []


def get_s3_path_for_date(date):
    # The path and file name inside our given bucket and S3 prefix to write the file to
    return '{date}/{date}.json'.format(date=date)


@task
def write_report_to_s3(download_results: tuple, s3_bucket: str, s3_path: str):
    logger = prefect.context.get("logger")

    date, report_str = download_results
    date_path = get_s3_path_for_date(date)
    s3_key = s3_path + date_path
    logger.info(f"Writing report to S3 for {date} to {s3_key}")

    s3.S3Upload(bucket=s3_bucket).run(
        report_str,
        key=s3_key
    )

    return date_path


@task
def get_s3_url(s3_bucket, s3_path):
    return f's3://{s3_bucket}/{s3_path}'
