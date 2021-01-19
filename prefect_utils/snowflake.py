"""
Utility methods and tasks for working with Snowflake from a Prefect flow.
"""
import backoff
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from prefect import task
from prefect.engine import signals
from prefect.utilities.logging import get_logger


def create_snowflake_connection(
    credentials: dict,
    role: str,
    autocommit: bool = False,
    warehouse: str = None
) -> snowflake.connector.SnowflakeConnection:
    """
    Connects to the Snowflake database.

      credentials (dict):
        Snowflake credentials including key & passphrase, along with user and account.
      role (str): Name of the role to use for the connection.
      autocommit (bool): True to enable autocommit for the connection, False if not.
      warehouse (str): The Snowflake warehouse to use for this connection. Defaults to the user's default warehouse.
    """
    private_key = credentials.get("private_key")

    private_key_passphrase = credentials.get("private_key_passphrase")
    user = credentials.get("user")
    account = credentials.get("account")

    p_key = serialization.load_pem_private_key(
        private_key.encode(),
        password=private_key_passphrase.encode(),
        backend=default_backend(),
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    connection = snowflake.connector.connect(
        user=user, account=account, private_key=pkb, autocommit=autocommit, warehouse=warehouse
    )

    # Switch to specified role.
    connection.cursor().execute(f"USE ROLE {role}")
    # Set timezone to UTC
    connection.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")

    return connection


def qualified_table_name(database, schema, table) -> str:
    """
    Fully qualified Snowflake table name.
    """
    return "{database}.{schema}.{table}".format(
        database=database, schema=schema, table=table
    )


def qualified_stage_name(database, schema, table) -> str:
    """
    Fully qualified Snowflake stage name.
    """
    return "{database}.{schema}.{table}_stage".format(
        database=database, schema=schema, table=table,
    )


@task
@backoff.on_exception(backoff.expo,
                      snowflake.connector.ProgrammingError,
                      max_tries=3)
def load_ga_data_to_snowflake(
    sf_credentials: dict,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    sf_role: str,
    sf_warehouse: str,
    sf_storage_integration: str,
    bq_dataset: str,
    gcs_url: str,
    date: str,
    pattern: str = ".*",
    overwrite: bool = False,
):
    """
    Loads JSON objects from GCS to Snowflake.
    Args:
      sf_credentials (dict):
        Snowflake public key credentials in the format required by create_snowflake_connection.
      sf_database (str): Name of the destination database.
      sf_schema (str): Name of the destination schema.
      sf_table (str): Name of the destination table.
      sf_role (str): Name of the snowflake role to assume.
      sf_warehouse (str): Name of the Snowflake warehouse to be used for loading.
      sf_storage_integration (str):
        The name of the pre-configured storage integration created for this flow.
      bq_dataset (str): BQ Dataset to which this load belongs to. This gets set as `ga_view_id' in the dest. table.
      gcs_url (str): Full URL to the GCS path containing the files to load.
      pattern (str, optional): Path pattern/regex to match GCS object to copy.
      date (str): Date of `ga_sessions` being loaded.
      overwrite (bool, optional): Whether to overwrite existing data for the given date. Defaults to `False`.
    """
    sf_connection = create_snowflake_connection(sf_credentials, sf_role)
    # Snowflake expects GCS locations to start with `gcs` instead of `gs`.
    gcs_url = gcs_url.replace("gs://", "gcs://")

    # Check for data existence for this date
    try:
        query = """
        SELECT 1 FROM {table}
        WHERE session:date='{date}'
            AND ga_view_id='{ga_view_id}'
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table),
            date=date,
            ga_view_id=bq_dataset,
        )
        cursor = sf_connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
    except snowflake.connector.ProgrammingError as e:
        if "does not exist" in e.msg:
            # If so then the query failed because the table doesn't exist.
            row = None
        else:
            raise

    if row and not overwrite:
        return

    try:
        query = """
        CREATE TABLE IF NOT EXISTS {table} (
            id number autoincrement start 1 increment 1,
            load_time timestamp_ltz default current_timestamp(),
            ga_view_id string,
            session VARIANT
        );
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table)
        )
        sf_connection.cursor().execute(query)

        if overwrite:
            query = """
            DELETE FROM {table}
            WHERE session:date='{date}'
                AND ga_view_id='{ga_view_id}'
            """.format(
                table=qualified_table_name(sf_database, sf_schema, sf_table),
                date=date,
                ga_view_id=bq_dataset,
            )
            sf_connection.cursor().execute(query)

        query = """
        CREATE OR REPLACE STAGE {stage_name}
            URL = '{stage_url}'
            STORAGE_INTEGRATION = {storage_integration}
            FILE_FORMAT = (TYPE = JSON);
        """.format(
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            stage_url=gcs_url,
            storage_integration=sf_storage_integration,
        )
        sf_connection.cursor().execute(query)

        query = """
        COPY INTO {table} (ga_view_id, session)
            FROM (
                SELECT
                    '{ga_view_id}',
                    t.$1
                FROM @{stage_name} t
            )
        PATTERN='{pattern}'
        FORCE={force}
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table),
            ga_view_id=bq_dataset,
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            pattern=pattern,
            force=str(overwrite),
        )
        sf_connection.cursor().execute(query)
        sf_connection.commit()
    except Exception:
        sf_connection.rollback()
        raise
    finally:
        sf_connection.close()


@task
def load_s3_data_to_snowflake(
    date: str,
    date_property: str,
    sf_credentials: dict,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    sf_role: str,
    sf_warehouse: str,
    sf_storage_integration_name: str,
    s3_url: str,
    sf_file_format: str = "TYPE='JSON', STRIP_OUTER_ARRAY=TRUE",
    file: str = None,
    pattern: str = None,
    overwrite: bool = False,
    truncate: bool = False,
):
    """
    Loads objects in S3 to a generic table in Snowflake, the data is stored in a variant column named
    `PROPERTIES`. Note that either `file` or `pattern` parameter must be specified.
    Notes:
      To load a single file, use the `file` parameter.
      You must explicitly include a separator (/) either at the end of the `s3_url` or at the beginning
      of file path specified in the `file` parameter. Typically you would include a trailing (/)
      in `s3_url`.
      To load multiple files, use `pattern`.
    Examples:
      To load a single file `s3://bucket/path/to/filename/filename.ext`:
      load_s3_data_to_snowflake(s3_url='s3://bucket/path/to/filename/, file='filename.ext')
      or
      load_s3_data_to_snowflake(s3_url='s3://bucket/path/, file='to/filename/filename.ext')

      Load all files under `s3://bucket/path/` with a certain name:
      load_s3_data_to_snowflake(s3_url='s3://bucket/path/, pattern='.*filename.ext')

    Args:
      date (str): Date of the data being loaded.
      date_property (str): Date type property name in the variant `PROPERTIES` column.
      sf_credentials (dict): Snowflake public key credentials in the format required by create_snowflake_connection.
      sf_database (str): Name of the destination database.
      sf_schema (str): Name of the destination schema.
      sf_table (str): Name of the destination table.
      sf_role (str): Name of the snowflake role to assume.
      sf_warehouse (str): Name of the Snowflake warehouse to be used for loading.
      sf_storage_integration_name (str): Name of the Snowflake storage integration to use. These are
              configured in Terraform.
      s3_url (str): Full URL to the S3 path containing the files to load.
      sf_file_format (str, optional): Snowflake file format for the Stage. Defaults to 'JSON'.
      file (str, optional): File path relative to `s3_url`.
      pattern (str, optional): Path pattern/regex to match S3 objects to copy. Defaults to `None`.
      overwrite (bool, optional): Whether to overwrite existing data for the given date. Defaults to `False`.
      truncate (bool, optional): Whether to truncate the table. Defaults to `False`.
    """
    logger = get_logger()
    if not file and not pattern:
        raise signals.FAIL('Either `file` or `pattern` must be specified to run this task.')

    sf_connection = create_snowflake_connection(sf_credentials, sf_role, warehouse=sf_warehouse)

    if truncate:
        query = "TRUNCATE IF EXISTS {}".format(qualified_table_name(sf_database, sf_schema, sf_table))
        logger.info(f"Truncating table: {sf_table}")
        cursor = sf_connection.cursor()
        cursor.execute(query)

    # Check for data existence for this date
    try:
        query = """
        SELECT 1 FROM {table}
        WHERE date(PROPERTIES:{date_property})=date('{date}')
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table),
            date=date,
            date_property=date_property,
        )

        logger.info(f"Checking existence of data for {date}")

        cursor = sf_connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
    except snowflake.connector.ProgrammingError as e:
        if "does not exist" in e.msg:
            # If so then the query failed because the table doesn't exist.
            row = None
        else:
            raise

    if row and not overwrite:
        raise signals.SKIP('Skipping task as data for the date exists and no overwrite was provided.')
    else:
        logger.info(f"Continuing with S3 load for {date}")

    try:
        # Create the generic loading table
        query = """
        CREATE TABLE IF NOT EXISTS {table} (
            ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            LOAD_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            ORIGIN_FILE_NAME VARCHAR(16777216),
            ORIGIN_FILE_LINE NUMBER(38,0),
            ORIGIN_STR VARCHAR(16777216),
            PROPERTIES VARIANT
        );
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table)
        )

        sf_connection.cursor().execute(query)

        # Delete existing data in case of overwrite.
        if overwrite and row:
            logger.info(f"Deleting data for overwrite for {date}")

            query = """
            DELETE FROM {table}
            WHERE date(PROPERTIES:{date_property})=date('{date}')
            """.format(
                table=qualified_table_name(sf_database, sf_schema, sf_table),
                date=date,
                date_property=date_property,
            )
            sf_connection.cursor().execute(query)

        # Create stage
        query = """
        CREATE STAGE IF NOT EXISTS {stage_name}
            URL = '{stage_url}'
            STORAGE_INTEGRATION = {storage_integration}
            FILE_FORMAT = ({file_format});
        """.format(
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            stage_url=s3_url,
            storage_integration=sf_storage_integration_name,
            file_format=sf_file_format,
        )
        sf_connection.cursor().execute(query)

        files_paramater = ""
        pattern_parameter = ""

        if file:
            logger.info(f"Loading file {file}")
            files_paramater = f"FILES = ( '{file}' )"

        if pattern:
            logger.info(f"Loading pattern {pattern}")
            pattern_parameter = f"PATTERN = '{pattern}'"

        query = """
        COPY INTO {table} (origin_file_name, origin_file_line, origin_str, properties)
            FROM (
                SELECT
                    metadata$filename,
                    metadata$file_row_number,
                    t.$1,
                    CASE
                        WHEN CHECK_JSON(t.$1) IS NULL THEN t.$1
                        ELSE NULL
                    END
                FROM @{stage_name} t
            )
        {files_parameter}
        {pattern_parameter}
        FORCE={force}
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table),
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            files_parameter=files_paramater,
            pattern_parameter=pattern_parameter,
            force=str(overwrite),
        )

        logger.info(f"Copying data into Snowflake as: \n{query}")

        sf_connection.cursor().execute(query)
        sf_connection.commit()
    except Exception:
        sf_connection.rollback()
        raise
    finally:
        sf_connection.close()
