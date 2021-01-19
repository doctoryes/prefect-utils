#!/usr/bin/env python

"""
Tests for Snowflake utils in the `prefect_utils` package.
"""

from unittest import mock

import pytest
from prefect.core import Flow
from prefect.engine import signals
from prefect.utilities.debug import raise_on_exception
from pytest_mock import mocker  # noqa: F401
from snowflake.connector import ProgrammingError

from prefect_utils import snowflake


def test_qualified_table_name():
    assert 'test_db.test_schema.test_table' == snowflake.qualified_table_name(
        'test_db', 'test_schema', 'test_table'
    )


def test_qualified_stage_name():
    assert 'test_db.test_schema.test_table_stage' == snowflake.qualified_stage_name(
        'test_db', 'test_schema', 'test_table'
    )


@pytest.fixture
def mock_sf_connection(mocker):  # noqa: F811
    # Mock the Snowflake connection and cursor.
    mocker.patch.object(snowflake, 'create_snowflake_connection')
    mock_cursor = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    snowflake.create_snowflake_connection.return_value = mock_connection
    return mock_connection


def test_create_snowflake_connection(mocker):  # noqa: F811
    # Mock out the snowflake connection method w/o mocking out the helper method.
    mocker.patch.object(snowflake.snowflake.connector, 'connect')
    mock_cursor = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    snowflake.snowflake.connector.connect.return_value = mock_connection
    # Mock the key decryption.
    mocker.patch.object(snowflake.serialization, 'load_pem_private_key')
    mock_key = mocker.Mock()
    mock_key.private_bytes.return_value = 1234
    snowflake.serialization.load_pem_private_key.return_value = mock_key
    # Call the connection method.
    snowflake.create_snowflake_connection(
        credentials={
            "private_key": "this_is_an_encrypted_private_key",
            "private_key_passphrase": "passphrase_for_the_private_key",
            "user": "test_user",
            "account": "company-cloud-region"
        },
        role="test_role"
    )
    snowflake.snowflake.connector.connect.assert_called_with(
        account='company-cloud-region',
        autocommit=False,
        private_key=1234,
        user='test_user',
        warehouse=None
    )
    mock_cursor.execute.assert_has_calls(
        (
            mocker.call("USE ROLE test_role"),
            mocker.call("ALTER SESSION SET TIMEZONE = 'UTC'"),
        )
    )


def test_load_json_objects_to_snowflake_no_existing_table(mock_sf_connection):
    # Mock the Snowflake connection, cursor, and fetchone method.
    mock_cursor = mock_sf_connection.cursor()
    mock_fetchone = mock.Mock(side_effect=ProgrammingError("does not exist"))
    mock_cursor.fetchone = mock_fetchone

    with Flow("test") as f:
        snowflake.load_ga_data_to_snowflake(
            sf_credentials={},
            sf_database="test_database",
            sf_schema="test_schema",
            sf_table="test_table",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_storage_integration="test_storage_integration",
            bq_dataset="test_dataset",
            gcs_url="gs://test-location",
            date="2020-01-01",
        )
    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("\n        SELECT 1 FROM test_database.test_schema.test_table\n        WHERE session:date='2020-01-01'\n            AND ga_view_id='test_dataset'\n        "), # noqa
            mock.call('\n        CREATE TABLE IF NOT EXISTS test_database.test_schema.test_table (\n            id number autoincrement start 1 increment 1,\n            load_time timestamp_ltz default current_timestamp(),\n            ga_view_id string,\n            session VARIANT\n        );\n        '), # noqa
            mock.call("\n        CREATE OR REPLACE STAGE test_database.test_schema.test_table_stage\n            URL = 'gcs://test-location'\n            STORAGE_INTEGRATION = test_storage_integration\n            FILE_FORMAT = (TYPE = JSON);\n        "), # noqa
            mock.call("\n        COPY INTO test_database.test_schema.test_table (ga_view_id, session)\n            FROM (\n                SELECT\n                    'test_dataset',\n                    t.$1\n                FROM @test_database.test_schema.test_table_stage t\n            )\n        PATTERN='.*'\n        FORCE=False\n        "), # noqa
        ]
    )


def test_load_json_objects_to_snowflake_error_on_table_exist_check(mock_sf_connection):
    # Mock the Snowflake connection, cursor, and fetchone method.
    mock_cursor = mock_sf_connection.cursor()
    mock_fetchone = mock.Mock(side_effect=ProgrammingError())
    mock_cursor.fetchone = mock_fetchone

    with Flow("test") as f:
        snowflake.load_ga_data_to_snowflake(
            sf_credentials={},
            sf_database="test_database",
            sf_schema="test_schema",
            sf_table="test_table",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_storage_integration="test_storage_integration",
            bq_dataset="test_dataset",
            gcs_url="gs://test-location",
            date="2020-01-01",
        )
    with raise_on_exception():
        with pytest.raises(ProgrammingError):
            f.run()


def test_load_json_objects_to_snowflake_overwrite(mock_sf_connection):
    # Mock the Snowflake connection, cursor, and fetchone method.
    mock_cursor = mock_sf_connection.cursor()
    mock_fetchone = mock.Mock(return_value=None)
    mock_cursor.fetchone = mock_fetchone

    with Flow("test") as f:
        snowflake.load_ga_data_to_snowflake(
            sf_credentials={},
            sf_database="test_database",
            sf_schema="test_schema",
            sf_table="test_table",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_storage_integration="test_storage_integration",
            bq_dataset="test_dataset",
            gcs_url="gs://test-location",
            date="2020-01-01",
            overwrite=True
        )
    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("\n        SELECT 1 FROM test_database.test_schema.test_table\n        WHERE session:date='2020-01-01'\n            AND ga_view_id='test_dataset'\n        "), # noqa
            mock.call('\n        CREATE TABLE IF NOT EXISTS test_database.test_schema.test_table (\n            id number autoincrement start 1 increment 1,\n            load_time timestamp_ltz default current_timestamp(),\n            ga_view_id string,\n            session VARIANT\n        );\n        '), # noqa
            mock.call("\n            DELETE FROM test_database.test_schema.test_table\n            WHERE session:date='2020-01-01'\n                AND ga_view_id='test_dataset'\n            "), # noqa
            mock.call("\n        CREATE OR REPLACE STAGE test_database.test_schema.test_table_stage\n            URL = 'gcs://test-location'\n            STORAGE_INTEGRATION = test_storage_integration\n            FILE_FORMAT = (TYPE = JSON);\n        "), # noqa
            mock.call("\n        COPY INTO test_database.test_schema.test_table (ga_view_id, session)\n            FROM (\n                SELECT\n                    'test_dataset',\n                    t.$1\n                FROM @test_database.test_schema.test_table_stage t\n            )\n        PATTERN='.*'\n        FORCE=True\n        "), # noqa
        ]
    )


def test_load_json_objects_to_snowflake_table_exists_no_overwrite(mock_sf_connection):  # noqa: F811
    # Mock the Snowflake connection, cursor, and fetchone method.
    mock_cursor = mock_sf_connection.cursor()
    mock_fetchone = mock.Mock()
    mock_cursor.fetchone = mock_fetchone

    with Flow("test") as f:
        snowflake.load_ga_data_to_snowflake(
            sf_credentials={},
            sf_database="test_database",
            sf_schema="test_schema",
            sf_table="test_table",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_storage_integration="test_storage_integration",
            bq_dataset="test_dataset",
            gcs_url="gs://test-location",
            date="2020-01-01",
        )
    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_called_once_with("\n        SELECT 1 FROM test_database.test_schema.test_table\n        WHERE session:date='2020-01-01'\n            AND ga_view_id='test_dataset'\n        ") # noqa


def test_load_json_objects_to_snowflake_table_general_exception(mock_sf_connection):
    # Mock the Snowflake connection, cursor, and fetchone method.
    mock_commit = mock.Mock(side_effect=Exception)
    mock_sf_connection.commit = mock_commit

    with Flow("test") as f:
        snowflake.load_ga_data_to_snowflake(
            sf_credentials={},
            sf_database="test_database",
            sf_schema="test_schema",
            sf_table="test_table",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_storage_integration="test_storage_integration",
            bq_dataset="test_dataset",
            gcs_url="gs://test-location",
            date="2020-01-01",
            overwrite=True
        )
    with raise_on_exception():
        with pytest.raises(Exception):
            f.run()


def test_load_s3_data_to_snowflake_missing_parameters():
    task = snowflake.load_s3_data_to_snowflake
    with pytest.raises(signals.FAIL, match="Either `file` or `pattern` must be specified to run this task."):
        task.run(
            date="2020-01-01",
            date_property='date',
            sf_credentials={},
            sf_database="test_database",
            sf_schema="test_schema",
            sf_table="test_table",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_storage_integration_name="test_storage_integration",
            s3_url="s3://edx-test/test/",
        )


def test_load_s3_data_to_snowflake_no_existing_table(mock_sf_connection):
    # Mock the Snowflake connection, cursor, and fetchone method.
    mock_cursor = mock_sf_connection.cursor()
    mock_fetchone = mock.Mock(side_effect=ProgrammingError("does not exist"))
    mock_cursor.fetchone = mock_fetchone

    task = snowflake.load_s3_data_to_snowflake
    task.run(
        date="2020-01-01",
        date_property='date',
        sf_credentials={},
        sf_database="test_database",
        sf_schema="test_schema",
        sf_table="test_table",
        sf_role="test_role",
        sf_warehouse="test_warehouse",
        sf_storage_integration_name="test_storage_integration",
        s3_url="s3://edx-test/test/",
        file="test_file.csv",
        pattern=".*",
    )
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("\n        SELECT 1 FROM test_database.test_schema.test_table\n        WHERE date(PROPERTIES:date)=date('2020-01-01')\n        "),  # noqa
            mock.call('\n        CREATE TABLE IF NOT EXISTS test_database.test_schema.test_table (\n            ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,\n            LOAD_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),\n            ORIGIN_FILE_NAME VARCHAR(16777216),\n            ORIGIN_FILE_LINE NUMBER(38,0),\n            ORIGIN_STR VARCHAR(16777216),\n            PROPERTIES VARIANT\n        );\n        '),  # noqa
            mock.call("\n        CREATE STAGE IF NOT EXISTS test_database.test_schema.test_table_stage\n            URL = 's3://edx-test/test/'\n            STORAGE_INTEGRATION = test_storage_integration\n            FILE_FORMAT = (TYPE='JSON', STRIP_OUTER_ARRAY=TRUE);\n        "),  # noqa
            mock.call("\n        COPY INTO test_database.test_schema.test_table (origin_file_name, origin_file_line, origin_str, properties)\n            FROM (\n                SELECT\n                    metadata$filename,\n                    metadata$file_row_number,\n                    t.$1,\n                    CASE\n                        WHEN CHECK_JSON(t.$1) IS NULL THEN t.$1\n                        ELSE NULL\n                    END\n                FROM @test_database.test_schema.test_table_stage t\n            )\n        FILES = ( 'test_file.csv' )\n        PATTERN = '.*'\n        FORCE=False\n        ")  # noqa
        ]
    )


def test_load_s3_data_to_snowflake_data_exists_no_overwrite(mock_sf_connection):
    mock_cursor = mock_sf_connection.cursor()
    mock_fetchone = mock.Mock()
    mock_cursor.fetchone = mock_fetchone

    task = snowflake.load_s3_data_to_snowflake
    with pytest.raises(signals.SKIP, match="Skipping task as data for the date exists and no overwrite was provided."):
        task.run(
            date="2020-01-01",
            date_property='date',
            sf_credentials={},
            sf_database="test_database",
            sf_schema="test_schema",
            sf_table="test_table",
            sf_role="test_role",
            sf_warehouse="test_warehouse",
            sf_storage_integration_name="test_storage_integration",
            s3_url="s3://edx-test/test/",
            pattern=".*",
        )
