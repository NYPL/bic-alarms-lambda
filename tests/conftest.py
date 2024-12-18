import os
import pytest

from freezegun import freeze_time

# Sets OS vars for entire set of tests
TEST_ENV_VARS = {
    "ENVIRONMENT": "test",
    "REDSHIFT_DB_HOST": "test_redshift_host",
    "REDSHIFT_DB_NAME": "test_redshift_db",
    "REDSHIFT_DB_USER": "test_redshift_user",
    "REDSHIFT_DB_PASSWORD": "test_redshift_password",
    "SIERRA_DB_HOST": "test_sierra_host",
    "SIERRA_DB_PORT": "test_sierra_port",
    "SIERRA_DB_NAME": "test_sierra_db",
    "SIERRA_DB_USER": "test_sierra_user",
    "SIERRA_DB_PASSWORD": "test_sierra_password",
    "ENVISIONWARE_DB_HOST": "test_envisionware_host",
    "ENVISIONWARE_DB_PORT": "test_envisionware_port",
    "ENVISIONWARE_DB_NAME": "test_envisionware_db",
    "ENVISIONWARE_DB_USER": "test_envisionware_user",
    "ENVISIONWARE_DB_PASSWORD": "test_envisionware_password",
    "OVERDRIVE_USERNAME": "test_overdrive_username",
    "OVERDRIVE_PASSWORD": "test_overdrive_password",
    "SHOPPERTRAK_S3_BUCKET": "test_shoppertrak_s3_bucket",
    "SHOPPERTRAK_S3_RESOURCE": "test_shoppertrak_s3_resource",
}


@pytest.fixture(scope="session", autouse=True)
def tests_setup_and_teardown():
    # Will be executed before the first test
    os.environ.update(TEST_ENV_VARS)
    freezer = freeze_time("2023-06-01 01:23:45+00:00")
    freezer.start()

    yield

    # Will execute after final test
    freezer.stop()
    for os_config in TEST_ENV_VARS.keys():
        del os.environ[os_config]
