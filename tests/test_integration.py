import pytest
import yaml
from google.cloud import storage, bigquery
from google.api_core import exceptions
import ee
import os

# Mark this entire module as 'integration' tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def gcp_config():
    """Loads GCP configuration from the config file."""
    try:
        with open("config.yml", "r") as f:
            return yaml.safe_load(f)["gcp"]
    except FileNotFoundError:
        pytest.fail("config.yml not found. Make sure you are running pytest from the project root.")


@pytest.mark.no_mock_ee  # Opt-out of the autouse ee mock from conftest.py
def test_gcp_connectivity(gcp_config):
    """
    Performs a series of read-only checks to verify connectivity and
    permissions for GCS, BigQuery, and Earth Engine.
    """
    project = gcp_config["project"]
    bucket_name = gcp_config["bucket"]
    dataset_id = gcp_config["bq-dataset"]

    # 1. Test Google Cloud Storage Connection
    try:
        storage_client = storage.Client(project=project)
        bucket = storage_client.get_bucket(bucket_name)
        print(f"\nSuccessfully connected to GCS bucket: {bucket.name}")
        assert bucket.name == bucket_name
    except exceptions.NotFound:
        pytest.fail(f"GCS bucket '{bucket_name}' not found.")
    except exceptions.Forbidden:
        pytest.fail(f"Permission denied for GCS bucket '{bucket_name}'. Check IAM permissions.")

    # 2. Test BigQuery Connection
    try:
        client = bigquery.Client(project=project)
        tables = client.list_tables(dataset_id)  # Make an API request.
        print(f"\nSuccessfully connected to BigQuery dataset: {dataset_id}\nTables:")
        for table in tables:
            print(f"\t{table.table_id}")
    except exceptions.NotFound:
        print(f"Dataset '{project}.{dataset_id}' not found.")
        pytest.fail(f"BigQuery dataset '{dataset_id}' not found in project '{project}'.")
    except exceptions.Forbidden:
        pytest.fail(f"Permission denied for BigQuery dataset '{dataset_id}'. Check IAM permissions.")

    # 3. Test Earth Engine Initialization and a simple request
    try:
        # In the GHA environment, the auth action sets up Application Default Credentials.
        # ee.Initialize() will find them automatically.
        ee.Initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")

        # Perform a simple, low-cost operation to verify API works
        image_info = ee.Image("USGS/SRTMGL1_003").getInfo()
        assert image_info is not None, "returned image_info is None"
        print("Successfully initialized Earth Engine and fetched info.")
    except Exception as e:
        pytest.fail(f"Failed to initialize or communicate with Earth Engine: {e}")