from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

class GcpSettings(BaseSettings):
    # Pydantic BaseSettings automatically maps field names to environment variables
    # (e.g., 'project' -> PROJECT, 'bq_dataset' -> BQ_DATASET).
    # We can use Field(alias=...) for 'bq-dataset' to map it to BQ_DATASET env var.
    # Or, more simply, just use the exact env var names as field names.
    # Let's use explicit env var names for clarity.
    project: str = Field(..., env='GCP_PROJECT')
    bq_dataset: str = Field(..., env='GCP_BQ_DATASET')
    bucket: str = Field(..., env='GCP_BUCKET')

class AppSettings(BaseSettings):
    gcp: GcpSettings

    model_config = SettingsConfigDict(
        env_file='.env', # Load environment variables from .env file for local development
        env_file_encoding='utf-8',
        extra='ignore' # Ignore extra fields in .env or environment
    )

@lru_cache()
def get_settings() -> AppSettings:
    """Loads and validates application settings from environment variables."""
    return AppSettings()