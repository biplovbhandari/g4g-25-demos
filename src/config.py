import yaml
from pydantic import BaseModel, Field
from functools import lru_cache

class GcpSettings(BaseModel):
    project: str
    bq_dataset: str = Field(..., alias='bq-dataset')
    bucket: str

class AppSettings(BaseModel):
    gcp: GcpSettings

@lru_cache()
def get_settings() -> AppSettings:
    """Loads and validates application settings from config.yml."""
    try:
        with open("config.yml", "r") as f:
            config_data = yaml.safe_load(f)
            return AppSettings(**config_data)
    except (FileNotFoundError, yaml.YAMLError) as e:
        raise RuntimeError(f"Fatal: Could not load or parse config.yml. Error: {e}")