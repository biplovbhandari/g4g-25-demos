import os
import yaml
from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends, Query
from pydantic import BaseModel, Field
import ee

from src.prep import prep_tables
from src.search import search_result
from src.config import get_settings, AppSettings

# --- Configuration and Initialization ---

app = FastAPI(
    title="GCP Similarity Search API",
    description="API for preparing data and running vector similarity searches.",
    version="1.0.0"
)

# --- Pydantic Models for Request Bodies ---

class PrepRequest(BaseModel):
    gcp_file: str = Field(..., example="gs://your-bucket/your-file.geojson", description="GCS path to the GeoJSON plot file.")
    years: list[int] = Field(..., example=[2020, 2021], description="List of years to process.")

class SearchResponse(BaseModel):
    target_plotid: int
    base_plotid: int
    distance: float

# --- API Endpoints ---

@app.on_event("startup")
async def startup_event():
    """Handles Earth Engine authentication on application startup."""
    try:
        # Load settings at startup to fail fast if config is missing/invalid
        settings = get_settings()
        # In a GCP environment (Cloud Run, GHA), ee.Initialize() will automatically
        # find the credentials. For local dev, GOOGLE_APPLICATION_CREDENTIALS must be set.
        ee.Initialize(
            # credentials=None lets the library find them automatically.
            credentials=None,
            project=settings.gcp.project,
            opt_url="https://earthengine-highvolume.googleapis.com"
        )
        ee.data.setWorkloadTag("gcp-sim-search-api")
        print("Earth Engine initialized successfully.")
    except Exception as e:
        # Log the error but allow the app to start, as the /search endpoint may still work.
        print(f"FATAL: Could not initialize Earth Engine. The /prep endpoint will fail. Error: {e}")

@app.post("/prep", status_code=202)
async def create_prep_job(
    request: PrepRequest,
    background_tasks: BackgroundTasks,
    settings: AppSettings = Depends(get_settings)
):
    """
    Accepts a data preparation job and runs it in the background.
    """
    print(f"Received prep job for {request.gcp_file} for years {request.years}")
    background_tasks.add_task(
        prep_tables, request.gcp_file, settings.gcp.project, settings.gcp.bq_dataset, request.years
    )
    return {"message": "Data preparation job accepted and running in the background."}

@app.get("/search", response_model=list[SearchResponse])
async def run_search(
    uniqueid: int = Query(..., description="Unique ID of the plot to search for.", example=5),
    table: str = Query(..., description="The BigQuery table to search within.", example="my_processed_table_pp"),
    matches: int = Query(5, ge=1, le=50, description="Number of matches to return."),
    settings: AppSettings = Depends(get_settings)
):
    """
    Performs a vector similarity search on a prepared BigQuery table.
    """
    try:
        results_df = search_result(uniqueid, matches, settings.gcp.project, settings.gcp.bq_dataset, table)
        return results_df.to_dict(orient="records")
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")