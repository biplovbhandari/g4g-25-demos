import os
import ee
import geemap
import geopandas as gpd
import pandas as pd
from google.cloud.bigquery import Client
from google.cloud import bigquery
import time
import random

ee.Initialize()
def poll_submitted_task(task,sleeper:int|float):
    """
    polls for the status of one started task, completes when task status is 'COMPLETED'
    args:
        task : task status dictionary returned by ee.batch.Task.status() method
        sleeper (int): minutes to sleep between status checks
    returns:
        None
    """
    # handles instances of ee.batch.Task, 
    # NOTE: task needs to be started in order to retrieve needed status info, 
    # so use this function after doing `task.start()`
    if isinstance(task,ee.batch.Task):
        t_id = task.status()['id']
        status = task.status()['state']
        
        if status == 'UNSUBMITTED':
            raise RuntimeError(f"run .start() method on task before polling. {t_id}:{status}")
        
        print(f"polling for task: {t_id}")
        while status != 'COMPLETED':
            if status in ['READY','RUNNING']:
                print(f"{t_id}:{status} [sleeping {sleeper} mins] ")
                time.sleep(60*sleeper)
                t_id = task.status()['id']
                status = task.status()['state'] 
            elif status in ['FAILED','CANCELLED','CANCEL_REQUESTED']:
                raise RuntimeError(f"problematic task status code - {t_id}:{status}")
        print(f"{t_id}:{status}")
    else:
        raise TypeError(f"{task} is not instance of <ee.batch.Task>. {type(task)}")
    return

# these two functions handle tasks as generated from 
# earthengine CLI (e.g. earthengine task list)
# good for polling for tasks outside scope of their generation
def ee_task_list_complete(desc:str,items:int):
    """
    Tests if particular type of task(s) have COMPLETED on EE server
    args:
        desc (str): description of task type in 2nd column of earthengine task list output
        items (int): number of task items to check on (descending by date submitted)
    returns:
        list (e.g. [True,True,False])
    """
    # parse earthengine task list output into python list
    ee_task_list = os.popen(f"earthengine task list").read().split('\n')
    upload_tasks = [t for t in ee_task_list if desc in t][0:items]
    # test each item's status reads COMPLETE
    test_complete = ['COMPLETED' in t for t in upload_tasks]
    return test_complete

def ee_task_list_poller(desc:str,items:int,sleep:int):
    """
    Polls for tasks running on EE server, fetched by earthengine task list CLI command
    args:
        desc (str): one of Upload,Export.image, others?..
        items (int): number of upload tasks to poll for
        sleep (int): number of minutes to sleep in between checks
    returns:
        None
    """
    #parses earthengine task list output
    test_complete = ee_task_list_complete(desc,items)
    while not all(test_complete):
        print(f"Waiting for one or more {desc} tasks to complete: {test_complete}. Sleeping {sleep} mins..")
        time.sleep(60*sleep)
        # Could fetch Upload, Export.image, other types of tasks 
        # as presented in 2nd column of earthengine task list output
        test_complete = ee_task_list_complete(desc,items)
    
    print(f'all {desc} complete')
    return

def has_common_elements_all(list_a, list_b):
  """
  Returns True if there items in list_a are contained in list_b.
  """
  # For best performance, create a set from the shorter list.
  set_b = set(list_b)
  return all(item in set_b for item in list_a)

def plot_to_fc(file:str):
        plots = gpd.read_file(file)
        columns = plots.columns
        schema = ['plotid','center_lon','center_lat','size_m']
        has_all_columns = has_common_elements_all(schema,columns)
        if not has_all_columns:
                raise ValueError(f"{file} does not contain required schema columns: {schema}")
        plots = plots[schema]
        size_m = int(str(plots.loc[0,'size_m']).split('.')[0])
        fc = geemap.df_to_ee(plots,latitude="center_lat",longitude="center_lon")
        return fc.set('plot_size',size_m)

def efm_plot_agg(fc:ee.FeatureCollection,
                 years:list[int],
                 ) -> list[ee.FeatureCollection]:
        """
        Computes band-wise means of Google EFM imageCollection to featureCollection regions for each year provided in `years`.

        Args:
            fc (ee.FeatureCollection): Earth Engine FeatureCollection to aggregate EFM to
            years (list[int]): list of years of EFM to aggregate to

        Returns:
            list[ee.FeatureCollection]: one FeatureCollection result per year in `years`
        """
        efm = ee.ImageCollection("GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL")
        fcs=[]
        if isinstance(years,list):
                for yr in years:
                        efm_yr = (efm
                        .filter(ee.Filter.calendarRange(yr,yr,'year'))
                        .mosaic())
                        fc_reduced = efm_yr.reduceRegions(collection=fc,
                                                        reducer=ee.Reducer.mean(),
                                                        scale=fc.get('plot_size'),
                                                        crs='EPSG:4326',
                                                        crsTransform=None,
                                                        maxPixelsPerRegion=1e12,
                                                        tileScale=16)
                        fcs.append(fc_reduced)
        else:
                raise ValueError(f"years expects a list. provided {type(years)}")
        return fcs

def export_to_bq(fc:ee.FeatureCollection,
                 project:str="collect-earth-online",
                 dataset:str="sim_search_test",
                 table:str='my_table',
                 yr_tag:str='year',
                 dry_run:bool=False,
                 wait:bool=False) -> str: 
        """
        Export an ee.FeatureCollection to a BigQuery table.

        Args:
            fc (ee.FeatureCollection): The input FeatureCollection to export.
            project (str): cloud project that resources are contained in
            dataset (str): BQ dataset to export tables into
            table (str): table name
            dry_run (bool): print table name to export and exit
            wait (bool): whether to poll for the submitted EE task to complete before returning
        
        Returns:
            str:fully qualified BQ table that was exported (e.g. my-project.my_dataset.my_table)
        """

        if not all((isinstance(yr_tag,str), len(yr_tag)==4)):
             raise ValueError(f" yr_tag expects a %04 formatted string (e.g. '2023'). provided {yr_tag}")
        r_id = str(random.Random().randint(100,999)) # this is just to avoid weird BQ write errors during testing (unexpected behavior when deleting and re-creating same table name repeatedly)
        tb = f'{project}.{dataset}.{table}_{yr_tag}_{r_id}' 
        if len(tb) > 100: # simpler if desc and out table are same but ee.export desc has 100 char limit; 
                base_char_len = len(tb)-len(table)
                leftovers = 100-base_char_len
                tb = f'{project}.{dataset}.{table[:leftovers]}_{yr_tag}'
                
        if dry_run:
                print(f"Would Export {tb}")

        else:
                print(f"Exporting {tb}")
                taskBQ = ee.batch.Export.table.toBigQuery(
                        collection=fc,
                        description=tb,
                        table=tb,
                        # append=True,
                        overwrite=True # would probably want to overwrite as exporting to exact same table would only happen on error
                )
                     
                taskBQ.start()
                if wait:
                    poll_submitted_task(taskBQ,0.25)
        return tb.split(".")[-1]

def postprocess_bq(project:str="collect-earth-online",
                   dataset:str="sim_search_test",
                   table:str='my-table',
                   wait:bool=True) -> str:
    """Postprocesses an intermediary GEE BQ export table.

    This function takes a table exported from Earth Engine, aggregates all EFM
    band columns into a single 'embedding' array, drops the original table,
    and returns the name of the new, processed table.
    
    Args:
        project (str): The cloud project containing your BigQuery resources.
        dataset (str): The BigQuery dataset where your table resides.
        table (str): The name of the source table to process.
        wait (bool): Whether to wait for the BQ processing job to complete.

    Returns:
        str: The name of the new, processed table (e.g., 'my-table_processed').
    """
    client = Client(project=project)
    source_table_ref = f"{project}.{dataset}.{table}"
    processed_table_name = f"{table}_pp"
    processed_table_ref = f"{project}.{dataset}.{processed_table_name}"
    print(source_table_ref)
    print(processed_table_ref)
    # Note: Earth Engine exports tables with clustering on the 'geo' column.
    # We preserve this clustering in the new table for geospatial performance.
    # We must create a new table because BigQuery doesn't allow a query to
    # read from and replace the same table in one operation.
    query = f"""
        CREATE OR REPLACE TABLE `{processed_table_ref}`
        CLUSTER BY geo
        AS
        SELECT
            plotid,
            geo,
            ARRAY[A00, A01, A02, A03, A04, A05, A06, A07, A08, A09, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, A23, A24, A25, A26, A27, A28, A29, A30, A31, A32, A33, A34, A35, A36, A37, A38, A39, A40, A41, A42, A43, A44, A45, A46, A47, A48, A49, A50, A51, A52, A53, A54, A55, A56, A57, A58, A59, A60, A61, A62, A63] AS embedding
        FROM
            `{source_table_ref}`
        WHERE
            A00 IS NOT NULL
        """
    print(f"Creating processed table: {processed_table_name}")
    job = client.query(query)
    if wait:
        job.result()  # Wait for the job to complete

    # Drop the original source table now that the processed one is created
    print(f"Dropping original source table: {table}")
    client.delete_table(source_table_ref, not_found_ok=True) # have to exclude `` since not inside a BQ sql query

    return processed_table_name

def vector_index(project:str='collect-earth-online',
                 dataset:str='sim_search_test',
                 table:str='my-table',
                 embedding_col:str='embedding',
                 wait:bool=False) -> None:
    """create vector index on pre-existing table containing an embeddings column.
    
    Args:
        project (str): cloud project that your BQ resources are contained in
        dataset (str): BQ dataset containing table
        table (str): fully qualified table
        embedding_col (str): name of column containing embedding array
        wait (bool): whether to wait for BQ processing job to complete before returning

    Returns:
        None
    """
    in_table = f"`{dataset}.{table}`"
    
    print(f'indexing {in_table} for vector search')
    
    query = f"""
CREATE VECTOR INDEX my_index ON {in_table}({embedding_col})
OPTIONS(distance_type='COSINE', index_type='IVF', ivf_options='{{"num_lists": 1000}}');
"""
    print(query)
    # Run the query to create the index
    client = Client(project=project)
    job = client.query(query)
    if wait:
        job.result()  # Wait for the job to complete
    return None

def table_exists(project:str,
                 dataset:str,
                 table:str) -> bool:
    """Check if the result_table exists.
    
    Args:
        project (str): cloud project that your BQ resources are contained in
        dataset (str): BQ dataset your table is contained in
        table (str): BQ table name

    Returns:
        bool
    """
    try:
        client = Client(project=project)
        client.get_table(f"{project}.{dataset}.{table}")
        print(f"Table {table} exists.")
        return True
    except Exception as e:
        print(f"Table {table} does not exist. Error: {e}")
        return False
    
def vector_search(plotid:str,
                  n_matches:int,
                  project:str='collect-earth-online',
                  dataset:str='sim_search_test',
                  table:str='my_table',
                  ) -> pd.DataFrame:
    """Performs a vector search and returns the result as a pandas.DataFrame.
    
    Args:
        project (str): cloud project your resources are contained in
        dataset (str): BQ dataset your table is in
        plotid (str): unique plotid value of the search's target record
        n_matches (int): number of matches to return

    Returns:
        (BigQuery job): https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job
    
    """
    
    query = f"""
SELECT
  '{str(plotid)}' AS target_plotid,
  base.plotid AS base_plotid,
  distance
FROM
    VECTOR_SEARCH(
        TABLE `{dataset}.{table}`,
        'embedding',
        (SELECT * FROM `{dataset}.{table}` WHERE plotid = '{str(plotid)}' LIMIT 1),
        top_k => {n_matches + 1},
        distance_type => 'COSINE',
        options => '{{"fraction_lists_to_search": 0.005}}'
    )
ORDER BY distance
LIMIT {n_matches}
OFFSET 1 -- Offset 1 to exclude the query plot itself from the results
"""

    # Run the query and return the job result as pd.DF
    client = Client(project=project)
    job = client.query(query)
    return job.to_dataframe(max_results=n_matches)