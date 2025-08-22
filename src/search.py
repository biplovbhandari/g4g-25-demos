import yaml
import pandas as pd

from src.utils import vector_search, table_exists

def search_result(uniqueid:int,
                  matches:int,
                  project:str,
                  dataset:str,
                  table:str
                  ) -> pd.DataFrame:
    """
    send a vector search request for a given bigquery table (project.dataset.table) and the search target's unique id, returning n_matches

    Args:
        uniqueid (int): unique id of the search target
        matches (int): number of matches to return
        project (str): cloud project that your BQ resources are contained in
        dataset (str): BQ dataset your table is contained in
        table (str): BQ table name

    Returns:
        pd.DataFrame: search result as a pandas DataFrame

    Raises:
        FileNotFoundError: If the target table does not exist in BigQuery.
    """
    if not table_exists(project, dataset, table):
        raise FileNotFoundError(f"The table {project}.{dataset}.{table} does not exist.")

    result_df = vector_search(plotid=uniqueid,
                            n_matches=matches,
                            project=project,
                            dataset=dataset,
                            table=table)
    return result_df


if __name__ == "__main__":
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    project_id = config['gcp']['project']
    dataset_id = config['gcp']['bq-dataset']

    results = search_result(uniqueid=5,
                            matches=5,
                            project=project_id,
                            dataset=dataset_id,
                            table="ceo-Okayama-Tile-5-v2-plot-data-2025-07-22_2022_2022_437_pp")
    print(results)