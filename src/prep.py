from src.utils import efm_plot_agg, export_to_bq, postprocess_bq, vector_index, plot_to_df, df_to_fc
import yaml
import os
import ee


def prep_tables(gcp_file:str,
                project:str,
                dataset:str,
                years:list[int],
                ) -> None:
    
    plot_df = plot_to_df(gcp_file)
    plot_fc = df_to_fc(plot_df)
    
    fc_embeddings = efm_plot_agg(plot_fc,years) # export EFM image data (n=64 bands) to each feature in collection
    
    new_table_base = f"{os.path.basename(gcp_file).split('.')[0]}"
    for i,yr_embed in enumerate(fc_embeddings):
        year_tag = str(years[i])
        table = export_to_bq(yr_embed, # export the featurecollection to BQ table
                            project,
                            dataset,
                            new_table_base,
                            year_tag,
                            wait=True,
                            dry_run=False)
        
        # If post-processing fails, we should not attempt to create an index.
        try:
            pp_table = postprocess_bq(project,dataset,table,wait=True) # fix the schema of the exported table to contain one 'embedding' column containing a 1x64 array

            # BigQuery does not allow creating a VECTOR index on a table with < 5k rows.
            # Perform the vector_index fn as the last part of postprocessing if the condition is met.
            row_count = len(plot_df)
            if row_count > 5000:
                print(f"Row count ({row_count}) > 5000. Creating Vector Index...")
                vector_index(project,dataset,pp_table,embedding_col='embedding',wait=True)

            print(f"Successfully created and processed table: {pp_table}")
        except Exception as e:
            print(f"Failed to post-process or index table for year {year_tag}. Reason: {e}")
            continue # Move to the next year in the loop
    return None
    
if __name__ == "__main__":
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    project = config['gcp']['project']
    dataset = config['gcp']['bq-dataset']
    print(f"Using config: {config}")

    # When running locally, GOOGLE_APPLICATION_CREDENTIALS must be set in the environment.
    # ee.Initialize() will automatically find and use them.
    ee.Initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")

    ee.data.setWorkloadTag("efm-table-prep")

    prep_tables(gcp_file="gs://sim-search/ceo-100-plots.geojson",
                project=project,
                dataset=dataset,
                years=[2018,2019]
                )