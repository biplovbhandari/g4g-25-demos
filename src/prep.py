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
        
        try:
            pp_table = postprocess_bq(project,dataset,table,wait=True) # fix the schema of the exported table to contain one 'embedding' column containing a 1x64 array
        except Exception as e:
            print(f"Couldn't post-process BQ table. reason: {e}")
        
        # apparently creating a VECTOR index on a BQ table with < 5k rows is not allowed
        # so perform the vector_index fn as last part of postprocessing if row count condition is met
        row_count = len(plot_df) 
        if row_count > 5000:
            print("Creating Vector Index for large (n>5k) table")
            vector_index(project,dataset,table+"_pp",embedding_col='embedding',wait=True)
        
        print(f"{pp_table} created")
    return None
    
if __name__ == "__main__":
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)

    project = config['gcp']['project']
    dataset = config['gcp']['bq-dataset']
    print(f"Using config: {config}")

    # Set the credentials and project
    ee.Initialize(project=project,
                    opt_url="https://earthengine-highvolume.googleapis.com"
                )
    ee.data.setWorkloadTag("efm-table-prep")

    prep_tables(gcp_file="gs://sim-search/pre-table-test.geojson",
                project=project,
                dataset=dataset,
                years=[2018,2019]
                )