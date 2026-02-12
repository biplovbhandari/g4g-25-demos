# g4g-25-demos

Demos and experiments for embeddings-as-a-service (EaaS): patch vs pixel embeddings, BigQuery vector search, and Earth Engine UIs.

### Background

Deep learning is becoming increasingly common for remote sensing applications. In deep learning applications of remote sensing, image classification and image segmentation serve different purposes and practitioners must make model architectural decisions early on to best serve their end goal.

So far we have seen embeddings as a service (EaaS) take off as an idea, championed by organizations like Earth Genome, others. The fundamental claim is that if an EO foundation model has learned useful enough representations of its remote sensing input data (typically spanning the globe), then the result of simply passing remote sensing data through it - an embedding - should be generally useful enough to be directly applied to your end goal task with little to no additional modeling overhead on your side (simple ML typically applied).

So far, the typical use cases highlighted are image classification. Examples include vector search ("show me areas of gold mining in the Amazon") and change detection ("Show me areas that changed from forest to urban development").

A large proportion (perhaps the majority) of remote sensing applications continue to require the specificity of image segmentation in order to quantify land form, changes, etc for important objectives like land use planning, climate and forestry reporting.

Currently, most EO foundation models either encode pixels or image patches, rendering their embeddings theoretically only directly useful for either image segmentation or classification tasks, respectively. In their published papers, all of these models are tested on both image classification and segmentation, however, further deep learning is done by the model developers to turn their model's patch-based embeddings into pixel-based results, or their model's pixel-based embeddings into patch-based results - typically known as the decoder portion of a model's entire architecture. If EaaS is to continue to mature into an actionable remote sensing application, we'd like to know if this divide can be bridged by the community.

Ideally an end user doesn't need to know which model(s) are used behind the application they are using. If there are in fact multiple models behind the curtain, we'll let the technical folk worry about that...

So...technical folk..

To practitioners: Wouldn't it be nice to have one model producing results for all of your use cases you serve in your application, or is that a pipe dream? Could some data engineering on the model output one day replace that extra deep learning overhead?

To model developers: Is it a pipe dream or are we close to a model's equal utility for all downstream task types? Could we ever truly remove the additional modeling overhead implied for EaaS products?

### Experiment

Recently, Earth Genome released global 2024 embeddings produced with a model that natively encodes image patch embeddings, while Google EFM released their pixel-based global EFM embeddings dataset (annual datasets for xx - 2024).

Our experiment:

Can a pixel-based embedding dataset be engineered to perform comparatively on image classification tasks against a patch-based embedding dataset? What's involved and how feasible is it to do?

The notebooks and demos in this repo (Southeast Asia pipeline 00–03 and the NAIP/Clay BigQuery demo) are the concrete code for exploring this question.

In the process we hope to at least come away with some newly discovered considerations, challenges, and opinions about what's next for bridging the pixel/patch divide. Our takeaways will obviously be limited in context to our results (this one model of each type among many other experiment decisions), so take them with a grain of salt.

### What's in this repo

This repo contains notebooks and Earth Engine scripts that implement the workflows above.

**Notebooks**

- **00_s2_tile_management_sea.ipynb** — Sentinel-2 tile management for Southeast Asia: aggregate ESA tile IDs to country boundaries (e.g. FAO), produce country-to-tiles mapping used to pull only relevant Earth Genome parquet files per country.
- **01_earthgenome_embeddings_bq_vectorsearch.ipynb** — Earth Genome patch embeddings: download GeoParquet from Source.Coop (AWS), load into BigQuery, enable vector search; uses the country-tile JSON from 00.
- **02_google_efm_patch_embeddings_export.ipynb** — Google EFM (pixel-based 10m) data aggregated into a patch-style format via Earth Engine Batch Export, so the same vector-search use cases can be run on EFM-derived "patch" embeddings.
- **03_google_efm_patch_embeddings_bq_vectorsearch.ipynb** — Postprocess Google EFM table (e.g. A00–A63 columns into `ARRAY<FLOAT>`), build/use BigQuery vector search on EFM embeddings (Cambodia/SEA).
- **demo__bigquery-vector-search-with-naip-clay-embedding.ipynb** — End-to-end demo: NAIP + Clay v1.5 embeddings from AWS S3 → GCS → BigQuery, build vector index (IVF), semantic search. US-focused, Colab-friendly; includes cost warning.

**Earth Engine UIs**

- **ee-ui.js** — BigQuery vector search with NAIP embeddings: select points on map or by coordinates, add points, run "Search Similar Tiles," export to Drive. Intended to run after the NAIP/Clay notebook (use in Earth Engine Code Editor).
- **ee-app-va-gse.js** — Virginia Sentinel-2 vector search app: BigQuery vector search over Sentinel-2 2024 and GAUL Virginia AOI; "next point" search behavior (Kyle Woodward).

**Archive**

- **src/_archive/** — Older or exploratory notebooks (e.g. clay-embeddings-to-bq, google_efm_patch_embeddings_step1, tour_embeddings_eda); useful for reference but not part of the main pipeline.

### How to use

Run the Southeast Asia pipeline in order: 00 (tile management) then 01 (Earth Genome to BigQuery), and 02 (Google EFM export) then 03 (Google EFM in BigQuery vector search). For the NAIP/Clay workflow, run the demo notebook (e.g. in Colab), then use `ee-ui.js` in the Earth Engine Code Editor with your exported BigQuery table. The Virginia app (`ee-app-va-gse.js`) is standalone in the Code Editor.
