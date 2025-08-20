import os
import geopandas as gpd
import pandas as pd
import random
# df = pd.read_csv("plots/ceo-Okayama-Tile-5-v2-plot-data-2025-07-22.csv")

# gdf = gpd.GeoDataFrame(
#     df, geometry=gpd.points_from_xy(df.center_lon, df.center_lat)
# )
# print(len(gdf))
# gdf.to_file("plots/pre-table-test.geojson",driver="GeoJSON")


fake_df = pd.DataFrame({
    'plotid': range(1, 7001),
    'center_lon': [random.uniform(-180, 180) for _ in range(7000)],
    'center_lat': [random.uniform(-90, 90) for _ in range(7000)],
    'size_m': 10
})

gdf = gpd.GeoDataFrame(
    fake_df, geometry=gpd.points_from_xy(fake_df.center_lon, fake_df.center_lat)
).set_crs(epsg=4326)
print(len(gdf))
print(gdf.head(5))
gdf.to_file("plots/pre-table-test-large.geojson",driver="GeoJSON")
