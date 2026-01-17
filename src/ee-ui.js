/**
 * Earth Engine UI for BigQuery Vector Search with NAIP Embeddings
 *
 * PREREQUISITES:
 * This script must be run after executing the notebook available at:
 * https://colab.research.google.com/drive/1-kxxqp42WZxS9Y0DEGI1Oq4PzAdZ4jGn?usp=sharing
 *
 * IMPORTANT: Before running this script, update the BigQuery table reference
 * in the input field below to point to the table exported from the notebook above.
 * The default value is an example and should be replaced with your exported table.
 *
 * DESCRIPTION:
 * Interactive Earth Engine UI for finding similar NAIP tiles using BigQuery
 * vector search. Users can click on the map or enter coordinates to select
 * example points, then search for similar tiles based on embedding similarity.
 *
 * USAGE:
 * 1. Update the BigQuery table reference to your exported embeddings table
 * 2. Click on the map or enter coordinates to select example points
 * 3. Click "Add Point" button as you add more example points
 * 4. Click "Search Similar Tiles" to find similar tiles using vector search
 * 5. Export results to Google Drive if needed
 *
 * @author Biplov Bhandari
 * @lastModified January 17, 2026
 */

// Base map and NAIP layer
var map = ui.Map();
map.style().set('cursor', 'crosshair');
map.setCenter(-119.216358, 46.733559, 8);

// Load and add NAIP 2020–2024 composite
var naip = ee.ImageCollection('USDA/NAIP/DOQQ');
var y_2020 = naip.filter(ee.Filter.calendarRange(2020, 2020, 'year')).select(['R', 'G', 'B', 'N']).median();
var y_2021 = naip.filter(ee.Filter.calendarRange(2021, 2021, 'year')).select(['R', 'G', 'B', 'N']).median();
var y_2022 = naip.filter(ee.Filter.calendarRange(2022, 2022, 'year')).select(['R', 'G', 'B', 'N']).median();
var y_2023 = naip.filter(ee.Filter.calendarRange(2023, 2023, 'year')).select(['R', 'G', 'B', 'N']).median();
var y_2024 = naip.filter(ee.Filter.calendarRange(2024, 2024, 'year')).select(['R', 'G', 'B', 'N']).median();

map.addLayer(y_2020, {bands: ['R','G','B'], min: 0, max: 255}, 'NAIP 2020', false);
map.addLayer(y_2021, {bands: ['R','G','B'], min: 0, max: 255}, 'NAIP 2021', true);
map.addLayer(y_2022, {bands: ['R','G','B'], min: 0, max: 255}, 'NAIP 2022', false);
map.addLayer(y_2023, {bands: ['R','G','B'], min: 0, max: 255}, 'NAIP 2023', false);
map.addLayer(y_2024, {bands: ['R','G','B'], min: 0, max: 255}, 'NAIP 2024', false);

// UI panel
var panel = ui.Panel({style:{position:'bottom-left', width:'300px'}});
ui.root.clear();
ui.root.add(map);
ui.root.add(panel);

// BigQuery table input
var bqTableInput = ui.Textbox({
  placeholder: 'project.dataset.table',
  value: 'biplov-gde-project.naip_embeddings.embeddings',
  style: {stretch: 'horizontal'}
});
panel.add(ui.Label('BigQuery Table Reference:', {fontWeight: 'bold'}));
panel.add(ui.Label('Provide a fully qualified table reference in the form `project.dataset.table`'));
panel.add(bqTableInput);

// Instructions
panel.add(ui.Label('1) Click map or enter coords below.'));
panel.add(ui.Label('2) Click "Add Point" button as you add more example points on the map.'));
panel.add(ui.Label('3) Then click "Search Similar Tiles".'));

// Coordinate inputs
var lonInput = ui.Textbox({placeholder:'Longitude', value: -117.921367, style:{stretch:'horizontal'}});
var latInput = ui.Textbox({placeholder:'Latitude', value: 48.548551, style:{stretch:'horizontal'}});
panel.add(ui.Label('Enter coordinates here:'));
panel.add(lonInput).add(latInput);

var addBtn = ui.Button('Add Point', function() { addPoint(); });
panel.add(addBtn);

// Point list and clear button
var listLabel = ui.Label('Selected Points:', {margin:'6px 0 0 0'});
var listPanel = ui.Panel([], ui.Panel.Layout.flow('vertical'));
var clearBtn = ui.Button('Clear All', function() { clearPoints(); });
panel.add(listLabel).add(listPanel).add(clearBtn);

// Search button
var searchBtn = ui.Button('Search Similar Tiles', function() { runSearch(); },
                          {disabled:true, margin:'6px 0 0 0'});
panel.add(searchBtn);

// Data structures
var selectedPoints = []; // array of {lon,lat}
var markers = [];
var resultLayer = null;
var bufferedResultLayer = null;

// Render list of selected points
function refreshList() {
  listPanel.clear();
  selectedPoints.forEach(function(pt, idx) {
    listPanel.add(ui.Label(
      (idx+1)+'.  '+pt.lon.toFixed(6)+', '+pt.lat.toFixed(6),
      {fontSize:'12px'}));
  });
  searchBtn.setDisabled(selectedPoints.length === 0);
}

// Add a point from coordinate inputs
function addPoint() {
  var lon = parseFloat(lonInput.getValue());
  var lat = parseFloat(latInput.getValue());
  if (isNaN(lon) || isNaN(lat)) return;

  selectedPoints.push({lon:lon, lat:lat});
  var dot = ee.Geometry.Point([lon,lat]);
  var m = ui.Map.Layer(dot, {color:'red'}, 'pt'+selectedPoints.length);
  map.layers().add(m);
  markers.push(m);
  refreshList();
}

// Clear all points and results
function clearPoints() {
  selectedPoints = [];
  listPanel.clear();
  searchBtn.setDisabled(true);
  markers.forEach(function(m){ map.layers().remove(m); });
  markers = [];
  if (resultLayer) {
    map.layers().remove(resultLayer);
    resultLayer = null;
  }
  if (bufferedResultLayer) {
    map.layers().remove(bufferedResultLayer);
    bufferedResultLayer = null;
  }
  enableExport();
}

// Map click handler - populate coordinate inputs
map.onClick(function(coords){
  lonInput.setValue(coords.lon.toFixed(6));
  latInput.setValue(coords.lat.toFixed(6));
  listLabel.setValue('Selected: ' + coords.lon.toFixed(6) + ', ' + coords.lat.toFixed(6));
});

// Build and run the BigQuery vector search using the mean embedding
function runSearch() {
  // Get BigQuery table reference from input
  var bqTable = bqTableInput.getValue().trim();
  if (!bqTable) {
    ui.alert('Please enter a BigQuery table reference.');
    return;
  }

  // Remove old results
  if (resultLayer) map.layers().remove(resultLayer);
  if (bufferedResultLayer) map.layers().remove(bufferedResultLayer);

  // Build the WITH points AS (...) SQL
  var pointsSQL = selectedPoints.map(function(pt){
    return 'SELECT ' + pt.lon + ' AS lon, ' + pt.lat + ' AS lat';
  }).join(' UNION ALL\n  ');

  // Build the SQL query string with parameterized table reference
  var sql =
  "WITH points AS (\n" +
  pointsSQL + "\n" +
  "),\n" +
  "query_embeddings AS (\n" +
  "SELECT\n" +
  "  e       AS val,\n" +
  "  idx     AS idx\n" +
  "FROM points\n" +
  "JOIN `" + bqTable + "` AS t\n" +
  "  ON ST_CONTAINS(\n" +
  "       t.geometry,\n" +
  "       ST_GEOGPOINT(points.lon, points.lat)\n" +
  "     )\n" +
  "CROSS JOIN UNNEST(t.embeddings) AS e WITH OFFSET AS idx\n" +
  "),\n" +
  "avg_per_idx AS (\n" +
  "  SELECT\n" +
  "    idx,\n" +
  "    AVG(val) AS avg_val\n" +
  "  FROM query_embeddings\n" +
  "  GROUP BY idx\n" +
  "),\n" +
  "mean_embedding AS (\n" +
  "  SELECT\n" +
  "    ARRAY_AGG(avg_val ORDER BY idx) AS embeddings\n" +
  "  FROM avg_per_idx\n" +
  ")\n" +
  "SELECT\n" +
  "  base.geometry,\n" +
  "  base.embeddings,\n" +
  "  base.state,\n" +
  "  base.year,\n" +
  "  base.dt,\n" +
  "  base.geohash5,\n" +
  "  distance\n" +
  "FROM\n" +
  "  VECTOR_SEARCH(\n" +
  "    TABLE `" + bqTable + "`,\n" +
  "    'embeddings',\n" +
  "    TABLE mean_embedding,\n" +
  "    TOP_K         => 100,\n" +
  "    DISTANCE_TYPE => 'COSINE',\n" +
  "    OPTIONS       => '{\"fraction_lists_to_search\": 0.02}'\n" +
  "  )\n" +
  "WHERE distance > 0\n" +
  "  AND distance < 0.2\n" +
  "ORDER BY distance ASC\n";

  // Run BigQuery and buffer the matches
  var fc = ee.FeatureCollection.runBigQuery(sql, 'geometry', 1e11);

  // Turn tiles into circles for visibility
  var circles = fc.map(function(f){
    return ee.Feature(f.geometry().centroid().buffer(500), f.toDictionary());
  });

  // Add buffered results layer
  var visParamsBuffered = { color: 'yellow' };
  bufferedResultLayer = ui.Map.Layer(circles, visParamsBuffered, 'Similar Tiles (buffered)');
  map.layers().add(bufferedResultLayer);

  // Add original results layer
  var visParams = { color: 'green' };
  resultLayer = ui.Map.Layer(fc, visParams, 'Similar Tiles');
  map.layers().add(resultLayer);

  print('returned embeddings tiles', fc.size());
  print('example embeddings tiles', fc.limit(5));

  enableExport();
}


// Export button
var exportBtn = ui.Button({
  label: 'Export Matches to Drive',
  disabled: true,
  onClick: exportMatches
});
panel.add(exportBtn);

// Enable export button when results are available
function enableExport() {
  exportBtn.setDisabled(resultLayer === null);
}

// Export function
function exportMatches() {
  if (!resultLayer) {
    ui.alert('No matches to export.');
    return;
  }
  var fc = resultLayer.getEeObject();
  Export.table.toDrive({
    collection: fc,
    description: 'NAIP_Similar_Tiles_Export',
    folder: 'EarthEngine_Exports',
    fileNamePrefix: 'similar_tiles',
    fileFormat: 'GeoJSON'
  });
}
