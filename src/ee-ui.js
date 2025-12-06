// 1) Base map and NAIP layer
var map = ui.Map();
// map.setOptions('SATELLITE');
map.style().set('cursor', 'crosshair');
map.setCenter(-119.216358, 46.733559, 8);

// Load and add NAIP 2020–2024 composite
var naip = ee.ImageCollection('USDA/NAIP/DOQQ');
// naip years: [ 2005, 2008, 2010, 2013, 2015, 2017, 2018, 2020, 2021, 2022, 2023, 2024, ]
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


// 2) UI panel
var panel = ui.Panel({style:{position:'bottom-left', width:'300px'}});
ui.root.clear(); ui.root.add(map); ui.root.add(panel);

// 3) Controls
panel.add(ui.Label('1) Click map or enter coords below.'));
panel.add(ui.Label('2) Then click "Search Similar Tiles".'));

// panel.add(ui.Label('Click map or type coords, then "Add Point":'));

var lonInput = ui.Textbox({placeholder:'Longitude', value: -117.921367, style:{stretch:'horizontal'}});
var latInput = ui.Textbox({placeholder:'Latitude', value: 48.548551, style:{stretch:'horizontal'}});
panel.add(ui.Label('Enter coordinates here:'));
panel.add(lonInput).add(latInput);

var addBtn = ui.Button('Add Point', function() { addPoint(); });
panel.add(addBtn);

// 4) Point list and clear button
var listLabel = ui.Label('Selected Points:', {margin:'6px 0 0 0'});
var listPanel = ui.Panel([], ui.Panel.Layout.flow('vertical'));
var clearBtn = ui.Button('Clear All', function() { clearPoints(); });
panel.add(listLabel).add(listPanel).add(clearBtn);

// 5) Search button
var searchBtn = ui.Button('Search Similar Tiles', function() { runSearch(); },
                          {disabled:true, margin:'6px 0 0 0'});
panel.add(searchBtn);

// 6) Data structures
var selectedPoints = [];   // array of {lon,lat}
var markers = [];          // ui.Map.Layer for each point
var resultLayer = null;    // for previous results
var bufferedResultLayer = null;

// 7) Helpers: render listPanel
function refreshList() {
  listPanel.clear();
  selectedPoints.forEach(function(pt, idx) {
    listPanel.add(ui.Label(
      (idx+1)+'.  '+pt.lon.toFixed(6)+', '+pt.lat.toFixed(6),
      {fontSize:'12px'}));
  });
  searchBtn.setDisabled(selectedPoints.length === 0);
}

// 8) Add a point (from inputs or last click)
function addPoint() {
  var lon = parseFloat(lonInput.getValue());
  var lat = parseFloat(latInput.getValue());
  if (isNaN(lon)||isNaN(lat)) return;

  selectedPoints.push({lon:lon, lat:lat});
  // add marker
  var dot = ee.Geometry.Point([lon,lat]);
  var m = ui.Map.Layer(dot, {color:'red'}, 'pt'+selectedPoints.length);
  map.layers().add(m);
  markers.push(m);
  refreshList();
}

// 9) Clear all
function clearPoints() {
  selectedPoints = [];
  listPanel.clear();
  searchBtn.setDisabled(true);
  markers.forEach(function(m){ map.layers().remove(m); });
  markers = [];
  if (resultLayer) map.layers().remove(resultLayer);
  if (resultLayer) {
    map.layers().remove(resultLayer);
    resultLayer = null;
    enableExport();
  }
}

// 10) Map click → populate inputs (does not auto-add)
map.onClick(function(coords){
  lonInput.setValue(coords.lon.toFixed(6));
  latInput.setValue(coords.lat.toFixed(6));
  listLabel.setValue('Selected: ' + coords.lon.toFixed(6) + ', ' + coords.lat.toFixed(6));
});

// 11) Build and run the BigQuery using the mean embedding
function runSearch() {
  // remove old results
  if (resultLayer) map.layers().remove(resultLayer);

  // Build the WITH points AS (...) SQL
  var pointsSQL = selectedPoints.map(function(pt){
    return 'SELECT ' + pt.lon + ' AS lon, ' + pt.lat + ' AS lat';
  }).join(' UNION ALL\n  ');

  // Build the SQL query string
  var sql =
  "WITH points AS (\n" +
  pointsSQL + "\n" +
  "),\n" +
  "query_embeddings AS (\n" +
  "SELECT\n" +
  "  e       AS val,\n" +
  "  idx     AS idx\n" +
  "FROM points\n" +
  "JOIN `biplov-gde-project.naip_embeddings.embeddings` AS t\n" +
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
  "    TABLE `biplov-gde-project.naip_embeddings.embeddings`,\n" +
  "    'embeddings',\n" +
  "    TABLE mean_embedding,\n" +
  "    TOP_K         => 100,\n" +
  "    DISTANCE_TYPE => 'COSINE',\n" +
  "    OPTIONS       => '{\"fraction_lists_to_search\": 0.02}'\n" +
  "  )\n" +
  "WHERE distance > 0\n" +
  "  AND distance < 0.2\n" +
  "ORDER BY distance ASC\n";

  // Run and buffer the matches
  var fc = ee.FeatureCollection.runBigQuery(sql, 'geometry', 1e11);
  // Turn tiles into circles for visibility
  var circles = fc.map(function(f){
    return ee.Feature(f.geometry().centroid().buffer(500),
                      f.toDictionary());
  });


  var visParamsBuffered = { color: 'yellow' };
  if (bufferedResultLayer) map.layers().remove(bufferedResultLayer);
  bufferedResultLayer = ui.Map.Layer(circles, visParamsBuffered, 'Similar Tiles (buffered)');
  map.layers().add(bufferedResultLayer);

  var visParams = { color: 'green' };
  if (resultLayer) map.layers().remove(resultLayer);
  resultLayer = ui.Map.Layer(fc, visParams, 'Similar Tiles');
  map.layers().add(resultLayer);


  print('returned embeddings tiles', fc.size());
  print('example embeddings tiles', fc.limit(5));

  enableExport();

}


// 12) Export button
var exportBtn = ui.Button({
  label: 'Export Matches to Drive',
  disabled: true,
  onClick: exportMatches
});
panel.add(exportBtn);

// 13) Enable export when results arrive
function enableExport() {
  exportBtn.setDisabled(resultLayer === null);
}

// 14) Export function
function exportMatches() {
  if (!resultLayer) {
    ui.alert('No matches to export.');
    return;
  }
  // Grab the FeatureCollection (buffered circles)
  var fc = resultLayer.getEeObject();

  // Kick off a Drive export job
  Export.table.toDrive({
    collection: fc,
    description: 'NAIP_Similar_Tiles_Export',
    folder: 'EarthEngine_Exports',
    fileNamePrefix: 'similar_tiles',
    fileFormat: 'GeoJSON'
  });
}
