# California has an update, and a new row.
# Delaware is a duplicate.
# Florida is a new row.
SELECT DATE(2020,2,15) ingest_ts, DATE(2020,1,15) reported_dt, 'California' COLA, '18-22' COLB, 200 MEASUREMENT, 'hello' COLC # update
UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'California', '22-30', 100, 'pong' # insert
UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'Delaware', '22-30', 200, 'ping' # duplicate ignore
UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'Florida', '18-22', 500, 'And I think to myself What a wonderful world' # insert
UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'California', '18-22', 100, 'hello' # duplicate ignore;