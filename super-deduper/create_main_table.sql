# Base Data
SELECT TRUE is_latest, DATE(2020,1,15) ingest_ts, DATE(2020,1,15) reported_dt, "California" COLA, "18-22" COLB, 100 MEASUREMENT, "hello" COLC
UNION ALL SELECT TRUE, DATE(2020,1,15), DATE(2020,1,15), "California", "31-50", 500, "world"
UNION ALL SELECT TRUE, DATE(2020,1,15), DATE(2020,1,15), "Delaware", "22-30", 200, "ping";