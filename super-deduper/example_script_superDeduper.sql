/*
  Copyright 2020 Brian Suk
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

DECLARE order_column STRING DEFAULT 'ingest_ts';
DECLARE unique_columns ARRAY<STRING> DEFAULT ['reported_dt', 'cola', 'colb'];
DECLARE measurement_columns ARRAY<STRING> DEFAULT ['measurement', 'colc'];
DECLARE rxstring_only_unique_columns STRING DEFAULT r'(?i)"(?:' || ARRAY_TO_STRING(ARRAY_CONCAT((SELECT [order_column, 'is_latest']), measurement_columns), "|") || r')"(?-i):.+?[,}]';
DECLARE rxstring_only_measurement_columns STRING DEFAULT r'(?i)"(?:' || ARRAY_TO_STRING(ARRAY_CONCAT((SELECT [order_column, 'is_latest']), unique_columns), "|") || r')"(?-i):.+?[,}]';

# Base Data
CREATE TEMP TABLE main_table
  AS (
    SELECT FALSE is_latest, DATE(2020,1,15) ingest_ts, DATE(2020,1,15) reported_dt, "California" COLA, "18-22" COLB, 100 MEASUREMENT, "hello" COLC
    UNION ALL SELECT TRUE, DATE(2020,1,16), DATE(2020,1,15), "California", "18-22", 150, "hello"
    UNION ALL SELECT TRUE, DATE(2020,1,15), DATE(2020,1,15), "California", "31-50", 500, "world"
    UNION ALL SELECT TRUE, DATE(2020,1,15), DATE(2020,1,15), "Delaware", "22-30", 200, "ping"
  );

# California has an update, and a new row, and a duplicate from history.
# Delaware is a duplicate.
# Florida is a new row.
CREATE TEMP TABLE ingest_table
  AS (
    SELECT DATE(2020,2,15) ingest_ts, DATE(2020,1,15) reported_dt, 'California' COLA, '18-22' COLB, 200 MEASUREMENT, 'hello' COLC # update
    UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'California', '22-30', 100, 'pong' # insert
    UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'Delaware', '22-30', 200, 'ping' # duplicate ignore
    UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'Florida', '18-22', 500, 'And I think to myself What a wonderful world' # insert
    UNION ALL SELECT DATE(2020,2,15), DATE(2020,1,15), 'California', '18-22', 100, 'hello' # historical duplicate ignore
  );
  
/*
The expected output should be:

is_latest | ingest_ts | reported_dt | COLA       | COLB  | MEASUREMENT | COLC
---------------------------------------------------------------------------------------------------------------------
FALSE     | 2020-1-15 | 2020-1-15   | California | 18-22 | 100         | hello
FALSE     | 2020-1-16 | 2020-1-15   | California | 18-22 | 150         | hello
TRUE      | 2020-1-15 | 2020-1-15   | California | 31-50 | 500         | world
TRUE      | 2020-1-15 | 2020-1-15   | Delaware   | 22-30 | 200         | ping
TRUE      | 2020-2-15 | 2020-1-15   | California | 18-22 | 200         | hello
TRUE      | 2020-2-15 | 2020-1-15   | California | 22-30 | 100         | pong
TRUE      | 2020-2-15 | 2020-1-15   | Florida    | 18-22 | 500         | And I think to myself What a wonderful world
*/

# Create temporaty staging table to hold just the incoming rows that are updates to rows in the main table.
CREATE TEMP TABLE update_staging AS (
  # The new rows will be the "latest" ones, so mark it as is_latest = TRUE
  SELECT TRUE AS is_latest, i.* FROM ingest_table i, main_table m
  WHERE
    # This clause looks for the latest row in the main table where the unique columns match, but the measurement columns differ. Since
    # the current load is more recent, we accept this as the new data, so this goes into the staging table.
    (m.is_latest = TRUE 
      AND REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_unique_columns, '') = REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_unique_columns, '')
      AND REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_measurement_columns, '') != REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_measurement_columns, '')
    )
    # This clause looks for rows in the main table with is_latest = FALSE, indicating historical rows, that match both the unique and measurement
    # columns, indicating that the inbound row is a duplicate of something in the past, and we want to ignore these.
    AND
    REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_unique_columns, '') || REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_measurement_columns, '')
    NOT IN
    (SELECT REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_unique_columns, '') || REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_measurement_columns, '')
      FROM ingest_table i, main_table m 
      WHERE (m.is_latest = FALSE
        AND REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_unique_columns, '') = REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_unique_columns, '')
        AND REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_measurement_columns, '') = REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_measurement_columns, ''))
    )
);

# Invalidate rows in the main table where a match exists in the staging table holding the new updates.
MERGE main_table m
USING update_staging u
ON REGEXP_REPLACE(TO_JSON_STRING(u), rxstring_only_unique_columns, '') = REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_unique_columns, '')
AND REGEXP_REPLACE(TO_JSON_STRING(u), rxstring_only_measurement_columns, '') != REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_measurement_columns, '')
AND m.is_latest = TRUE
WHEN MATCHED THEN UPDATE SET m.is_latest = FALSE;

# Load the new updated rows.
INSERT INTO main_table
(SELECT * FROM update_staging);

# Load rows that are true new rows.
MERGE main_table m
USING (SELECT TRUE is_latest, * FROM ingest_table) i
ON REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_unique_columns, '') = REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_unique_columns, '')
AND REGEXP_REPLACE(TO_JSON_STRING(i), rxstring_only_measurement_columns, '') = REGEXP_REPLACE(TO_JSON_STRING(m), rxstring_only_measurement_columns, '')
WHEN NOT MATCHED THEN INSERT ROW;

# Huzzah, it works!
SELECT * FROM main_table ORDER BY 4, 5, 3, 2 ASC, 1 DESC