#bigquery

CREATE OR REPLACE PROCEDURE metadata_catalog.generateMetadataAll( # CHANGE TARGET DATASET HERE IF NEEDED
  metadata_dataset STRING
)

BEGIN
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

  /*
  Input:
    metadata_dataset STRING: The name of the dataset where you want to store
      the results, usually will be the same dataset where you will want to create
      this procedure.
  */
  
  DECLARE schema_list ARRAY<STRING>;
  DECLARE iter INT64 DEFAULT 0;
  DECLARE query_string STRING;
  DECLARE current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  SET
    schema_list = (
    SELECT
      ARRAY_AGG(schema_name)
    FROM
      INFORMATION_SCHEMA.SCHEMATA);
  SELECT
    schema_list;
  WHILE
    iter < ARRAY_LENGTH(schema_list) DO
    IF iter = 0 THEN
      SET query_string = "CREATE OR REPLACE TABLE " || metadata_dataset || ".metadata_all AS ";
      ELSE SET query_string = "INSERT " || metadata_dataset || ".metadata_all ";
    END IF;

    SET
      query_string = query_string || """
      (# bigquery
      # Query that will combine table details with its column details
      # from the INFORMATION_SCHEMA.

      WITH
        # Parse out and clean up the option_value column from TABLE_OPTIONS
        cleaned_table_options AS (
          SELECT
            t.table_catalog,
            t.table_schema,
            t.table_name,
            t.table_type,
            t_o.option_name,
            IF
              (t_o.option_name = "labels",
                SPLIT( REPLACE( REPLACE( REPLACE( 
                  SUBSTR(t_o.option_value, 2, LENGTH(option_value)-4), 
                  "STRUCT(\\"", '' ), 
                  "\\", \\"", ":" ), 
                  "\\"), ", "," )
                ),
              NULL) AS new_option_value,
            t_o.option_value AS original_option_value
          FROM
            """ || schema_list[
    OFFSET
      (iter)] || """.INFORMATION_SCHEMA.TABLES t
          LEFT JOIN
            """ || schema_list[
    OFFSET
      (iter)] || """.INFORMATION_SCHEMA.TABLE_OPTIONS t_o
          ON
            t.table_catalog = t_o.table_catalog
            AND t.table_schema = t_o.table_schema
            AND t.table_name = t_o.table_name ), 

        # Create arrays and structs for the options and values.
        table_options_struct AS (
          SELECT
            cleaned_table_options.table_catalog,
            cleaned_table_options.table_schema,
            cleaned_table_options.table_name,
            cleaned_table_options.table_type,
          IF
            (option_name = "labels",
              STRUCT(ARRAY_AGG(STRUCT(SPLIT(unnested_option_value, ":")[OFFSET(0)] AS name,
                SPLIT(unnested_option_value, ":")[OFFSET(1)] AS value) RESPECT NULLS) AS label_object),
              NULL) AS label,
          IF
            (option_name != "labels",
              ANY_VALUE(STRUCT(option_name AS name,
                  original_option_value AS value)),
              NULL) AS option,
          FROM
            cleaned_table_options
          LEFT JOIN
            UNNEST(new_option_value) AS unnested_option_value
          GROUP BY
            table_catalog,
            table_schema,
            table_name,
            table_type,
            option_name ),

        # Aggregate column information.
        COLUMNS AS (
        SELECT
          table_name,
          ARRAY_AGG(STRUCT(
              column_name AS name,
              field_path AS field_path,
              data_type AS type,
              description)) column
        FROM
          """ || schema_list[
    OFFSET
      (iter)] || """.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        GROUP BY
          table_name )

      SELECT
        current_timestamp AS last_updated,
        tos.table_catalog,
        tos.table_schema,
        tos.table_name,
        tos.table_type,
        STRUCT(ARRAY_AGG(tos.label IGNORE NULLS) as label, 
          ARRAY_AGG(tos.option IGNORE NULLS) as option) as table_options,
        ANY_VALUE(columns.column) as column
      FROM
        table_options_struct tos
      LEFT JOIN
        columns
      ON
        tos.table_name = COLUMNS.table_name
      GROUP BY
        tos.table_catalog,
        tos.table_schema,
        tos.table_name,
        tos.table_type)
        """;
    SELECT query_string;
    EXECUTE IMMEDIATE query_string;
    SET
      iter = iter + 1;
  END WHILE;
END;