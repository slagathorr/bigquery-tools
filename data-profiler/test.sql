
  # bigquery
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
              "STRUCT("", '' ), 
              "", "", ":" ), 
              ""), ", "," )
            ),
          NULL) AS new_option_value,
        t_o.option_value AS original_option_value
      FROM
        jenniebrown_bq_dataset.INFORMATION_SCHEMA.TABLES t
      LEFT JOIN
        jenniebrown_bq_dataset.INFORMATION_SCHEMA.TABLE_OPTIONS t_o
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
      jenniebrown_bq_dataset.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    GROUP BY
      table_name )

  SELECT
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
    tos.table_type