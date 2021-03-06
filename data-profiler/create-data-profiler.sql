#bigquery

CREATE OR REPLACE PROCEDURE
  data_profiler_test.ProfileTable( # REPLACE data_profiles WITH YOUR DATASET
    full_source_table_name STRING,
    full_destination_table_name STRING,
    max_groups INT64
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
    READ ME FIRST:
    Be sure to understand BigQuery pricing, and the plan that you are on, as you
    will likely be incurring query costs to your project that you are responsible for.
    More information at: https://cloud.google.com/bigquery/pricing

    INPUT PARAMETERS:
      full_source_table_name:
        Fully qualified input string.
        Ex: `bigquery-public-data.covid19_nyt.us_counties`
      full_destination_table_name:
        Fully qualified destination table.
        Ex: `hereismyorg.hereismydataset.hello_profile`
      max_groups:
        Maximum number of groups for certain profile tasks, such as pattern analysis.
  */
  
  # DECLARE Working Variables
  DECLARE profile_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE profile_id STRING DEFAULT GENERATE_UUID();
  DECLARE query_string STRING;
  
  DECLARE total_row_count INT64;

  DECLARE null_count INT64;
  DECLARE non_null_count INT64;

  DECLARE mean FLOAT64;
  DECLARE median FLOAT64;

  DECLARE min STRING;
  DECLARE max STRING;

  DECLARE var_pattern_distribution ARRAY<STRUCT<value STRING, raw_count INT64, percentage NUMERIC>>;
  DECLARE var_inferred_data_type ARRAY<STRUCT<inferred_type STRING, type_count INT64, type_percentage NUMERIC>>;

  DECLARE columns_to_profile ARRAY<STRING>;
  DECLARE column_iterator INT64 DEFAULT 0;

  DECLARE source_org_name STRING;
  DECLARE source_project_name STRING;
  DECLARE source_table_name STRING;

  SET source_org_name = SPLIT(TRIM(full_source_table_name, '`'), '.')[OFFSET(0)];
  SET source_project_name = SPLIT(TRIM(full_source_table_name, '`'), '.')[OFFSET(1)];
  SET source_table_name = SPLIT(TRIM(full_source_table_name, '`'), '.')[OFFSET(2)];

  # Create a temp function that does type inference.
  CREATE TEMP FUNCTION
    get_type(instring STRING) AS 
    (
      CASE
        WHEN instring IS NULL THEN NULL
        WHEN 
        ( 
          SAFE_CAST(instring AS BOOL) IS NOT NULL
            OR TRIM(LOWER(instring)) = "yes"
            OR TRIM(LOWER(instring)) = "no"
        ) THEN "BOOL"
        WHEN SAFE_CAST(instring AS INT64) IS NOT NULL THEN "INT64"
        WHEN SAFE_CAST(instring AS NUMERIC) IS NOT NULL THEN "NUMERIC"
        WHEN SAFE_CAST(instring AS FLOAT64) IS NOT NULL THEN "FLOAT64"
        WHEN 
        (
          SAFE.PARSE_DATE("%Y-%m-%d", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%m-%d-%Y", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%d-%m-%Y", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%Y/%m/%d", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%m/%d/%Y", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%d/%m/%Y", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%Y.%m.%d", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%m.%d.%Y", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%d.%m.%Y", instring) IS NOT NULL
            OR SAFE.PARSE_DATE("%B %d, %Y", instring) IS NOT NULL
        ) THEN "DATE"
        ELSE "STRING"
      END
    );

  # Get the list of columns in the table to profile.
  EXECUTE IMMEDIATE
    FORMAT("""
      SELECT
        ARRAY_AGG(column_name)
      FROM
        %s
      WHERE
        table_catalog = @var_source_org_name
        AND table_schema = @var_source_project_name
        AND table_name = @var_source_table_name;""",
      CONCAT("`",source_org_name,".",source_project_name,".INFORMATION_SCHEMA.COLUMNS`")
    )
    INTO columns_to_profile
    USING
      source_org_name AS var_source_org_name,
      source_project_name AS var_source_project_name,
      source_table_name AS var_source_table_name;

  # Create Profile Results Table
  EXECUTE IMMEDIATE
    FORMAT("""CREATE TABLE IF NOT EXISTS %s 
    (
      profile_id STRING,
      profile_timestamp TIMESTAMP,
      column_name STRING,
      column_distribution ARRAY<STRUCT<value STRING, raw_count INT64, percentage NUMERIC>>,
      null_analysis STRUCT<null_count INT64, null_percentage NUMERIC, non_null_count INT64, non_null_percentage NUMERIC>,
      numeric_analysis STRUCT<mean NUMERIC, median NUMERIC>,
      min_max STRUCT<min STRING, max STRING>,
      inferred_data_type ARRAY<STRUCT<inferred_type STRING, type_count INT64, type_percentage NUMERIC>>
    )""", full_destination_table_name);

  # Get the total row count.
  EXECUTE IMMEDIATE
    FORMAT("SELECT COUNT(*) FROM %s", full_source_table_name)
    INTO total_row_count;

  # Build query for min/max, nulls, mean/median
  SET query_string = "CREATE TEMP TABLE numeric_calculation_results AS (";
  WHILE
    column_iterator < ARRAY_LENGTH(columns_to_profile)
  DO
    SET
      query_string = query_string ||
        IF
          (column_iterator != 0," UNION ALL ","");
    SET
      query_string = query_string 
        || "SELECT \"" 
          # column name
          || columns_to_profile[OFFSET(column_iterator)] || "\" AS column_name," 

          # min/max
          || "SAFE_CAST(MIN(" || columns_to_profile[OFFSET(column_iterator)] || ") AS STRING) AS min," 
          || "SAFE_CAST(MAX(" || columns_to_profile[OFFSET(column_iterator)] || ") AS STRING) AS max,"

          # nulls
          || "SUM(IF(" || columns_to_profile[OFFSET(column_iterator)] || " IS NULL,1,0)) AS null_count," 
          || "SUM(IF(" || columns_to_profile[OFFSET(column_iterator)] || " IS NULL,0,1)) AS non_null_count,"

          # mean/median
          || "IFNULL(ROUND(AVG(SAFE_CAST(SAFE_CAST(" || columns_to_profile[OFFSET(column_iterator)] || " AS STRING) AS FLOAT64)), 6), -1.0) AS mean," 
          || "(SELECT IFNULL(ROUND((PERCENTILE_CONT(SAFE_CAST(SAFE_CAST("
            || columns_to_profile[OFFSET(column_iterator)] || " AS STRING) AS FLOAT64), 0.5) OVER()), 6), -1.0) "
            || "FROM " || full_source_table_name || " LIMIT 1) AS median";
    SET
      query_string = query_string || " FROM " || full_source_table_name;
    SET
      column_iterator = column_iterator + 1;
  END WHILE;
  SET query_string = query_string || ");";

  EXECUTE IMMEDIATE query_string;
  
  # Write results to table
  EXECUTE IMMEDIATE
    FORMAT("""
      INSERT %s
        (profile_id, profile_timestamp, column_name, null_analysis, numeric_analysis, min_max)
      SELECT
        "%s" as profile_id,
        @var_profile_timestamp as profile_timestamp,
        column_name,
        
        #null_analysis
        STRUCT(null_count, 
          CAST(ROUND(safe_multiply(
          safe_divide(
            null_count,
            %d),
          100), 6) AS NUMERIC) AS null_percentage,
          non_null_count,
          CAST(ROUND(safe_multiply(
          safe_divide(
            non_null_count,
            %d),
          100), 6) AS NUMERIC) AS non_null_percentage)
          AS null_analysis,
        
        # numeric_analysis
        STRUCT(SAFE_CAST(IF(mean = -1.0, NULL, mean) AS NUMERIC) AS mean, 
          SAFE_CAST(IF(median = -1.0, NULL, median) AS NUMERIC) AS median)
          AS numeric_analysis,
        
        # min_max
        STRUCT(min, max) AS min_max
      FROM
        numeric_calculation_results;
    """,
    full_destination_table_name,
    profile_id,
    total_row_count, total_row_count)
    USING
    profile_timestamp as var_profile_timestamp;

  # START Processing
  SET column_iterator = 0;
  WHILE
    column_iterator < ARRAY_LENGTH(columns_to_profile) DO

    # Column Pattern Analysis
    EXECUTE IMMEDIATE
      FORMAT("""
      WITH AGG_DATA AS (
        SELECT
        "%s" AS column_name,
        STRUCT(SAFE_CAST(%s AS STRING) AS value,
        COUNT(*) AS raw_count,
        CAST(ROUND(safe_multiply(
          safe_divide(COUNT(*), %d),
          100), 6) AS NUMERIC) AS percentage
        ) column_distribution
        FROM %s
        GROUP BY %s
        ORDER BY column_distribution.percentage DESC
        LIMIT %d)
      SELECT ARRAY_AGG(column_distribution)
      FROM AGG_DATA
      GROUP BY column_name;""", 
      columns_to_profile[OFFSET(column_iterator)],
      columns_to_profile[OFFSET(column_iterator)],
      total_row_count,
      full_source_table_name,  
      columns_to_profile[OFFSET(column_iterator)],
      max_groups)
    INTO var_pattern_distribution;

    # Calculate inferred data type
    EXECUTE IMMEDIATE
      FORMAT("""
      WITH raw_data AS (
        SELECT
          COUNT(*) AS type_count,
          get_type(SAFE_CAST(%s AS STRING)) AS inferred_type
        FROM %s
        GROUP BY inferred_type
        ORDER BY type_count DESC
      )
      SELECT ARRAY_AGG(STRUCT(
        raw_data.inferred_type, 
        raw_data.type_count,
        CAST(ROUND(safe_multiply(
          safe_divide(raw_data.type_count, %d),
          100), 6) AS NUMERIC) AS type_percentage)) AS inferred_data_type
      FROM raw_data;
      """,
      columns_to_profile[OFFSET(column_iterator)],
      full_source_table_name,
      total_row_count
      )
    INTO var_inferred_data_type;

    # Write results.
    EXECUTE IMMEDIATE FORMAT("""
      UPDATE %s
      SET 
        # column_distribution
        column_distribution = (SELECT @var_pattern_distribution),
        
        # type
        inferred_data_type = (SELECT @var_inferred_data_type)
      WHERE
        column_name = "%s"
        AND profile_id = "%s"
        AND profile_timestamp = @var_profile_timestamp
        
      ;""",
      full_destination_table_name, 
      columns_to_profile[OFFSET(column_iterator)],
      profile_id
      )
    USING 
    profile_timestamp as var_profile_timestamp,
    var_pattern_distribution as var_pattern_distribution,
    var_inferred_data_type as var_inferred_data_type;

    SET column_iterator = column_iterator + 1;

  END WHILE;
  
END;