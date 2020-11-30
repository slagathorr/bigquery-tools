from google.cloud import bigquery

import csv
import sys

filename = sys.argv[1]

table_name_label = 'SAS Variable Name' # Change this to what marks your table name

table_name = ""
description = ""
labels = []

bq_client = bigquery.Client()

def parse_file():
  global description
  global labels
  global table_name
  with open(filename) as csv_file:
    with open('outfile.csv', mode='w') as csv_outfile:
      out_writer = csv.writer(csv_outfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      csv_reader = csv.reader(csv_file, delimiter = ',')
      
      line_count = 0
      is_metadata = 1
      for row in csv_reader:
        if row[0] == table_name_label:
          table_name = row[1]
          print("Set the table name", table_name)
        elif row[0] == 'Label':
          description = description + "Label: " + row[1]
          print("Set the lable field", description)
        elif row[0] == 'Section Name':
          labels.append({'section-name': row[1].replace(' ', '-').lower()})
          print("Set the section name", labels)
        elif row[0] == 'Section Number':
          labels.append({'section-number': row[1]})
          print("Set the section number", labels)
        elif row[0] == 'Question':
          description = description + " Question: " + row[1]
          print("Set the question", description)
        elif row[0] == 'DATA':
          is_metadata = 0
          print("Starting data write")
        elif row[0] == 'Question Number':
          labels.append({'question-number': row[1]})
          print("Set the question number", labels)
        elif is_metadata == 0 and row[0] != 'DATA':
          out_writer.writerow(row)
          line_count += 1
          if line_count % 10 == 0: print(line_count)
        else:
          print("Discarded", row)
      print(f'Processed {line_count} lines.')
      print(table_name, description)

def create_load_table():
  global bq_client

  # TODO(developer): Set table_id to the ID of the table to create.
  table_id = "temporary-sandbox-290223.brfss." + table_name

  job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True, write_disposition="WRITE_TRUNCATE")
  
  with open('outfile.csv', "rb") as source_file:
    job = bq_client.load_table_from_file(source_file, table_id, job_config=job_config)

  job.result()  # Waits for the job to complete.

  table = bq_client.get_table(table_id)  # Make an API request.
  print(
      "Loaded {} rows and {} columns to {}".format(
          table.num_rows, len(table.schema), table_id
      )
  )

  project = bq_client.project
  dataset_ref = bigquery.DatasetReference(project, 'brfss')
  table_ref = dataset_ref.table(table_name)
  table = bq_client.get_table(table_ref)  # API request

  for label in labels:
    print(label, type(label))
    table.labels = label

    table = bq_client.update_table(table, ["labels"])  # API request

def update_brfss_description():
  global bq_client

  project = bq_client.project
  dataset_ref = bigquery.DatasetReference(project, 'brfss')
  table_ref = dataset_ref.table('brfss_llcp2019')
  table = bq_client.get_table(table_ref)
  new_schema = []
  for x in table.schema:
    if(x.name == table_name):
      print("Updating field")
      current_field = x.to_api_repr()
      current_field.update({'description': description})
      new_schema.append(bigquery.SchemaField.from_api_repr(current_field))
      print(current_field)
    else:
      new_schema.append(x)
  table.schema = new_schema
  table = bq_client.update_table(table, ['schema'])


if __name__ == "__main__":
  parse_file()
  create_load_table()
  update_brfss_description()