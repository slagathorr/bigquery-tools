import struct
import csv

# Create Schema
column_list = []

with open('./test_data/brfss_format.csv', mode = 'r') as schema_file:
  csv_reader = csv.reader(schema_file, delimiter=",")
  line_count = 0
  for row in csv_reader:
    if line_count == 0:
      print ("Column names are: ")
      print (row)
      line_count += 1
    else:
      column_list.append({"name": row[0], "start": int(row[1]), "length": int(row[2])})
      print (row[0], row[1], row[2])

# Read through input data, and write rows to output file in CSV.
with open('./test_data/LLCP2019_TOP10ROWS.ASC', mode = 'r') as input_file:
  with open('./test_data/LLCP2019_TOP10ROWS.CSV', mode = 'w') as output_file:
    line_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    # Print the header row.
    column_header = []
    for iter in column_list:
      column_header.append(iter["name"])
    line_writer.writerow(column_header) # Write the line

     # Start traversing the source file.
    line_count = 0
    for input_line in input_file:
      out_line = []
      for iter in column_list: # Run through the list of columns and get their values.
        out_line.append(str.strip(input_line[iter["start"] - 1:iter["start"] - 1 + iter["length"]]))
      line_writer.writerow(out_line) # Write the line.
      line_count += 1
      if line_count % 1000 == 0:
        print ("Processed: ", line_count)
    print "Processed: "
    print line_count