import struct
import csv
import argparse

parser = argparse.ArgumentParser(description="Process a fixed width file.")
parser.add_argument('formatfile', type=str, help='the path and filename of the format and column positions')
parser.add_argument('sourcefile', type=str, help='the path and filename of the source fixed width file')
parser.add_argument('destinationfile', type=str, help='the path and filename of the output file')

args = parser.parse_args()
print(args)
print("Column Format File Location:", args.formatfile)
print("Source File Location:", args.sourcefile)
print("Destination File Location:", args.destinationfile)

# Create Schema
column_list = []

with open(args.formatfile, mode = 'r') as schema_file:
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
with open(args.sourcefile, mode = 'r') as input_file:
  with open(args.destinationfile, mode = 'w') as output_file:
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
    print ("Processed: ")
    print (line_count)