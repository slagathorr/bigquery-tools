import struct
import csv

# Create Schema
with open('./data/brfss_format.csv') as schema_file:
  csv_reader = csv.reader(schema_file, delimiter=",")
  line_count = 0
  for row in csv_reader:
    if line_count == 0:
      print "Column names are: "
      print row
      line_count += 1
    else:
      print "Row: "
      print row
      line_count += 1
  print "Processed: "
  print line_count

fieldwidths = (2,2,8,2,2,4,4)  # negative widths represent ignored padding fields
fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                        for fw in fieldwidths)
print fmtstring
fieldstruct = struct.Struct(fmtstring)
parse = fieldstruct.unpack_from
print('fmtstring: {!r}, recsize: {} chars'.format(fmtstring, fieldstruct.size))

line = "01              0101182019     11002019000001                 11 121 0120001              2         31588881121112112222 222223  1121207                                    232        12127880301540502 22212213 073888      2                2101025553022012033152      412      213                                                                                                 134 42           22221131111                                                                                                                                                                                                                                                                           1001                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      11     101101146.9849076                           0.523.4924538     02                                                                                                                                                                                                                 1 0.5617998190.819282                                                 135.30408                                                                                                                                          13192122113112                                                        0202  2222221328060621570698528173211231200010000012      0176600303                            023330                              4233213200000200000700140043005000110002000001141111002121"
fields = parse(line)
print('fields: {}'.format(fields))