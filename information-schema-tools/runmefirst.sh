bq mk --dataset --description "Hello, metadata world!" hellometadata
bq mk \
--table \
--expiration 360000 \
--description "Helo, first table!" \
--label category:cool-data --label ingestion-method:manual \
hellometadata.first_table table1_schema
bq mk -t \
--time_partitioning_type=DAY \
--time_partitioning_expiration 259200 \
--description "This is my time partitioned table" \
--label organization:development --label category:cool-data \
hellometadata.second_table \
table2_schema
bq mk -t \
hellometadata.third_table \
table3_schema