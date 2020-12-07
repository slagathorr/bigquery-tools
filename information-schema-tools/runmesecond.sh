bq mk --dataset --description "Hello, metadata world, again!" hellometadata2
bq mk -t \
--time_partitioning_type=DAY \
--description "Surprise table!" \
--label organization:production --label category:cool-data \
hellometadata2.production_table table4_schema

bq mk --dataset --description "Metadata Catalog" metadata_catalog