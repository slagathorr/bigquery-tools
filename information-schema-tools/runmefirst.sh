bq mk --dataset --default_table_expiration 3600 --description "Hello, metadata world!" hellometadata
bq mk \
--table \
--expiration 3600 \
--description "Helo, first table!" \
--label category:cool-data, ingestion-method:manual > hellometadata.first_table 