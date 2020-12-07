bq mk --dataset --description "Hello, metadata world, again!" hellometadata2
bq mk -t \
--time_partitioning_type=DAY \
--description "Surprise table!" \
--label organization:production --label category:cool-data \