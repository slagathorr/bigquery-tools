bq mk --dataset --description "Super Deduper!" dedup_stuff
bq query --destination_table dedup_stuff.main_table --use_legacy_sql=false --replace < create_main_table.sql
bq query --destination_table dedup_stuff.ingest_table --use_legacy_sql=false --replace < create_ingest_table.sql