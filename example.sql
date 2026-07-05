-- dataset
INSERT INTO dataset (dataset_name, dataset_description) VALUES
('media', 'Media source system');

-- dataset_database
INSERT INTO dataset_database (dataset_id, database_name, connection_type, secret_key_name) VALUES
((SELECT dataset_id  FROM dataset          WHERE dataset_name  = 'media'), 'media_db', 'rds_mysql', 'media-secret');

-- dataset_schema
INSERT INTO dataset_schema (database_id, schema_name) VALUES
((SELECT database_id FROM dataset_database WHERE database_name = 'media_db'), 'media_db');

-- dataset_entity
INSERT INTO dataset_entity (schema_id, entity_name, entity_description, s3_bucket, s3_prefix) VALUES
((SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db'), 'media_content', 'MySQL source - media_content', NULL, NULL),
((SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db'), 'media_reviews', 'MySQL source - media_reviews', NULL, NULL),
((SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db'), 'media_content', 'LandingZone dest - media_content', 'ingestion-rawzone-group', 'source/'),
((SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db'), 'media_reviews', 'LandingZone dest - media_reviews', 'ingestion-rawzone-group', 'source/');

-- job
INSERT INTO job (job_name, job_description) VALUES
('Media MySQL Ingestion Job', 'Ingests media tables from MySQL RDS to S3 LandingZone');

-- job_step
INSERT INTO job_step (job_id, step_sequence, step_name, source_connection_type, dest_connection_type, run_crawler, continue_on_error) VALUES
((SELECT job_id      FROM job              WHERE job_name      = 'Media MySQL Ingestion Job'), 1, 'MySQL to LandingZone', 'rds_mysql', 's3_parquet', 0, 1);

-- source_to_destination_mapping
INSERT INTO source_to_destination_mapping (job_step_id, source_entity_id, dest_entity_id) VALUES
((SELECT job_step_id FROM job_step         WHERE job_id = (SELECT job_id      FROM job              WHERE job_name      = 'Media MySQL Ingestion Job') AND step_sequence = 1), (SELECT entity_id FROM dataset_entity WHERE schema_id = (SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db') AND entity_name = 'media_content' AND s3_bucket IS NULL), (SELECT entity_id FROM dataset_entity WHERE schema_id = (SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db') AND entity_name = 'media_content' AND s3_bucket IS NOT NULL))   -- media_content MySQL → media_content LandingZone,
((SELECT job_step_id FROM job_step         WHERE job_id = (SELECT job_id      FROM job              WHERE job_name      = 'Media MySQL Ingestion Job') AND step_sequence = 1), (SELECT entity_id FROM dataset_entity WHERE schema_id = (SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db') AND entity_name = 'media_reviews' AND s3_bucket IS NULL), (SELECT entity_id FROM dataset_entity WHERE schema_id = (SELECT schema_id   FROM dataset_schema   WHERE schema_name   = 'media_db') AND entity_name = 'media_reviews' AND s3_bucket IS NOT NULL))   -- media_reviews MySQL → media_reviews LandingZone;