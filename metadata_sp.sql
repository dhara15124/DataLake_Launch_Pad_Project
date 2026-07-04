-- ============================================================
-- FILE: metadata_sp.sql
-- DATABASE: datalake_metadata
-- PURPOSE: Stored procedures and metadata additions to support
--          media_ingestion_job.py
-- ============================================================

USE datalake_metadata;


-- ============================================================
-- Add crawler config columns to job_step
-- glue_database_name : Athena/Glue database to register tables
-- crawler_name       : Name of the Glue crawler to create/run
-- ============================================================
ALTER TABLE job_step
    ADD COLUMN glue_database_name  VARCHAR(200) DEFAULT NULL,
    ADD COLUMN crawler_name        VARCHAR(200) DEFAULT NULL;

-- Update the existing job_step row with crawler details
UPDATE job_step
SET
    run_crawler        = 1,
    glue_database_name = 'media_glue_db',
    crawler_name       = 'media-mysql-crawler'
WHERE job_step_id = 1;


-- ============================================================
-- STORED PROCEDURE: sp_get_job_step_mappings
--
-- Called by media_ingestion_job.py with job_name argument.
-- Returns all active source→destination mappings for the job.
--
-- Returns columns:
--   mapping_id
--   dataset_name            (dataset name, used to build S3 prefix in Python)
--   source_schema_name      (schema of the MySQL source table)
--   source_entity_name      (MySQL table name)
--   dest_s3_bucket          (S3 bucket to write Parquet)
--   dest_s3_prefix          (base prefix, e.g. 'source/')
--   source_secret_key_name  (Secrets Manager key for source MySQL)
--   source_database_name    (MySQL database name)
--   glue_database_name      (Glue/Athena database name)
--   crawler_name            (Glue crawler name)
--   run_crawler             (1 = run crawler after load)
-- ============================================================
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_get_job_step_mappings $$

CREATE PROCEDURE sp_get_job_step_mappings(IN p_job_name VARCHAR(200))
BEGIN
    SELECT
        m.mapping_id,
        d.dataset_name,
        src_schema.schema_name                                          AS source_schema_name,
        src_entity.entity_name                                          AS source_entity_name,
        dest_entity.s3_bucket                                           AS dest_s3_bucket,
        dest_entity.s3_prefix                                           AS dest_s3_prefix,
        db.secret_key_name                                              AS source_secret_key_name,
        db.database_name                                                AS source_database_name,
        js.glue_database_name,
        js.crawler_name,
        js.run_crawler
    FROM source_to_destination_mapping  m
    JOIN job_step                       js          ON js.job_step_id   = m.job_step_id
    JOIN job                            j           ON j.job_id         = js.job_id
    JOIN dataset_entity                 src_entity  ON src_entity.entity_id  = m.source_entity_id
    JOIN dataset_schema                 src_schema  ON src_schema.schema_id  = src_entity.schema_id
    JOIN dataset_database               db          ON db.database_id        = src_schema.database_id
    JOIN dataset                        d           ON d.dataset_id          = db.dataset_id
    JOIN dataset_entity                 dest_entity ON dest_entity.entity_id = m.dest_entity_id
    WHERE j.job_name    = p_job_name
      AND j.is_active   = 1
      AND js.is_active  = 1
      AND m.is_active   = 1
    ORDER BY js.step_sequence, m.mapping_id;
END $$

DELIMITER ;


-- ============================================================
-- Quick test — verify the SP returns correct rows
-- ============================================================
CALL sp_get_job_step_mappings('Media MySQL Ingestion Job');
