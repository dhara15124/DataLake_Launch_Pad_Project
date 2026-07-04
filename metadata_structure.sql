-- ============================================================
-- FILE: metadata_structure.sql
-- DATABASE: datalake_metadata
-- PURPOSE: Full metadata structure for MySQL ingestion pipeline
--
-- TABLE HIERARCHY:
--   dataset
--     └── dataset_database
--           └── dataset_schema
--                 └── dataset_entity
--
--   job
--     └── job_step  (one step: MySQL source → LandingZone)
--           └── source_to_destination_mapping
-- ============================================================

CREATE DATABASE IF NOT EXISTS datalake_metadata;
USE datalake_metadata;


-- ============================================================
-- TABLE 1: dataset
-- Top level — represents a source system or data product
-- e.g. "media", "freud", "catie", "mirad"
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset (
    dataset_id          INT AUTO_INCREMENT PRIMARY KEY,
    dataset_name        VARCHAR(100) NOT NULL UNIQUE,   -- e.g. media, freud
    dataset_description VARCHAR(300),
    is_active           TINYINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- TABLE 2: dataset_database
-- Represents the physical database inside a source system
-- e.g. "media_db" inside dataset "media"
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset_database (
    database_id         INT AUTO_INCREMENT PRIMARY KEY,
    dataset_id          INT NOT NULL,                   -- FK → dataset
    database_name       VARCHAR(100) NOT NULL,          -- e.g. media_db
    connection_type     VARCHAR(50)  NOT NULL,          -- e.g. rds_mysql, mssql, oracle
    secret_key_name     VARCHAR(200) NOT NULL,          -- Secrets Manager key name
    is_active           TINYINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_db_dataset FOREIGN KEY (dataset_id)
        REFERENCES dataset(dataset_id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    UNIQUE KEY uq_database (dataset_id, database_name)
);


-- ============================================================
-- TABLE 3: dataset_schema
-- Represents a schema inside a database
-- e.g. "media_db" schema inside "media_db" database
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset_schema (
    schema_id           INT AUTO_INCREMENT PRIMARY KEY,
    database_id         INT NOT NULL,                   -- FK → dataset_database
    schema_name         VARCHAR(100) NOT NULL,          -- e.g. media_db, dbo, public
    is_active           TINYINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_schema_database FOREIGN KEY (database_id)
        REFERENCES dataset_database(database_id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    UNIQUE KEY uq_schema (database_id, schema_name)
);


-- ============================================================
-- TABLE 4: dataset_entity
-- Represents a single table inside a schema
-- e.g. "media_content", "media_reviews"
-- ============================================================
CREATE TABLE IF NOT EXISTS dataset_entity (
    entity_id           INT AUTO_INCREMENT PRIMARY KEY,
    schema_id           INT NOT NULL,                   -- FK → dataset_schema
    entity_name         VARCHAR(200) NOT NULL,          -- e.g. media_content
    entity_description  VARCHAR(300),
    s3_bucket           VARCHAR(300),                   -- destination S3 bucket
    s3_prefix           VARCHAR(500),                   -- destination S3 path prefix
    is_active           TINYINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_entity_schema FOREIGN KEY (schema_id)
        REFERENCES dataset_schema(schema_id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    KEY idx_entity (schema_id, entity_name)
);


-- ============================================================
-- TABLE 5: job
-- Represents a Glue ETL job
-- e.g. "Media MySQL Ingestion Job"
-- ============================================================
CREATE TABLE IF NOT EXISTS job (
    job_id              INT AUTO_INCREMENT PRIMARY KEY,
    job_name            VARCHAR(200) NOT NULL UNIQUE,   -- must match Glue job name
    job_description     VARCHAR(300),
    is_active           TINYINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- TABLE 6: job_step
-- Represents one step inside a job
-- A job can have multiple steps e.g. LZ step, RZ step
-- ============================================================
CREATE TABLE IF NOT EXISTS job_step (
    job_step_id         INT AUTO_INCREMENT PRIMARY KEY,
    job_id              INT NOT NULL,                   -- FK → job
    step_sequence       INT NOT NULL,                   -- order of execution: 1, 2, 3...
    step_name           VARCHAR(200) NOT NULL,          -- e.g. "MySQL to LandingZone"
    source_connection_type  VARCHAR(50) NOT NULL,       -- e.g. rds_mysql, mssql, s3_parquet
    dest_connection_type    VARCHAR(50) NOT NULL,       -- e.g. s3_parquet, s3_json
    run_crawler         TINYINT DEFAULT 0,              -- 1 = run Glue crawler after step
    continue_on_error   TINYINT DEFAULT 0,              -- 1 = continue if one table fails
    is_active           TINYINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_step_job FOREIGN KEY (job_id)
        REFERENCES job(job_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);


-- ============================================================
-- TABLE 7: source_to_destination_mapping
-- Maps a source entity to a destination entity for a job step
-- This is the heart of the pipeline config:
--   "for this job step, take THIS source table → write to THAT destination"
-- ============================================================
CREATE TABLE IF NOT EXISTS source_to_destination_mapping (
    mapping_id          INT AUTO_INCREMENT PRIMARY KEY,
    job_step_id         INT NOT NULL,                   -- FK → job_step
    source_entity_id    INT NOT NULL,                   -- FK → dataset_entity (source)
    dest_entity_id      INT NOT NULL,                   -- FK → dataset_entity (destination)
    is_active           TINYINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_mapping_step    FOREIGN KEY (job_step_id)
        REFERENCES job_step(job_step_id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    CONSTRAINT fk_mapping_source  FOREIGN KEY (source_entity_id)
        REFERENCES dataset_entity(entity_id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    CONSTRAINT fk_mapping_dest    FOREIGN KEY (dest_entity_id)
        REFERENCES dataset_entity(entity_id)
        ON DELETE CASCADE ON UPDATE CASCADE,

    UNIQUE KEY uq_mapping (job_step_id, source_entity_id, dest_entity_id)
);


-- ============================================================
-- SAMPLE DATA
-- ============================================================

-- dataset
INSERT INTO dataset (dataset_name, dataset_description) VALUES
('media', 'Media source system with movies and reviews');

-- dataset_database
INSERT INTO dataset_database (dataset_id, database_name, connection_type, secret_key_name) VALUES
(1, 'media_db', 'rds_mysql', 'media-secret');

-- dataset_schema
INSERT INTO dataset_schema (database_id, schema_name) VALUES
(1, 'media_db');

-- dataset_entity — source tables (MySQL): no S3 details needed
-- dataset_entity — dest tables: only s3_bucket + s3_prefix = 'source/'
--   full path is built dynamically in SP as: source/<dataset>/<schema>/<entity>
INSERT INTO dataset_entity (schema_id, entity_name, entity_description, s3_bucket, s3_prefix) VALUES
(1, 'media_content', 'MySQL source - media_content',     NULL,                       NULL),
(1, 'media_reviews', 'MySQL source - media_reviews',     NULL,                       NULL),
(1, 'media_content', 'LandingZone dest - media_content', 'aps-group-rawzone-bucket', 'source/'),
(1, 'media_reviews', 'LandingZone dest - media_reviews', 'aps-group-rawzone-bucket', 'source/');

-- job
INSERT INTO job (job_name, job_description) VALUES
('Media MySQL Ingestion Job', 'Ingests media tables from MySQL RDS to S3 LandingZone');

-- job_step — only one step: MySQL source → LandingZone
INSERT INTO job_step (job_id, step_sequence, step_name, source_connection_type, dest_connection_type, run_crawler, continue_on_error) VALUES
(1, 1, 'MySQL to LandingZone', 'rds_mysql', 's3_parquet', 0, 1);

-- source_to_destination_mapping
-- Step 1: MySQL source entities (1,2) → LandingZone dest entities (3,4)
INSERT INTO source_to_destination_mapping (job_step_id, source_entity_id, dest_entity_id) VALUES
(1, 1, 3),   -- media_content MySQL → media_content LandingZone
(1, 2, 4);   -- media_reviews MySQL → media_reviews LandingZone
